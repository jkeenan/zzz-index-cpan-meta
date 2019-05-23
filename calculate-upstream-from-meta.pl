#!/usr/bin/env perl
use v5.14;
use strict;
use warnings;

use Carp;
use Cwd;
use Getopt::Long;

use CPAN::DistnameInfo;
use CPAN::Meta;
use CPAN::Visitor;
use List::MoreUtils qw/uniq/;
use MongoDB;
use Parallel::ForkManager;
use Try::Tiny;

=head1 NAME

calculate-upstream-from-meta.pl - determine upstream requirements for a CPAN distribution

=head1 USAGE

    perl calculate-upstream-from-meta.pl \
        --jobs=8 \
        --repository=/path/to/minicpan \
        --verbose

=head1 PREREQUISITES

What you must install from CPAN:

    CPAN::DistnameInfo
    CPAN::Meta
    CPAN::Visitor
    List::MoreUtils qw/uniq/
    MongoDB
    Parallel::ForkManager
    Try::Tiny

What you should have from the Perl 5 core distribution:

    Carp
    Cwd
    Getopt::Long

=cut

my $start_time = time();
my ($repository, $db, $collection, $meta_collection, $jobs, $verbose) = ('') x 6;
GetOptions(
    "repository=s"      => \$repository,
    "db=s"              => \$db,
    "collection=s"      => \$collection,
    "meta_collection=s" => \$meta_collection,
    "jobs=i"            => \$jobs,
    "verbose"           => \$verbose,
) or croak "Error in command-line arguments: $!";
$repository ||= File::Spec->catdir($ENV{HOMEDIR}, 'minicpan');
croak "Cannot locate directory '$repository' for path to CPAN installation"
    unless (-d $repository);
$db                 ||= 'cpan';
$collection         ||= 'packages';
$meta_collection    ||= 'meta';
$jobs               ||= 4;

#--------------------------------------------------------------------------#
# main program
#--------------------------------------------------------------------------#

$|++;

my $CHUNKING = 100;
my $CWD      = Cwd::getcwd;
my $PID      = $$;

say "Prepping fresh collection $db.$meta_collection";
my $mongo_client  = MongoDB::MongoClient->new;
my $mongo_db      = $mongo_client->get_database($db);
my $meta_collection_object      = $mongo_db->get_collection($meta_collection);
my $package_collection_object   = $mongo_db->get_collection($collection);
$meta_collection_object->drop;
$meta_collection_object->indexes->create_many(
    { keys => [ _requires => 1 ] },
    { keys => [ _upstream => 1 ] },
    { keys => [ name      => 1 ] },
);

say "Queueing tasks...";
my @dists =
  map { $_->{_id} }
  $package_collection_object->aggregate(
    [ { '$group' => { _id => '$distfile' } }, { '$sort' => { _id => 1 } } ],
    { cursor => 1 } )->all;

say sprintf( "%d distributions to process in %d blocks",
    0+ @dists, int( @dists / $CHUNKING ) + 1 );

say "Running tasks...";
my ( $n, $pm ) = ( 0, Parallel::ForkManager->new( $jobs > 1 ? $jobs : 0 ) );

$SIG{INT} = sub {
    say "Caught SIGINT; Waiting for child processes";
    $pm->wait_all_children;
    exit 1;
};

while (@dists) {
    my @chunk = splice( @dists, 0, $CHUNKING );
    $n++;
    $pm->start and next;
    $SIG{INT} = sub { chdir $CWD; $pm->finish };
    worker( [ \@chunk, $n, $repository, $db, $meta_collection, $collection ] );
    $pm->finish;
}

$pm->wait_all_children;

my $end_time = time();
if ($verbose) {
    say "Elapsed time: ", $end_time - $start_time, " seconds";
    say "Finished";
}

exit;

#--------------------------------------------------------------------------#
# worker function
#--------------------------------------------------------------------------#

sub worker {
    my ( $chunk, $n, $cpan_path, $db_name, $coll_name, $collection ) = @{ $_[0] };

    my $mongo_client            = MongoDB::MongoClient->new;
    my $meta_collection_object  = $mongo_client->get_database($db_name)->get_collection($coll_name);
    my $package_collection_object = $mongo_client->get_database($db_name)->get_collection($collection);
    my $batch   = $meta_collection_object->unordered_bulk;

    my $visitor = CPAN::Visitor->new(
        cpan  => $cpan_path,
        quiet => 0,
        files => $chunk,
        stash => { prefer_bin => 1, }
    );
    $visitor->iterate(
        check => sub { -f $_[0]->{distpath} },
        visit => sub {
            my $job = shift;

            my $d = CPAN::DistnameInfo->new( $job->{distpath} );

            my @pkgs =
              $package_collection_object->find( { distfile => $job->{distfile} } )
              ->fields( {
                  authority     => 1,
                  maintainers   => 1,
                  distlatest    => 1,
                  distcore      => 1,
              } )->all;
            my @maints = sort( uniq( map { @{ $_->{maintainers} || [] } } @pkgs ) );

            my ($upstream) = grep { defined } map { $_->{distcore} } @pkgs;

            my $doc = {
                _id          => $job->{distfile},
                _upload_date => ( stat( $job->{distpath} ) )[9],
                _uploader    => $d->cpanid,
                _authority   => $pkgs[0]{authority},
                _latest      => $pkgs[0]{distlatest},
                _maintainers => \@maints,
                ( $upstream ? ( _core => $upstream ) : () ),
            };

            my ($meta_file) = grep { -f $_ } qw/META.json META.yml/;

            if ( !$meta_file ) {
                $batch->insert_one($doc);
                return;
            }

            $doc->{_meta_file} = $meta_file;

            my $meta = try {
                CPAN::Meta->load_file($meta_file);
            }
            catch {
                /\A([^\n]+)/;
                $doc->{_meta_error} = $1;
                undef; # return value from try
            };

            if ( !$meta ) {
                $batch->insert_one($doc);
                return;
            }

            # _requires are modules
            my $merged_prereqs =
              $meta->effective_prereqs->merged_requirements( [qw/configure build test runtime/],
                [qw/requires/] )->as_string_hash;

            $doc->{_requires} = [ sort keys %$merged_prereqs ];

            # _upstream are distributions
            my @dists_req =
              map { $_->{distname} }
              $package_collection_object->find( { _id => { '$in' => $doc->{_requires} } } )
              ->fields( { distname => 1 } )->all;

            $doc->{_upstream} = [ sort( uniq(@dists_req) ) ];

            %{$doc} = ( %{ $meta->as_struct }, %$doc );
            _clean_bad_keys($doc->{name}, $doc);
            $batch->insert_one($doc);
        }
    );

    try {
        $batch->execute;
        say "Block $n: Inserted";
    }
    catch {
        say "Block $n: MongoDB::Error: $_\n";
        say for @$chunk;
    }
}

sub _clean_bad_keys {
    my ($name, $doc) = @_;
    for my $k ( keys %$doc ) {
        my $v = $doc->{$k};
        if ( $k =~ /\./ ) {
            warn "Bad key '$k' in $name. Scrubbing it.\n";
            my $new_k = $k =~ s/\./_/gr;
            $doc->{$new_k} = delete $doc->{$k};
        }
        if (ref($v) eq 'HASH') {
            _clean_bad_keys( $name, $v );
        }
    }
}

