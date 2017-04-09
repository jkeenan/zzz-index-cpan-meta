#!/usr/bin/env perl
use v5.14;
use strict;
use warnings;
use CPAN::DistnameInfo;
use CPAN::Meta;
use CPAN::Visitor;
use Cwd;
use List::MoreUtils qw/uniq/;
use MongoDB;
use Parallel::ForkManager;
#use Ramdisk;
#use Sys::Ramdisk;
use Try::Tiny;
use Getopt::Long;
use File::HomeDir;

=pod

GetOptions ("length=i" => \$length,    # numeric
                  "file=s"   => \$data,      # string
                  "verbose"  => \$verbose)   # flag
      or die("Error in command line arguments\n");

=cut

my $home = File::HomeDir->my_home;
my $JOBS = 4;
my $CPAN = "$home/minicpan";
#my $tdir = "$home/tmp/ramdisk";
my $verbose = '';

GetOptions(
    "jobs=i"        => \$JOBS,
    "CPAN=s"        => \$CPAN,
    #    "ramdisk=s"     => \$tdir,
    "verbose"       => \$verbose,
) or die "Error in command-line arguments: $!";
unless (-d $CPAN) {
    die "Unable to locate '$CPAN' to serve as root of CPAN installation: $!";
}
#unless (-d $tdir) {
#    mkdir $tdir or die "Unable to mkdir '$tdir' prior to use as ramdisk: $!";
#}

#--------------------------------------------------------------------------#
# worker function
#--------------------------------------------------------------------------#

sub worker {
    my ( $chunk, $n, $cpan_path, $db_name, $coll_name ) = @{ $_[0] };

    my $mc      = MongoDB::MongoClient->new;
    my $coll    = $mc->get_database($db_name)->get_collection($coll_name);
    my $pkgcoll = $mc->get_database($db_name)->get_collection("packages");
    my $batch   = $coll->unordered_bulk;

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
              $pkgcoll->find( { distfile => $job->{distfile} } )
              ->fields( { 'maintainers' => 1, distlatest => 1, distcore => 1 } )->all;
            my @maints = sort( uniq( map { @{ $_->{maintainers} || [] } } @pkgs ) );

            my ($upstream) = grep { defined } map { $_->{distcore} } @pkgs;

            my $doc = {
                _id          => $job->{distfile},
                _upload_date => ( stat( $job->{distpath} ) )[9],
                _uploader    => $d->cpanid,
                _latest      => $pkgs[0]{distlatest},
                _maintainers => \@maints,
                ( $upstream ? ( _core => $upstream ) : () ),
            };

            my ($meta_file) = grep { -f $_ } qw/META.json META.yml/;

            if ( !$meta_file ) {
                #$batch->insert($doc);
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
              $pkgcoll->find( { _id => { '$in' => $doc->{_requires} } } )
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

#--------------------------------------------------------------------------#
# main program
#--------------------------------------------------------------------------#

$|++;

#my $JOBS = shift || 10;
#my $CPAN = shift || '/srv/cpan';
my $DB   = 'cpan';
my $COLL = 'meta';
my $CHUNKING = 100;
my $CWD      = Cwd::getcwd;
my $PID      = $$;

say "Prepping fresh collection $DB.$COLL";
my $mc      = MongoDB::MongoClient->new;
my $db      = $mc->get_database($DB);
my $coll    = $db->get_collection($COLL);
my $pkgcoll = $db->get_collection("packages");
$coll->drop;
$coll->ensure_index( [ _requires => 1 ] );
$coll->ensure_index( [ _upstream => 1 ] );
$coll->ensure_index( [ name      => 1 ] );

#say "Setting up ramdisk";
##my $ramdisk = Ramdisk->new(1024);
#my $ramdisk = Sys::Ramdisk->new(
#    size => "100m",
#    dir  => $tdir,
#);
#
#$ramdisk->mount();

say "Queueing tasks...";
my @dists =
  map { $_->{_id} }
  $pkgcoll->aggregate(
    [ { '$group' => { _id => '$distfile' } }, { '$sort' => { _id => 1 } } ],
    { cursor => 1 } )->all;

say sprintf( "%d distributions to process in %d blocks",
    0+ @dists, int( @dists / $CHUNKING ) + 1 );

say "Running tasks...";
my ( $n, $pm ) = ( 0, Parallel::ForkManager->new( $JOBS > 1 ? $JOBS : 0 ) );

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
    worker( [ \@chunk, $n, $CPAN, $DB, $COLL ] );
    $pm->finish;
}

$pm->wait_all_children;

#$ramdisk->mount();
exit;
