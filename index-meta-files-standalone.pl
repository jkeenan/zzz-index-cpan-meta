#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use CPAN::DistnameInfo;
use CPAN::Meta;
use CPAN::Visitor;
use Cwd;
use MongoDB;
use Parallel::ForkManager;
use Parse::CPAN::Packages::Fast;
use Ramdisk;
use Try::Tiny;

#--------------------------------------------------------------------------#
# worker function
#--------------------------------------------------------------------------#

sub worker {
    my ( $chunk, $n, $cpan_path, $db_name, $coll_name ) = @{ $_[0] };

    my $mc    = MongoDB::MongoClient->new;
    my $coll  = $mc->get_database($db_name)->get_collection($coll_name);
    my $batch = $coll->unordered_bulk;

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

            my $doc = {
                _id          => $job->{distfile},
                _upload_date => ( stat( $job->{distpath} ) )[9],
                _uploader    => $d->cpanid,
            };

            my ($meta_file) = grep { -f $_ } qw/META.json META.yml/;

            if ( !$meta_file ) {
                $batch->insert($doc);
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
                $batch->insert($doc);
                return;
            }

            my $merged_prereqs =
              $meta->effective_prereqs->merged_requirements( [qw/configure build test runtime/],
                [qw/requires/] )->as_string_hash;
            $doc->{_requires} = [ sort keys %$merged_prereqs ];

            %{$doc} = ( %{ $meta->as_struct }, %$doc );
            $batch->insert($doc);
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

#--------------------------------------------------------------------------#
# main program
#--------------------------------------------------------------------------#

$|++;

my $JOBS = shift || 10;
my $CPAN = shift || '/srv/cpan';
my $DB   = 'cpan';
my $COLL = 'meta';
my $CHUNKING = 100;
my $CWD      = Cwd::getcwd;
my $PID      = $$;

say "Prepping fresh collection $DB.$COLL";
my $mc   = MongoDB::MongoClient->new;
my $db   = $mc->get_database($DB);
my $coll = $db->get_collection($COLL);
$coll->drop;
$coll->ensure_index( [ name => 1 ] );
$coll->ensure_index( [ _requires => 1 ] );

say "Setting up ramdisk";
my $ramdisk = Ramdisk->new(1024);
local $ENV{TMPDIR} = $ramdisk->root;

say "Queueing tasks...";
my $p = Parse::CPAN::Packages::Fast->new("$CPAN/modules/02packages.details.txt.gz");
my @dists = map { s{^./../}{}r } map { $_->pathname } $p->latest_distributions;
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
exit;
