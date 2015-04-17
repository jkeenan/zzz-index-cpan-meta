#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use CPAN::DistnameInfo;
use CPAN::Meta;
use CPAN::Visitor;
use MongoDB;
use Try::Tiny;
use Parallel::ForkManager;
use List::MoreUtils qw/uniq/;

$|++;

my $JOBS = shift || 10;
my $CPAN = shift || '/srv/cpan';
my $DB   = 'cpan';
my $COLL = 'meta';
my $CHUNKING = 100;

my $mc      = MongoDB::MongoClient->new;
my $db      = $mc->get_database($DB);
my $coll    = $db->get_collection($COLL);
my $pkgcoll = $db->get_collection("packages");

$coll->drop;
$coll->ensure_index( [ flat_prereqs => 1 ] );
$coll->ensure_index( [ name         => 1 ] );

my $pm = Parallel::ForkManager->new( $JOBS > 1 ? $JOBS : 0 );

say "Queueing tasks...";
my $n = 0;

my @dists =
  map { $_->{_id} }
  $pkgcoll->aggregate(
    [ { '$group' => { _id => '$distfile' } }, { '$sort' => { _id => 1 } } ],
    { cursor => 1 } )->all;

say "Running tasks...";
while (@dists) {
    my @chunk = splice( @dists, 0, $CHUNKING );
    $n++;
    $pm->start and next;
    _work( [ \@chunk, $n ] );
    $pm->finish;
}
$pm->wait_all_children;

sub _work {
    my ( $chunk, $n ) = @{ $_[0] };
    my $mc      = MongoDB::MongoClient->new;
    my $coll    = $mc->get_database($DB)->get_collection($COLL);
    my $pkgcoll = $mc->get_database($DB)->get_collection("packages");

    my $bulk = $coll->unordered_bulk;

    my $visitor = CPAN::Visitor->new(
        cpan  => $CPAN,
        quiet => 0,
        files => $chunk,
        stash => { prefer_bin => 1, }
    );

    # Action is specified via a callback
    $visitor->iterate(
        check => sub { -f $_[0]->{distpath} },
        visit => sub {
            my $job = shift;

            my $upload_date = ( stat( $job->{distpath} ) )[9];

            my $d = CPAN::DistnameInfo->new($job->{distpath});

            my @pkgs =
              $pkgcoll->find( { distfile => $job->{distfile} } )->fields( { 'maintainers' => 1, distlatest => 1, distcore => 1 } )->all;
            my @maints = sort( uniq( map { @{ $_->{maintainers} || [] } } @pkgs ) );

            my ($upstream) = grep { defined } map { $_->{distcore} } @pkgs;

            my $doc = {
                _id          => $job->{distfile},
                _upload_date => $upload_date,
                _uploader    => $d->cpanid,
                _latest      => $pkgs[0]{distlatest},
                _maintainers => \@maints,
                ( $upstream ? ( _core => $upstream ) : () ),
            };

            my ($meta_file) = grep { -f $_ } qw/META.json META.yml/;

            if ($meta_file) {
                $doc->{_meta_file} = $meta_file;
            }
            else {
                $bulk->insert($doc);
                return;
            }

            my $meta = try {
                CPAN::Meta->load_file($meta_file);
            }
            catch {
                /\A([^\n]+)/;
                $doc->{_meta_error} = $1;
                undef; # return value from try
            };

            if ( !$meta ) {
                $bulk->insert($doc);
                return;
            }

            %{$doc} = ( %{ $meta->as_struct }, %$doc );

            my $prereqs_req =
              $meta->effective_prereqs->merged_requirements( [qw/configure build test runtime/],
                [qw/requires/] )->as_string_hash;

            my $prereqs_rec =
              $meta->effective_prereqs->merged_requirements( [qw/configure build test runtime/],
                [qw/recommends/] )->as_string_hash;

            $doc->{_flat_prereqs} = [ sort keys %$prereqs_req ];
            $doc->{_flat_prereqs_xl} = [ sort( uniq( keys %$prereqs_req, keys %$prereqs_rec ) ) ];

            my @dists_req =
              map { $_->{distname} }
              $pkgcoll->find( { _id => { '$in' => $doc->{_flat_prereqs} } } )
              ->fields( { distname => 1 } )->all;

            my @dists_rec =
              map { $_->{distname} }
              $pkgcoll->find( { _id => { '$in' => [ keys %$prereqs_rec ] } } )
              ->fields( { distname => 1 } )->all;

            $doc->{_upstream} = [ sort( uniq(@dists_req) ) ];

            $doc->{_upstream_xl} = [ sort( uniq( @dists_req, @dists_rec ) ) ];

            $bulk->insert($doc);
        }
    );

    try {
        $bulk->execute;
        say "Block $n: Inserted";
    }
    catch {
        say "Block $n: MongoDB::Error: $_\n";
        say for @$chunk;
    }
}
