#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use CPAN::DistnameInfo;
use IO::Zlib;
use MongoDB;
use Module::CoreList;
use Parse::CPAN::Packages::Fast;
use Tie::IxHash;
use boolean;

=head1 NAME

parse-packages-to-mongodb.pl - store 02 and 06 data to MongoDB

=head1 USAGE

    perl parse-packages-to-mongodb.pl /path/to/cpan [perl_version]

Example:

    perl parse-packages-to-mongodb.pl /home/username/minicpan  5.024001

=head1 PREREQUISITES

What you must install from CPAN:

    CPAN::DistnameInfo
    MongoDB
    Parse::CPAN::Packages::Fast
    Tie::IxHash
    boolean

What you should have from the Perl 5 core distribution:

    IO::Zlib
    Module::CoreList

=cut

my $CPAN = shift || '/srv/cpan';
die "Cannot locate directory '$CPAN' for path to CPAN installation"
    unless (-d $CPAN);
my $targeted_perl_version = shift || $];
die "No data in $Module::CoreList::version for '$targeted_perl_version'"
    unless exists $Module::CoreList::version{$targeted_perl_version};
my $DB   = 'cpan';
my $COLL = 'packages';

say "Parsing 06perms...";
my $fh = IO::Zlib->new( "$CPAN/modules/06perms.txt.gz", "rb" );
my %pkg_to_maint = _read_perms($fh);

say "Parsing 02packages...";
my $p = Parse::CPAN::Packages::Fast->new("$CPAN/modules/02packages.details.txt.gz");

my %latest = map { $_ => true } map { s{^./../}{}r } map { $_->pathname } $p->latest_distributions;

my $mc   = MongoDB::MongoClient->new;
my $coll = $mc->get_database($DB)->get_collection($COLL);
$coll->drop;
$coll->indexes()->create_many(
    { keys => [ dist => 1 ] },
);

my $bulk = $coll->unordered_bulk;

my $cnt = 0;
say "Iterating packages...";

my %current_core = ();
while (my ($k, $v) = each %{$Module::CoreList::version{$targeted_perl_version}}) {
    $current_core{$k} = $v;
}

STDOUT->autoflush(1);
for my $pkg ( $p->packages ) {
    print "." if ++$cnt % 100 == 0;
    my $p = $p->package($pkg);
    my $d = $p->distribution;
    my $distfile = $d->pathname =~ s{^./../}{}r;
    my $upstream = $Module::CoreList::upstream{$pkg} || 'blead';
    my $doc = Tie::IxHash->new(
        _id => $pkg,
        version => $p->version,
        distfile => $distfile,
        distname => $d->dist,
        distversion => $d->version,
        distvname => $d->distvname,
        distlatest => $latest{$distfile} || false,
        uploader => $d->cpanid,
        authority => $pkg_to_maint{$pkg}{auth},
        maintainers => $pkg_to_maint{$pkg}{maint},
        ( exists $current_core{$pkg} ? ( distcore => $upstream ) : () ),
    );
    $bulk->insert_one($doc);
}

say "Sending to database";
$bulk->execute;

exit;

sub _read_perms {
    my ( $fh ) = @_;

    my $inheader = 1;
    my $perms    = {};

    while (<$fh>) {

        if ($inheader) {
            $inheader = 0 if not m/ \S /x;
            next;
        }

        chomp;
        my ( $module, $author, $perm ) = split m/\s* , \s*/x;
        push @{$perms->{$module}{maint}}, $author;
        if ( $perm eq 'f' || $perm eq 'm' ) {
            if ( exists $perms->{$module}{auth} ) {
                my $o_author = $perms->{$module}{auth};
                my $o_perm = $perm eq 'f' ? 'm' : 'f';
                if ( $o_author eq $author ) {
                    warn "duplicate primary on $module (same ID: $author)\n"
                }
                elsif ( $perm = 'm' ) {
                    warn "duplicate primary on $module (m:$author f:$o_author)\n"
                }
                else {
                    warn "duplicate primary on $module (m:$o_author f:$author)\n"
                }
                $perms->{$module}{auth} = $perm eq 'f' ? $author : $o_author;
            }
            else {
                $perms->{$module}{auth} =  $author;
            }
        }
    }

    return %$perms;
}
