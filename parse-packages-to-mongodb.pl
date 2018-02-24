#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use Carp;
use File::Spec;
use Getopt::Long;
use IO::Zlib;
use MongoDB;
use Module::CoreList;
use Parse::CPAN::Packages::Fast;
use Tie::IxHash;
use boolean;

=head1 NAME

parse-packages-to-mongodb.pl - store 02 and 06 data to MongoDB

=head1 USAGE

    perl parse-packages-to-mongodb.pl --repository=/path/to/cpan

=head1 PREREQUISITES

What you must install from CPAN:

    MongoDB
    Parse::CPAN::Packages::Fast
    Tie::IxHash
    boolean

What you should have from the Perl 5 core distribution:

    Carp
    File::Spec
    Getopt::Long
    IO::Zlib
    Module::CoreList

=head1 DESCRIPTION

F<parse-packages-to-mongodb.pl> creates a MongoDB B<collection> named
C<packages> which is indexed on the names of Perl packages found on CPAN or in
a F<minicpan> repository.  The collection also includes data from the CPAN
F<modules/06perms.txt.gz> and F<modules/02packages.details.txt.gz> files as
well as Module::CoreList data.

In this context, "packages" are spelled with double-colon separators
(C<List::Compare>) rather than with the hyphens (C<List-Compare>) used for
CPAN distributions.

The collection has information about the package's version, distribution,
etc., as well as the package's first-come authority and maintainers.
Subsequent programs look up data based just on the package name.

=cut

my ($CPAN, $hacking, $verbose) = ('') x 3;
GetOptions(
    "repository=s"      => \$CPAN,
    "hacking"           => \$hacking,
    "verbose"           => \$verbose,
) or croak("Error in command line arguments");

$CPAN ||= File::Spec->catdir($ENV{HOMEDIR}, 'minicpan');
croak "Cannot locate directory '$CPAN' for path to CPAN installation"
    unless (-d $CPAN);

my $perms       = File::Spec->catfile($CPAN, 'modules', '06perms.txt.gz');
say "Parsing 06perms...";
my $fh = IO::Zlib->new( $perms, "rb")
    or croak "Unable to open $perms for reading";
my %pkg_to_maint = _read_perms($fh);
close $fh or croak "Unable to close $perms after reading";

my $packages    = File::Spec->catfile($CPAN, 'modules', '02packages.details.txt.gz');
say "Parsing 02packages...";
my $p = Parse::CPAN::Packages::Fast->new($packages)
    or croak "Unable to parse $packages";

my %latest = map { $_ => true } map { s{^./../}{}r } map { $_->pathname } $p->latest_distributions;

my ($DB, $COLL, $mc, $coll) = ('') x 4;
#my $DB   = 'cpan';
#my $COLL = 'packages';
#my $mc   = MongoDB::MongoClient->new;
#my $coll = $mc->get_database($DB)->get_collection($COLL);
#$coll->drop;
#$coll->indexes();

#my $bulk = $coll->unordered_bulk;

my $cnt = 0;
say "Iterating packages...";
STDOUT->autoflush(1);
for my $pkg ( $p->packages ) {
    say "XXX: <$pkg>";
    print "." if ++$cnt % 100 == 0;
    my $p = $p->package($pkg);
    my $d = $p->distribution;
    my $distfile = $d->pathname =~ s{^./../}{}r;
    my $core = Module::CoreList->is_core($pkg);
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
        ( $core ? ( distcore => $upstream ) : () ),
    );
    #    $bulk->insert_one($doc);
}

#say "Sending to database";
#$bulk->execute;

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
