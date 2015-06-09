#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use IO::Zlib;
use Module::CoreList::More;
use Parse::CPAN::Packages::Fast;

my $CPAN = shift || "/srv/cpan";

say "Parsing 02packages...";
my $p = Parse::CPAN::Packages::Fast->new("$CPAN/modules/02packages.details.txt.gz");

say "Parsing 06perms...";
my $fh = IO::Zlib->new( "$CPAN/modules/06perms.txt.gz", "rb" );

my $inheader = 1;
my $perms    = {};

STDOUT->autoflush(1);

while (<$fh>) {

    if ($inheader) {
        $inheader = 0 if not m/ \S /x;
        next;
    }

    chomp;
    my ( $module, $author, $perm ) = split m/\s* , \s*/x;
    my $core = Module::CoreList::More::is_core($module) ? " CORE " : "";
    push @{$perms->{$module}{maint}}, $author;
    if ( $perm eq 'f' || $perm eq 'm' ) {
        if ( exists $perms->{$module}{auth} ) {
            my $dist = $p->{pkg_to_dist}{$module} // "NOT IN 02packages";
            my $o_author = $perms->{$module}{auth};
            my $o_perm = $perm eq 'f' ? 'm' : 'f';
            if ( $o_author eq $author ) {
                say "$module (same ID: $author)"
            }
            elsif ( $perm = 'm' ) {
                say "$module (m:$author f:$o_author) $core$dist"
            }
            else {
                say "$module (m:$o_author f:$author) $core$dist"
            }
            $perms->{$module}{auth} = $perm eq 'f' ? $author : $o_author;
        }
        else {
            $perms->{$module}{auth} =  $author;
        }
    }
}
