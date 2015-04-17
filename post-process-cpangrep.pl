#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use HTTP::Tiny;
use JSON::MaybeXS;
use Path::Tiny;

my $ua    = HTTP::Tiny->new;
my $dists = decode_json( path("dists.json")->slurp );

STDOUT->autoflush(1);

while (<>) {
    chomp;
    s{^([A-Z]+/[^/]+)/.*}{$1};
    for my $s (qw/.tar.gz .tgz .tar.bz .zip/) {
        my $distfile = "$_$s";
        ( my $url = $distfile ) =~ s{(((.).).*)}{$3/$2/$1};
        say $distfile
          if $ua->head("http://www.cpan.org/authors/id/$url")->{success}
          && exists $dists->{$distfile};
    }
}

