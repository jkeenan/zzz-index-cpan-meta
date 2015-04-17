#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use boolean;
use version;
use MongoDB;

my $mc = MongoDB::MongoClient->new;

my @dists = $mc->db("cpan")->coll("meta")->find(
    {
        "prereqs.configure.requires.ExtUtils::MakeMaker" => { '$exists' => true },
        "prereqs.build.requires.ExtUtils::MakeMaker"     => { '$exists' => true },
    }
  )->fields(
    {
        _id                                              => 1,
        "prereqs.configure.requires.ExtUtils::MakeMaker" => 1,
        "prereqs.build.requires.ExtUtils::MakeMaker"     => 1,
    }
  )->all;

use DDP;
for my $d (@dists) {
    my $cr =  $d->{prereqs}{configure}{requires}{"ExtUtils::MakeMaker"};
    my $br =  $d->{prereqs}{build}{requires}{"ExtUtils::MakeMaker"};
    printf("CR:%10s BR:%10s %s\n" ,$cr, $br, $d->{_id})
        if version->new($cr) > version->new($br);
}

