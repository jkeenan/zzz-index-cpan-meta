#!/usr/bin/env perl
use v5.10;
use utf8;
use strict;
use warnings;

use MongoDB;
use boolean;
use utf8;

my $mc   = MongoDB::MongoClient->new;
my $river_coll = $mc->ns("cpan.river");
my $meta_coll = $mc->ns("cpan.meta");

my %river = map { $_->{_id} => $_->{downriver_count} } $river_coll->find()->all;
my %found;

my $c = $meta_coll->find();

while ( my $d = $c->next ) {
    my $name = $d->{name};
    next unless $name && $river{$name};
    next if !$river{$name} || $river{$name} < 100;
    next if $d->{generated_by} =~ /Module::Build|ExtUtils::MakeMaker/;
    next if exists $d->{prereqs}{configure}{requires}{'Module::Build'};

    if ( $d->{generated_by} =~ /Module::Install/ ) {
        $found{$name} = "generated_by M::I";
    }
    elsif ( $d->{_meta_file} eq 'META.yml' ) {
        $found{$name} = "no META.json";
    }
}

for my $d ( sort { $river{$b} <=> $river{$a} } keys %found ) {
    printf( "%7d %-40s %s\n", $river{$d}, $d, $found{$d} );
}

