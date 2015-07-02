#!/usr/bin/env perl
use v5.10;
use utf8;
use strict;
use warnings;

use MongoDB;
use boolean;
use utf8;

my $mc   = MongoDB::MongoClient->new;
my $coll = $mc->ns("cpan.river");

my $c = $coll->find();

my %dists;
my %delta;

while ( my $d = $c->next ) {
    $dists{ $d->{_id} } = $d;
    my $dep_count = scalar keys %{ $d->{downriver_dists} };
    next unless $dep_count > 1;
    my %first  = ( k => '', v => -1 );
    my %second = ( k => '', v => -2 );
    local $SIG{__WARN__} = sub { warn "Warning in $d->{_id}: " . shift };
    while ( my ( $k, $v ) = each %{ $d->{downriver_dists} } ) {
        $v ||= 0;
        if ( $v > $first{v} ) {
            @second{qw/k v/} = @first{qw/k v/};
            @first{qw/k v/} = ( $k, $v );
        }
        elsif ( $v > $second{v} ) {
            @second{qw/k v/} = ( $k, $v );
        }
    }
    $delta{ $d->{_id} }{delta} = $first{v} - $second{v};
    $delta{ $d->{_id} }{dist}  = "$d->{_id} $d->{downriver_count}";
    $delta{ $d->{_id} }{report} =
      sprintf( "%-40s %-40s", "$first{k} $first{v}", "$second{k} $second{v}" );
}

#say "Count|Distribution|Maintainers|Top 3 Downstream";
##say "-----------------------------------------------";
for my $d ( sort { $delta{$b}{delta} <=> $delta{$a}{delta} } keys %delta ) {
    printf( "%-40s %s\n", $delta{$d}{dist}, $delta{$d}{report} );
}

