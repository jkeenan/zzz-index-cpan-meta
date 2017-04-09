#!/usr/bin/env perl
use v5.10;
use utf8;
use strict;
use warnings;

use MongoDB;
use Graph;
use boolean;
use utf8;

my $g = Graph->new;

my $mc = MongoDB::MongoClient->new;
my $coll = $mc->ns("cpan.meta");
my $rivercoll = $mc->ns("cpan.river");
$rivercoll->drop;

my $c = $coll->find({_latest => true})->fields({name => 1, _maintainers => 1, _upstream => 1, _core => 1});

my %maint;
my %core;

while ( my $doc = $c->next ) {
    my $to = $doc->{name}
        or next;
    $g->add_vertex($to);
    $maint{$to} = $doc->{_maintainers} // [];
    $core{$to} = $doc->{_core} // '';
    if ( my $arcs = $doc->{_upstream} ) {
        $g->add_edge( $_, $to ) for @$arcs;
    }
}

my %nd;
for my $v ( $g->vertices ) {
    $nd{$v} =()= $g->all_successors($v);
}

my $bulk = $rivercoll->unordered_bulk;

my %top;
for my $v ( $g->vertices ) {
    $top{$v} = [
      map { "$_->[0]:$_->[1]" }
      sort { $b->[1] <=> $a->[1] }
      map { [ $_, $nd{$_} ] }
      $g->successors($v)
    ];
    $bulk->insert_one(
        {
            _id => $v,
            downriver_count => $nd{$v},
            downriver_dists => { map { split /:/ } @{$top{$v}} },
        }
    );
}

$bulk->execute;


##say "Count|Distribution|Maintainers|Top 3 Downstream";
##say "-----------------------------------------------";
for my $d ( sort { $nd{$b} <=> $nd{$a} } keys %nd ) {
    printf(
##        "%6d|%s%s|[%s]|(%s)\n",
        "%6d %s%s   [%s]   (%s)\n",
        $nd{$d},
        $d,
        (     ( $core{$d} || '' ) eq 'cpan' ? ' <cpan-upstream> '
            : ( $core{$d} || '' ) eq 'blead' ? ' <blead-upstream> '
            :                                  '' ),
        join(" ", @{$maint{$d} || []} ),
        join(" ", grep { defined } @{$top{$d} || []}[0..4] ),
    );
}

