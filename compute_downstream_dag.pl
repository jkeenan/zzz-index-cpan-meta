#!/usr/bin/env perl
use v5.10;
use utf8;
use strict;
use warnings;

use MongoDB;
use Graph;
use boolean;
use utf8;
use Getopt::Long;
use Text::CSV;
use Carp;

=pod

    perl compute_downstream_dag.pl \
        --csvout=/path/to/output.csv \
        --show_downstream=5 \
        --verbose

=cut

my ($csvout, $show_downstream, $verbose) = ('') x 3;
GetOptions(
    "csvout=s"          => \$csvout,
    "show_downstream=i" => \$show_downstream,
    "verbose"           => \$verbose,
) or croak("Error in command line arguments");
$csvout ||= 'output.csv';
$show_downstream ||= 5;

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
if ($verbose) {
    say STDERR "\%maint";
    for my $k (sort keys %maint) { say STDERR join('|' => $k, join(' ' => @{$maint{$k}})); }
    say STDERR "";
    say STDERR "\%core";
    for my $k (sort keys %core) { say STDERR join('|' => $k, $core{$k}); }
    say STDERR "";
}

my %revdepcounts;
for my $v ( $g->vertices ) {
    $revdepcounts{$v} =()= $g->all_successors($v);
}
if ($verbose) {
    say STDERR "\%revdepcounts after invoking all_successors";
    for my $k (sort keys %revdepcounts) { say STDERR join('|' => $k, $revdepcounts{$k}); }
    say STDERR "";
}

my $bulk = $rivercoll->unordered_bulk;

my %top;
for my $v ( $g->vertices ) {
    $top{$v} = [
      map { "$_->[0]:$_->[1]" }
      sort { $b->[1] <=> $a->[1] }
      map { [ $_, $revdepcounts{$_} ] }
      $g->successors($v)
    ];
    $bulk->insert_one(
        {
            _id => $v,
            downriver_count => $revdepcounts{$v},
            downriver_dists => { map { split /:/ } @{$top{$v}} },
        }
    );
}
$bulk->execute;

my $csv = Text::CSV->new( {
    binary => 1,
    eol => "\n",
    sep_char => ',',
} ) or croak "Cannot use CSV: " . Text::CSV->error_diag ();

open my $FH, ">:encoding(utf8)", $csvout
    or croak "Unable to open $csvout for writing";
$csv->print(
    $FH,
    [ "Count", "Distribution", "Upstream Status",
        "Maintainers", "Top $show_downstream Downstream" ]
);
for my $d ( sort { $revdepcounts{$b} <=> $revdepcounts{$a} } keys %revdepcounts ) {
    my $row = [
        $revdepcounts{$d},
        $d,
        (     ( $core{$d} || '' ) eq 'cpan'  ? 'cpan-upstream'
            : ( $core{$d} || '' ) eq 'blead' ? 'blead-upstream'
            :                                  '' ),
        join(" ", @{$maint{$d} || []} ),
        join(" ", grep { defined } @{$top{$d} || []}[0 .. ($show_downstream - 1)] ),
    ];
    $csv->print($FH, $row) or croak "Unable to print @$row";
}
close $FH or croak "Unable to close $csvout after writing";

say "See results in: $csvout" if $verbose;

