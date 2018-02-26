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
use Data::Dump qw(dd pp);

=pod

    perl compute_downstream_dag.pl \
        --csvout=/path/to/output.csv \
        --show_downstream=5 \
        --sort=trad \
        --verbose \
        --debug 2>dump

=cut

my ($csvout, $show_downstream, $sort_formula, $verbose, $debug) = ('') x 5;
GetOptions(
    "csvout=s"          => \$csvout,
    "show_downstream=i" => \$show_downstream,
    "sort=s"            => \$sort_formula,
    "verbose"           => \$verbose,
    "debug"             => \$debug,
) or croak("Error in command line arguments");
$csvout ||= 'output.csv';
$show_downstream ||= 5;
my %eligible_sort_formulas = map {$_ => 1} qw( trad qp );
croak "Formula for sort must be 'trad' or 'qp'"
    unless $eligible_sort_formulas{$sort_formula};

my $mc = MongoDB::MongoClient->new;

# MongoDB::MongoClient->get_namespace():
#   get a MongoDB::Collection instance for the given namespace
#   convention: "database:collection"

my $coll = $mc->get_namespace("cpan.meta");     # Created in calculate-upstream-from-meta.pl
my $rivercoll = $mc->get_namespace("cpan.river");

# MongoDB::Collection->drop(): deletes a collection as well as all of its indexes

$rivercoll->drop;

# MongoDB::Collection->find(): executes a query with a filter expression;
# returns a MongoDB::Cursor object.

my $cursor = $coll->find({_latest => true})->fields({
    name            => 1,
    _maintainers    => 1,
    _upstream       => 1,
    _core           => 1,
    _uploader       => 1,
});

my $g = Graph->new;
my %maint;
my %core;
my %uploaders;

# Iterate through the collection, (a) establishing a vertex in the Graph
# object for each distribution, (b) populating lookup tables for a it's
# maintainers and "core-ness" and (c) establishing edges to that vertex
# representing reverse dependencies, i.e., A, B and C are dependent on X.

while ( my $doc = $cursor->next ) {
    my $to = $doc->{name}
        or next;
    $g->add_vertex($to);
    $maint{$to} = $doc->{_maintainers} // [];
    $core{$to} = $doc->{_core} // '';
    $uploaders{$to} = $doc->{_uploader} // '';
    if ( my $arcs = $doc->{_upstream} ) {
        # foreach distribution in @$arcs, $_ is dependent on $to
        $g->add_edge( $_, $to ) for @$arcs;
    }
}
if ($debug) {
    say STDERR "\%maint";
    for my $k (sort keys %maint) { say STDERR join('|' => $k, join(' ' => @{$maint{$k}})); }
    say STDERR "";
    say STDERR "\%core";
    for my $k (sort keys %core) { say STDERR join('|' => $k, $core{$k}); }
    say STDERR "";
}

my %revdepcounts;
my %qp;
my @lack_uploaders = ();
VERTICES: for my $v ( $g->vertices ) {
    if (! defined $uploaders{$v}) {
        push @lack_uploaders, $v;
    }
    $qp{$v} = 0;
    my @revdeps = $g->all_successors($v);
    $revdepcounts{$v} = scalar(@revdeps);
    next VERTICES if $sort_formula eq 'trad';

    for my $distro (@revdeps) {
        for my $maintainer (@{$maint{$distro}}) {
            if (! defined $maintainer) {
                say STDERR "MMM: maintainer not defined for $distro";
                next;
            }
            if ((defined $uploaders{$v}) and ($maintainer ne $uploaders{$v})) {

                # Distros without a defined uploader in MongoDB, per above
                # statement, retain qp 0.
                # Have to decide whether that is feature or bug from point of
                # view of using in test-against-dev.

                $qp{$v} = 1;
                last;
            }
        }
    }
}
if ($debug) {
    say STDERR "\%revdepcounts after invoking all_successors";
    for my $k (sort keys %revdepcounts) { say STDERR join('|' => $k, $revdepcounts{$k}); }
    say STDERR "";
    say STDERR "\@lack_uploaders";
    if (@lack_uploaders) { pp( [ sort @lack_uploaders ] ); }
    say STDERR "";
}

#####
my %alt = ();
for my $v ( $g->vertices ) {
    $alt{$v} =()= $g->all_predecessors($v);
}
if ($debug) {
    say STDERR "\%alt after invoking all_predecessors";
    for my $k (sort keys %alt) { say STDERR join('|' => $k, $alt{$k}); }
    say STDERR "";
}
#####


my $bulk = $rivercoll->unordered_bulk;

# %top is a table in which we will look up a given distro's $show_downstream
# top revdeps.  We will use that in printing the CSV output file, but first we
# store that information back in the MongoDB.

my %top;
for my $v ( $g->vertices ) {
    $top{$v} = [
      map { "$_->[0]:$_->[1]" }
      sort { $b->[1] <=> $a->[1] || $a->[0] cmp $b->[0] }
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

#####

{
    # The following demonstrates that, with respect to the setup in this program,
    # 'all_successors' are all the distros that are dependent upon the module in
    # question -- the "revdeps".
    # 'all_predecessors' are all the distros upon which the module in question
    # itself has a dependency.

    if ($debug) {
        my $this_module = 'List-Compare';
        my @this_all_successors = $g->all_successors($this_module);
        my @this_all_predecessors = $g->all_predecessors($this_module);
        say STDERR "$this_module: all_successors";
        say STDERR "@this_all_successors";
        say STDERR "";
        say STDERR "$this_module: all_predecessors";
        say STDERR "@this_all_predecessors";
        say STDERR "";
        say STDERR "$this_module: immediate downriver dists:count of their all_successors";
        say STDERR "@{$top{$this_module}}";
        say STDERR "";
    }
}

#####

my $csv = Text::CSV->new( {
    binary => 1,
    eol => "\n",
    sep_char => ',',
} ) or croak "Cannot use CSV: " . Text::CSV->error_diag ();

open my $FH, ">:encoding(utf8)", $csvout
    or croak "Unable to open $csvout for writing";
$csv->print(
    $FH,
    [ "count", "distribution", "core_upstream_status",
        "maintainers", "top_${show_downstream}_downstream" ]
);

my %sort_routines = (
    qp   => sub {
        $qp{$b} <=> $qp{$a} || $revdepcounts{$b} <=> $revdepcounts{$a} || $a cmp $b
    },
    trad => sub {
                               $revdepcounts{$b} <=> $revdepcounts{$a} || $a cmp $b
    },
);
my @sorted_keys = sort { &{$sort_routines{$sort_formula}} } keys %revdepcounts;
for my $d ( @sorted_keys ) {
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

