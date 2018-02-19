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

my $c = $coll->find({_latest => true})->fields({
        name => 1, _maintainers => 1, _upstream => 1, _core => 1,
        _uploader => 1,
});

my %maint;
my %core;
my %uploaders;

# Iterate through the collection, (a) establishing a vertex in the Graph
# object for each distribution, (b) populating lookup tables for a it's
# maintainers and "core-ness" and (c) establishing edges to that vertex
# representing reverse dependencies, i.e., A, B and C are dependent on X.

while ( my $doc = $c->next ) {
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
if ($verbose) {
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
for my $v ( $g->vertices ) {
    if (! defined $uploaders{$v}) { push @lack_uploaders, $v; }
    $qp{$v} = 0;
    my @revdeps = $g->all_successors($v);
    $revdepcounts{$v} = scalar(@revdeps);
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
if ($verbose) {
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
if ($verbose) {
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

    if ($verbose) {
        my $this_module = 'List-Compare';
        my @this_all_successors = $g->all_successors($this_module);
        my @this_all_predecessors = $g->all_predecessors($this_module);
        say STDERR "$this_module: all_successors";
        say STDERR "@this_all_successors";
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
    [ "Count", "Distribution", "Upstream Status",
        "Maintainers", "Top $show_downstream Downstream" ]
);
for my $d ( sort {
        $qp{$b} <=> $qp{$a} ||
        $revdepcounts{$b} <=> $revdepcounts{$a} ||
        $a cmp $b
    } keys %revdepcounts ) {
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

