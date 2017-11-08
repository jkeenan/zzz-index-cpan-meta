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

=head1 NAME

compute_downstream_dag.pl - generate CPAN river

=head1 USAGE

=over 4

=item * Traditional: print to STDOUT, then re-direct to file

    perl compute_downstream_dag.pl 1> cpan.river.txt 2> err

=item * Output to CSV file named F<cpan.river.csv>

    perl compute_downstream_dag.pl --csv 1> /dev/null

=item * Output to CSV file with name provided by user

    perl compute_downstream_dag.pl \
        --csv \
        --file=/path/to/20171107.cpan.river.csv \
        1> /dev/null

=back

=head1 PREREQUISITES

From Perl 5 core distribution:

    Getopt::Long
    utf8

From CPAN:

    Graph
    MongoDB
    Text::CSV  # If invoked with '--csv'
    boolean

=cut

my ($create_csv, $outputfile, $verbose) = ('') x 2;
GetOptions(
    "csv"           => \$create_csv,
    "verbose"       => \$verbose,
    "file=s"        => \$outputfile,
) or die("Error in command line arguments: $!");

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
      sort { $b->[1] <=> $a->[1] || $a->[0] cmp $b->[0] }
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

for my $d ( sort { $nd{$b} <=> $nd{$a} || $a cmp $b } keys %nd ) {
    printf(
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

if ($create_csv) {
    require Text::CSV;
    my $csv = Text::CSV->new( { binary => 1 } )
        or die "Cannot use CSV: " . Text::CSV->error_diag ();
    $csv->eol("\n");
    $outputfile ||= "cpan.river.csv";
    open my $OUT, ">:encoding(utf8)", $outputfile
        or die "Cannot open $outputfile for writing: $!";
    my $header = [ qw(
        count
        distribution
        core_upstream_status
        maintainers
        top_5_downstream
    ) ];
    $csv->print($OUT, $header);
    for my $d ( sort { $nd{$b} <=> $nd{$a} || $a cmp $b } keys %nd ) {
        my $colref = [
            $nd{$d},
            $d,
            (     ( $core{$d} || '' ) eq 'cpan' ? 'cpan-upstream'
                : ( $core{$d} || '' ) eq 'blead' ? 'blead-upstream'
                :                                  '' ),
            join(" ", @{$maint{$d} || []} ),
            join(" ", grep { defined } @{$top{$d} || []}[0..4] ),
        ];
        $csv->print($OUT, $colref);
    }
    close $OUT or die "Cannot close $outputfile after writing: $!";
}
