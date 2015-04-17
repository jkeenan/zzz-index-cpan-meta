#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use CPAN::Meta;
use CPAN::Visitor;
use MongoDB;

my $CPAN = shift || '/srv/cpan';

my $visitor = CPAN::Visitor->new( cpan => $CPAN );
my $mc = MongoDB::MongoClient->new;
my $coll = $mc->get_database("cpan")->get_collection("meta");
$coll->drop;
$coll->ensure_index( [ flat_prereqs => 1 ] );

# Prepare to visit all distributions
$visitor->select();

# Action is specified via a callback
$visitor->iterate(
    visit => sub {
        my $job = shift;
        my ($meta_file) = grep { -f $_ } qw/META.json META.yml/;
        return unless $meta_file;

        my $meta = eval { CPAN::Meta->load_file( $meta_file, { lazy_validation => 1 } ) };
        return unless $meta;

        my $prereqs = $meta->effective_prereqs->merged_requirements->as_string_hash;

        my $doc = $meta->as_struct;
        $doc->{flat_prereqs} = [ sort keys %$prereqs ];

        $coll->insert($doc);
    }
);

