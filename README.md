## How to generate CPAN river output

1. run `parse-packages-to-mongodb.pl` to store 02 and 06 data to MongoDB
2. run `calculate-upstream-from-meta.pl` to index META with upstream data
3. run `compute_downstream_dag.pl` to generate river and print it out
