## bgen_reader

Rust library and binary for very fast operations on bgen files

binary
======

The bgen_reader binary implements some common operations on bgen files:

- Reading and writing
- Listing variants
- Indexing (faster than bgenix, see benchmarks)
- Filtering on genomic position and variant id
- Merging on variants

# Examples



library
=======

Bgen_reader supports reading bgen files as an iterator on variants.

Writing to
bgen specification: https://www.chg.ox.ac.uk/~gav/bgen_format/spec/latest.html


benchmark
=========
