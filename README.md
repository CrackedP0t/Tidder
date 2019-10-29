# Tidder
## *Reddit backwards*
***
Tidder is (will be) a reverse image website for Reddit. It uses the [DHash algorithm](https://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html) in conjunction with the PostgreSQL extension [`pg-spgist_hamming`](https://github.com/fake-name/pg-spgist_hamming) to efficiently hash, store, and look up similar images from user input.

The entire backend is written in Rust, for its safety and ergonomics as well as its performance capabilities.
