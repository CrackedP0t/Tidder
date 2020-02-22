# Tidder
## *Reddit backwards*
***
Tidder is a reverse image website for Reddit. It uses the power of the [DHash algorithm](https://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html) in conjunction with the PostgreSQL extension [`pg-spgist_hamming`](https://github.com/fake-name/pg-spgist_hamming) to efficiently hash, store, and search over 60 million images. It can find identical images as well as visually similar but non-identical ones with a low false positive rate.

The entire service is written in Rust, for its safety and ergonomics as well as its performance capabilities.
