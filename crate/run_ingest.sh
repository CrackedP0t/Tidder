#!/bin/bash

set -e

~/.cargo/bin/cargo build --bin ingest --release

for URL in $(< ingest/todo.txt); do
    target/release/ingest -D $URL
    tail -n +2 ingest/todo.txt | sponge todo.txt
done
