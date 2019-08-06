#!/bin/bash

set -e

~/.cargo/bin/cargo build --bin ingest --release

for URL in $(< ingest/todo.txt); do
    target/release/ingest -Dv $URL
done
