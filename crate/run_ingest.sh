#!/bin/bash

for URL in $(< ingest/todo.txt); do
    ~/.cargo/bin/cargo run --release --bin ingest -- -DMv $URL
done
