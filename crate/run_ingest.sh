#!/bin/bash

for URL in $(< ingest/todo.txt); do
    cargo run --release --bin ingest -- -DMv $URL
done
