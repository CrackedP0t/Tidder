#!/bin/bash

for URL in $(< ingest/pushshift_urls.txt); do
    cargo run --release --bin ingest -- -DMv $URL
done
