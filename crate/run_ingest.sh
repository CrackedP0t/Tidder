#!/bin/bash

set -e
shopt -s extglob

for URL in $(< ~/tidder/crate/ingest/todo.txt); do
    cd ~/tidder/crate/
    ~/.cargo/bin/cargo build --bin ingest --release
    cd /mnt/permanent/archives

    ARCHIVE="${URL##*/}"
    UNPACK="${ARCHIVE%%.*}"
    EXT="${ARCHIVE##*.}"

    if [[ ! -e "$ARCHIVE" ]]; then
        wget $URL
    fi

    if [[ ! -e "$UNPACK" ]]; then
        if [[ "$EXT" == "zst" ]]; then
            zstd -d "$ARCHIVE"
        else
            7z x "$ARCHIVE"
        fi
    fi

    RUST_LOG="info" ~/tidder/crate/target/release/ingest $@ "$UNPACK" | tee -a ~/logs/ingest.log
    tail -n +2 ~/tidder/crate/ingest/todo.txt | sponge ~/tidder/crate/ingest/todo.txt

    rm "$ARCHIVE" "$UNPACK"
done
