# Installation

## Requirements

- Rust toolchain (stable)

## Install from source

```bash
git clone https://github.com/tokuhirom/murodb.git
cd murodb
cargo install --path .
```

This installs the `murodb` binary to `~/.cargo/bin/`.

## Build only

```bash
cargo build --release
```

The binary will be at `target/release/murodb`.
