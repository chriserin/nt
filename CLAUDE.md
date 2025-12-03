# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`nt` is a Rust CLI for number theory computations with three commands:
- **primes**: Generate primes using Sieve of Eratosthenes (2 variations)
- **primes-bases**: Display primes in bases 2-62 and detect palindromes
- **pi**: Calculate π to arbitrary precision and scan for primes

## Commands

```bash
# Build and test
cargo build --release
cargo test
cargo test test_name          # Run specific test

# Run commands
cargo run -- primes 1000 --variation 2    # Variation 2 is ~40% faster
cargo run -- primes-bases --pal-only      # Show only palindromes
cargo run -- pi 1000                      # Calculate π to 1000 digits
```

## Architecture

**Data Flow**: `primes` command saves to `XDG_DATA_HOME/nt/primes.txt` → `primes-bases` and `pi` commands read from this file.

**primes.rs**: Two Sieve of Eratosthenes implementations:
- v1: Basic sieve processing all numbers
- v2: Optimized odd-only sieve, half the memory, ~40% faster

**primes_bases.rs**: Converts primes to bases 2-62 (0-9, A-Z, a-z) and detects palindromes. Base conversion for bases >36 uses lowercase (36='a', 61='z'). Palindromes require 2+ characters.

**pi.rs**: Uses Machin's formula (π/4 = 4*arctan(1/5) - arctan(1/239)) with arbitrary-precision arithmetic (`rug` crate). Scans π digits for 4+ digit prime occurrences.

**storage.rs**: XDG-compliant persistence at `XDG_DATA_HOME/nt/` (fallback: `~/.local/share/nt/`):
- `{number}.txt`: Individual property files
- `primes.txt`: Newline-separated prime list
- `execution_log.txt`: Timestamped performance logs

## Key Dependencies

- `clap`: CLI parsing
- `rug`: Arbitrary-precision arithmetic (wraps GMP/MPFR)
- `chrono`: Timestamps
