# HDRS - Hierarchical Delta-RLE Compression for Financial Time Series

[![CI](https://github.com/ngullotta/hdrs/actions/workflows/rust.yml/badge.svg)](https://github.com/ngullotta/hdrs/actions/workflows/rust.yml)

A high-performance Rust library for compressing multi-symbol financial time
series data with built-in integrity verification.

This is just a pet project (and my first in rust), don't take it seriously.

## Features

- **High compression ratio** on _typical_ financial data
- **Microsecond-level** serialization and deserialization
- **Checksum verification** (CRC32)
- **Multi-symbol support** - compress multiple instruments simultaneously
- **Lossless with controlled precision** - basis point accuracy
- **Fail-fast corruption detection**

## How It Works

HDRS exploits three key properties of financial data:

1. **Temporal correlation** - Prices change in small increments _most of the
   time_
2. **Cross-symbol correlation** - Related assets often move together
3. **Sparse significant events** - Large moves are rare

### Compression Architecture

```
┌─────────────────────────────────────────────────────┐
│ Layer 1: Reference Frame                            │
│ • Full precision baseline at T₀                     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ Layer 2: Variable-Bit Delta Encoding                │
│ • 4 bits:  ±7 basis points (common case)            │
│ • 8 bits:  ±127 basis points (moderate moves)       │
│ • 32 bits: Full precision (rare large moves)        │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ Layer 3: Symbol Interleaving                        │
│ • Bitmap indicates which symbols changed            │
│ • Only store deltas for changed symbols             │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ Layer 4: Integrity Checksums (CRC32)                │
│ • Reference frame checksum                          │
│ • Compressed data checksum                          │
│ • Overall structure checksum                        │
└─────────────────────────────────────────────────────┘
```

## Roadmap

Flesh this out more, drive error down, up compression

## License

This project is licensed under the WTFPL. See `LICENSE` for more details
