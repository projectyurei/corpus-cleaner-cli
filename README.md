# corpus-cleaner-cli

**Separating the Signal from the Noise.**

`corpus-cleaner-cli` is a high-performance data refinery designed to sanitize terabytes of raw Solana transaction logs. It serves as the critical filtration layer between raw ingestion (`yurei-geyser-client`) and the Protocol NAVI tokenization engine.

## ruthlessly Efficient

Built with Rust and Rayon, this tool utilizes every available CPU cycle to process data in parallel. It is designed to be **blazingly fast**.

### Performance
*Benchmark pending. Target: >40x speedup over legacy Python scripts.*

## Features

- **Parallel Processing**: Multi-threaded engine capable of saturating Disk I/O or CPU.
- **Aggressive Filtering**:
  - Drops failed transactions.
  - Eliminates dust/spam transfers.
  - Sanitizes malformed UTF-8.
- **Deduplication**: High-speed hash-based uniqueness checks to prevent data poisoning.
- **Industrial Telemetry**: Real-time progress monitoring with precise throughput metrics.

## Usage

```bash
# Build for Release (Mandatory for performance)
cargo build --release

# Run
./target/release/corpus-cleaner-cli --input <RAW_DATA_DIR> --output <CLEAN_DATA_DIR> --threads <NUM_CORES>
```

**Arguments:**

- `-i, --input <PATH>`: Directory containing raw `.jsonl` or `.parquet` files.
- `-o, --output <PATH>`: Destination for refined logs.
- `-t, --threads <NUM>`: (Optional) Force specific thread count. Defaults to auto-detection.

## Architecture

This tool operates as a pure function: `Raw(TB) -> Filter(dedup, sanitize) -> Clean(GB)`.
It maintains no persistent state between runs (unless configured) and relies on high-speed in-memory structures (`DashMap`) for session-scoped deduplication.
