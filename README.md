[![Pg-Reindexer CI](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml)

# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. Safely reindex your PostgreSQL indexes with intelligent resource management and comprehensive logging. Put another way: it's a Rust-based command-line tool designed to automate and optimize the critical but often complex task of PostgreSQL index maintenance. Unlike manual reindexing operations that can be risky and time-consuming.

![Reindexer](assets/reindex.gif)

## Table of Contents

- [Quick Start](#quick-start)
  - [Installation](#installation)
  - [Basic Usage](#basic-usage)
- [Usage Examples](#usage-examples)
  - [Basic Operations](#-basic-operations)
  - [Thread Count Variations](#-thread-count-variations)
  - [Memory and Performance Settings](#-memory-and-performance-settings)
  - [Size Filtering](#-size-filtering)
  - [Production Scenarios](#-production-scenarios)
- [Environment Variables](#environment-variables)
- [Command Line Interface](#command-line-interface)
- [Key Features](#key-features)
- [Database Schema](#database-schema)
- [License](#license)
- [Support](#support)

## Quick Start

### Installation

```bash
# Build from source
git clone <repository-url>
cd pg-reindexer
cargo build --release

# Or download binary
wget https://github.com/your-org/pg-reindexer/releases/latest/download/pg-reindexer-x86_64-unknown-linux-gnu
chmod +x pg-reindexer-x86_64-unknown-linux-gnu
```

### Basic Usage

```bash
# See what would be reindexed (always test first!)
pg-reindexer --schema public --dry-run

# Reindex all indexes in a schema
pg-reindexer --schema public

# Reindex indexes for a specific table
pg-reindexer --schema public --table users

# High-performance reindexing
pg-reindexer --schema public --threads 8
```

## Usage Examples

### 🚀 **Basic Operations**

```bash
# Dry run (always test first!)
pg-reindexer --schema public --dry-run


# Specific table reindexing
pg-reindexer --schema public --table users
```

### 🔧 **Thread Count Variations**

```bash
# Production safe (1 thread)
pg-reindexer --schema public --threads 1 

# High performance (8 threads)
pg-reindexer --schema public --threads 8 

# Maximum threads (32)
pg-reindexer --schema public --threads 32 
```

### 💾 **Memory and Performance Settings**

```bash
# Conservative settings
pg-reindexer --schema public --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1

# High performance settings
pg-reindexer --schema public --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4

# Use PostgreSQL defaults
pg-reindexer --schema public --max-parallel-maintenance-workers 0

# High IO concurrency
pg-reindexer --schema public --maintenance-io-concurrency 256
```

### 📊 **Size Filtering**

```bash
# Only medium indexes (max 10GB)
pg-reindexer --schema public --max-size-gb 10

# Large indexes only (min 100GB)
pg-reindexer --schema public --min-size-gb 100

# Size range filtering (1GB to 50GB)
pg-reindexer --schema public --min-size-gb 1 --max-size-gb 50
```

**Note**: The tool will log the index size limits being applied for clarity:
```
Index size limits: minimum 1 GB, maximum 50 GB
```

### 🎯 **Bloat-based Reindexing**

```bash
# Only reindex indexes with bloat ratio >= 15%
pg-reindexer --schema public --reindex-only-bloated 15
```

### 🧹 **Orphaned Index Cleanup**

```bash
# Clean up orphaned _ccnew indexes before reindexing
pg-reindexer --schema public --clean-orphant-indexes

# Combine with other operations
pg-reindexer --schema public --clean-orphant-indexes --threads 4 --maintenance-work-mem-gb 2
```

### 🛡️ **Production Scenarios**

```bash
# Production hours (minimal impact)
pg-reindexer --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1

# Maintenance window (high performance)
pg-reindexer --schema public --threads 8 --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4 --maintenance-io-concurrency 256

# Emergency operation (maximum safety)
pg-reindexer --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --skip-active-vacuums
```


## Environment Variables

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=postgres
export PG_USER=postgres
export PG_PASSWORD=mypassword

# Then run without connection parameters
pg-reindexer --schema public 
```

## Command Line Interface

```bash
PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table

Usage: pg-reindexer [OPTIONS] --schema <SCHEMA>

Options:
  -H, --host <HOST>                                    PostgreSQL host (can also be set via PG_HOST environment variable)
  -p, --port <PORT>                                     PostgreSQL port (can also be set via PG_PORT environment variable)
  -d, --database <DATABASE>                             Database name (can also be set via PG_DATABASE environment variable)
  -U, --username <USERNAME>                             Username (can also be set via PG_USER environment variable)
  -P, --password <PASSWORD>                             Password (can also be set via PG_PASSWORD environment variable)
  -s, --schema <SCHEMA>                                 Schema name to reindex (required)
  -t, --table <TABLE>                                   Table name to reindex (optional - if not provided, reindexes all indexes in schema)
  -f, --dry-run                                         Dry run - show what would be reindexed without actually doing it
  -n, --threads <THREADS>                               Number of concurrent threads for reindexing (default: 2, max: 32) [default: 2]
  -i, --skip-inactive-replication-slots                 Skip inactive replication slots check
  -r, --skip-sync-replication-connection                Skip sync replication connection check
      --skip-active-vacuums                              Skip active vacuum check
  -m, --max-size-gb <MAX_SIZE_GB>                      Maximum index size in GB. Indexes larger than this will be excluded from reindexing [default: 1024]
      --min-size-gb <MIN_SIZE_GB>                      Minimum index size in GB. Indexes smaller than this will be excluded from reindexing [default: 0]
  -w, --maintenance-work-mem-gb <MAINTENANCE_WORK_MEM_GB>  Maximum maintenance work mem in GB (max: 32 GB) [default: 1]
  -x, --max-parallel-maintenance-workers <MAX_PARALLEL_MAINTENANCE_WORKERS>  Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety. Use 0 for PostgreSQL default (typically 2) [default: 2]
  -c, --maintenance-io-concurrency <MAINTENANCE_IO_CONCURRENCY>  Maintenance IO concurrency. Controls the number of concurrent I/O operations during maintenance operations [default: 10]
  -l, --log-file <LOG_FILE>                             Log file path (default: reindexer.log in current directory) [default: reindexer.log]
      --reindex-only-bloated <PERCENTAGE>               Reindex only indexes with bloat ratio above this percentage (0-100). If not specified, all indexes will be reindexed
      --concurrently                                     Use REINDEX INDEX CONCURRENTLY for online reindexing. Set to false to use offline reindexing (REINDEX INDEX) [default: true]
      --clean-orphant-indexes                            Drop orphaned _ccnew indexes (temporary concurrent reindex indexes) before starting the reindexing process. These indexes are created by PostgreSQL during REINDEX INDEX CONCURRENTLY operations and may be left behind if the operation was interrupted.
  -h, --help                                            Print help
  -V, --version                                         Print version
```

## Key Features

### 🎯 **Granular Maintenance Control**
- **Schema-level Reindexing**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Table-level Reindexing**: Target specific tables for focused maintenance cycles
- **Flexible Scheduling**: Create different maintenance strategies for different schemas/tables
- **B-tree Focus**: Optimized for the most common index type in PostgreSQL
- **Constraint Awareness**: Automatically skips primary keys and unique constraints.

### ⚡ **Concurrent Operations with Safety**
- **Flexible Reindexing Modes**: Choose between online (`REINDEX INDEX CONCURRENTLY`) or offline (`REINDEX INDEX`) reindexing
- **Non-blocking Reindexing**: Uses `REINDEX INDEX CONCURRENTLY` by default to minimize downtime
- **Smart Threading**: Multiple threads for different tables, but protects same table from concurrent operations
- **Configurable Concurrency**: 1-32 threads with automatic validation against PostgreSQL limits

### 🛡️ **Built-in Safety Checks**
- **Active Vacuum Detection**: Automatically skips reindexing when manual vacuum operations are active (excludes autovacuum)
- **Replication Safety**: Checks for inactive replication slots to prevent WAL size issues
- **Sync Replica Protection**: Stops operations when sync replication is detected to prevent primary unresponsiveness
- **Per-Thread Validation**: Each thread performs fresh safety checks when it starts (no stale data)
- **Orphaned Index Cleanup**: Automatically detects and optionally drops orphaned `_ccnew` indexes left behind by interrupted concurrent reindex operations

### 📊 **Intelligent Bloat Detection**
- **Bloat Ratio Calculation**: Uses PostgreSQL's internal statistics to calculate index bloat percentage
- **Threshold-Based Filtering**: Only reindex indexes that exceed the specified bloat threshold
- **Efficient Maintenance**: Focus resources on indexes that actually need reindexing
- **Configurable Sensitivity**: Set bloat threshold from 0-100% to match your maintenance strategy

### 🔧 **Performance Optimization**
- **Configurable GUCs**: Set PostgreSQL parameters for optimal performance:
  - `maintenance_work_mem`: Control memory allocation for index operations (max: 32 GB)
  - `maintenance_io_concurrency`: Manage concurrent I/O operations (max: 512)
  - `max_parallel_maintenance_workers`: Control parallel worker count
- **Resource Management**: Balance performance vs. system resource consumption
- **Smart Defaults**: Uses PostgreSQL defaults when parameters are set to 0

## Database Schema

The tool automatically creates a `reindexer` schema with a `reindex_logbook` table to track reindexing operations:

```sql
CREATE SCHEMA reindexer;

CREATE TABLE reindexer.reindex_logbook (
    schema_name VARCHAR(255) NOT NULL,
    index_name VARCHAR(255) NOT NULL,
    index_type VARCHAR(255) NOT NULL,
    reindex_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reindex_status VARCHAR(255) NOT NULL,
    before_size BIGINT,
    after_size BIGINT,
    size_change BIGINT
);
```

### Logbook Status Values

- `success`: Index was successfully reindexed and validated
- `validation_failed`: Index reindexing completed but validation failed
- `failed`: Index reindexing failed due to SQL errors, connection issues, or task panics
- `skipped`: Index was skipped due to active vacuum, inactive replication slots, or sync replication
- `below_bloat_threshold`: Index was skipped because its bloat ratio was below the specified threshold
- `invalid_index`: Index was skipped because it was found to be invalid

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on the project repository.
