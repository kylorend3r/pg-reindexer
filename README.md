[![Pg-Reindexer CI](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml)

# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. Safely reindex your PostgreSQL indexes with intelligent resource management and comprehensive logging.

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
pg-reindexer --schema public --verbose

# Reindex indexes for a specific table
pg-reindexer --schema public --table users --verbose

# High-performance reindexing
pg-reindexer --schema public --threads 8 --verbose
```

## Usage Examples

### 🚀 **Basic Operations**

```bash
# Dry run (always test first!)
pg-reindexer --schema public --dry-run

# Basic operation with verbose output
pg-reindexer --schema public --verbose

# Specific table reindexing
pg-reindexer --schema public --table users --verbose
```

### 🔧 **Thread Count Variations**

```bash
# Production safe (1 thread)
pg-reindexer --schema public --threads 1 --verbose

# High performance (8 threads)
pg-reindexer --schema public --threads 8 --verbose

# Maximum threads (32)
pg-reindexer --schema public --threads 32 --verbose
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
pg-reindexer --schema public --max-size-gb 100
```

### 🎯 **Bloat-based Reindexing**

```bash
# Only reindex indexes with bloat ratio >= 15%
pg-reindexer --schema public --reindex-only-bloated 15
```

### 🛡️ **Production Scenarios**

```bash
# Production hours (minimal impact)
pg-reindexer --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1

# Maintenance window (high performance)
pg-reindexer --schema public --threads 8 --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4 --maintenance-io-concurrency 256

# Emergency operation (maximum safety)
pg-reindexer --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection
```


## Environment Variables

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=postgres
export PG_USER=postgres
export PG_PASSWORD=mypassword

# Then run without connection parameters
pg-reindexer --schema public --verbose
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
  -v, --verbose                                         Verbose output
  -i, --skip-inactive-replication-slots                 Skip inactive replication slots check
  -r, --skip-sync-replication-connection                Skip sync replication connection check
  -m, --max-size-gb <MAX_SIZE_GB>                      Maximum index size in GB. Indexes larger than this will be excluded from reindexing [default: 1024]
  -w, --maintenance-work-mem-gb <MAINTENANCE_WORK_MEM_GB>  Maximum maintenance work mem in GB (max: 32 GB) [default: 1]
  -x, --max-parallel-maintenance-workers <MAX_PARALLEL_MAINTENANCE_WORKERS>  Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety. Use 0 for PostgreSQL default (typically 2) [default: 2]
  -c, --maintenance-io-concurrency <MAINTENANCE_IO_CONCURRENCY>  Maintenance IO concurrency. Controls the number of concurrent I/O operations during maintenance operations [default: 10]
  -l, --log-file <LOG_FILE>                             Log file path (default: reindexer.log in current directory) [default: reindexer.log]
      --reindex-only-bloated <PERCENTAGE>               Reindex only indexes with bloat ratio above this percentage (0-100). If not specified, all indexes will be reindexed
      --concurrently                                     Use REINDEX INDEX CONCURRENTLY for online reindexing. Set to false to use offline reindexing (REINDEX INDEX) [default: true]
  -h, --help                                            Print help
  -V, --version                                         Print version
```

## Key Features

### 🎯 **Granular Maintenance Control**
- **Schema-level reindexing**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Table-level precision**: Target specific tables for focused maintenance cycles
- **Flexible scheduling**: Create different maintenance strategies for different schemas/tables
- **B-tree focus**: Optimized for the most common index type in PostgreSQL
- **Constraint awareness**: Automatically skips primary keys and unique constraints (which can't use CONCURRENTLY)

### ⚡ **Concurrent Operations with Safety**
- **Flexible reindexing modes**: Choose between online (`REINDEX INDEX CONCURRENTLY`) or offline (`REINDEX INDEX`) reindexing
- **Non-blocking reindexing**: Uses `REINDEX INDEX CONCURRENTLY` by default to minimize downtime
- **Smart threading**: Multiple threads for different tables, but protects same table from concurrent operations
- **Deadlock prevention**: Intelligent concurrent operations that allow multiple tables but protect same table from simultaneous reindex operations
- **Configurable concurrency**: 1-32 threads with automatic validation against PostgreSQL limits

### 🛡️ **Built-in Safety Checks**
- **Active process detection**: Automatically skips reindexing when vacuums or other maintenance processes are active
- **Replication safety**: Checks for inactive replication slots to prevent WAL size issues
- **Sync replica protection**: Stops operations when sync replication is detected to prevent primary unresponsiveness
- **Configurable overrides**: Use CLI arguments to manage safety check behavior
- **Thread validation**: Automatic validation of thread count and PostgreSQL worker limits

### 📊 **Intelligent Bloat Detection**
- **Bloat ratio calculation**: Uses PostgreSQL's internal statistics to calculate index bloat percentage
- **Threshold-based filtering**: Only reindex indexes that exceed the specified bloat threshold
- **Efficient maintenance**: Focus resources on indexes that actually need reindexing
- **Configurable sensitivity**: Set bloat threshold from 0-100% to match your maintenance strategy

### 🔧 **Performance Optimization**
- **Configurable GUCs**: Set PostgreSQL parameters for optimal performance:
  - `maintenance_work_mem`: Control memory allocation for index operations
  - `maintenance_io_concurrency`: Manage concurrent I/O operations
  - `max_parallel_maintenance_workers`: Control parallel worker count
- **Resource management**: Balance performance vs. system resource consumption
- **Smart defaults**: Uses PostgreSQL defaults when parameters are set to 0

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
- `skipped`: Index was skipped due to active vacuum or other pgreindexer processes
- `below_bloat_threshold`: Index was skipped because its bloat ratio was below the specified threshold
- `invalid_index`: Index was skipped because it was found to be invalid

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on the project repository.
