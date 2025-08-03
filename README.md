# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. This tool provides safe, efficient, and controlled reindexing operations with intelligent resource management and comprehensive logging.

## Table of Contents

- [Purpose and Definition](#purpose-and-definition)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Build from Source](#build-from-source)
  - [Download Binary](#download-binary)
- [Command Line Interface](#command-line-interface)
- [Usage](#usage)
  - [Environment Variables](#environment-variables)
  - [Basic Operations](#basic-operations)
- [Production System Protection](#production-system-protection)
- [Database Schema](#database-schema)
- [License](#license)
- [Support](#support)

## Purpose and Definition

PostgreSQL's native `REINDEX` command is powerful but lacks the orchestration capabilities needed for production environments. This Rust-based tool bridges that gap by providing:

- **Safe Concurrent Operations and Resource Management**: Thread-safe reindexing with built-in conflict detection and configurable threading for optimal performance vs. resource usage
- **Replication Safety**: Advanced checks for replication slots and sync connections
- **Production Safety**: Built-in safeguards against maintenance operations (VACUUM etc..) and previous active reindexer sessions.
- **Granular Control**: Target specific schemas or tables to create a different indexing strategies increasing flexibility.
- **Comprehensive Logging**: Track all reindex operations with detailed before/after metrics

### Key Features

#### 🎯 **Granular Control for Maintenance Categorization**
- **Schema-level operations**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Table-level precision**: Target specific tables for focused maintenance cycles
- **Categorized maintenance**: Organize maintenance processes by business logic (e.g., user tables, order tables, reporting tables)

#### 🔧 **Smart Index Selection**
- **B-tree focus**: Optimized for the most common index type in PostgreSQL
- **Constraint awareness**: Automatically skips primary keys and unique constraints (which can't use CONCURRENTLY)
- **Size filtering**: Exclude oversized indexes that could impact system performance
- **Safe operations**: Only processes indexes that can be safely reindexed concurrently

#### ⚡ **Performance Optimization Through Threading**
- **Configurable concurrency**: Control how fast you want to complete tasks (1-16+ threads)
- **Resource management**: Balance speed vs. system resource consumption
- **Adaptive processing**: Scale up for maintenance windows, scale down for production hours
- **Progress tracking**: Real-time monitoring of concurrent operations

#### 🛡️ **Built for Safety and Reliability**
- **Non-blocking operations**: Uses `REINDEX INDEX CONCURRENTLY` to minimize downtime
- **Conflict detection**: Automatically skips operations when vacuums or other processes are active
- **Replication safety**: Built-in checks for inactive replication slots and sync replication connections
- **Production controls**: Configurable safety overrides for maintenance windows and emergency operations
- **Index validation**: Automatic integrity checks after each reindex operation
- **Dry run mode**: Preview operations before execution
- **Resource protection**: Configurable maintenance work memory and parallel worker limits to protect production systems

## Installation

### Prerequisites

- Rust 1.70+ (for building from source)
- PostgreSQL 12+ (for concurrent reindex support)
- Access to PostgreSQL database

### Build from Source

```bash
# Clone the repository
git clone <repository-url>
cd pg-reindexer

# Build the project
cargo build --release

# The binary will be available at target/release/pg-reindexer
```

### Download Binary

```bash
# Download the latest release binary
wget https://github.com/your-org/pg-reindexer/releases/latest/download/pg-reindexer-x86_64-unknown-linux-gnu

# Make it executable
chmod +x pg-reindexer-x86_64-unknown-linux-gnu

# Move to a directory in your PATH (optional)
sudo mv pg-reindexer-x86_64-unknown-linux-gnu /usr/local/bin/pg-reindexer
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
  -n, --threads <THREADS>                               Number of concurrent threads for reindexing (default: 2) [default: 2]
  -v, --verbose                                         Verbose output
  -i, --skip-inactive-replication-slots                 Skip inactive replication slots check
  -r, --skip-sync-replication-connection                Skip sync replication connection check
  -m, --max-size-gb <MAX_SIZE_GB>                      Maximum index size in GB. Indexes larger than this will be excluded from reindexing [default: 1024]
  -w, --maintenance-work-mem-gb <MAINTENANCE_WORK_MEM_GB>  Maximum maintenance work mem in GB [default: 1]
  -x, --max-parallel-maintenance-workers <MAX_PARALLEL_MAINTENANCE_WORKERS>  Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety [default: 2]
  -l, --log-file <LOG_FILE>                             Log file path (default: reindexer.log in current directory) [default: reindexer.log]
  -h, --help                                            Print help
  -V, --version                                         Print version
```

## Usage

### Environment Variables

Configure the tool using environment variables for seamless integration:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=myapp
export PG_USER=postgres
export PG_PASSWORD=mypassword
```

### Basic Operations

#### Schema-Wide Reindexing
```bash
./pg-reindexer --schema public --verbose
```

#### Table-Specific Reindexing
```bash
./pg-reindexer --schema public --table orders --verbose
```

#### Preview Operations (Dry Run)
```bash
./pg-reindexer --schema public --dry-run
```

#### High-Performance Reindexing
```bash
./pg-reindexer --schema public --threads 8 --verbose
```


## Production System Protection

The tool includes advanced resource management features to protect production systems while optimizing performance:

### Memory Management (`maintenance_work_mem`)

The `maintenance_work_mem` parameter is critical for index operation performance:

- **Performance Impact**: Larger values enable faster index operations by providing more memory for sorting and building operations
- **Memory Usage**: Each parallel worker uses this amount of memory, so total usage = `maintenance_work_mem × max_parallel_maintenance_workers`
- **Production Safety**: Default of 1 GB is conservative to prevent memory exhaustion
- **Optimization Strategy**: 
  - Small indexes (< 1GB): 1-2 GB is sufficient
  - Large indexes (> 10GB): 4-8 GB can significantly improve performance
  - Monitor system memory usage during operations

### Parallel Worker Management (`max_parallel_maintenance_workers`)

The `max_parallel_maintenance_workers` parameter controls parallel index operations:

- **Safety Validation**: Automatically checks against system `max_parallel_workers` setting
- **Resource Protection**: Enforces limit of `max_parallel_workers/2` to prevent resource exhaustion
- **Performance Scaling**: Enables parallel index operations for improved throughput
- **Production Guidelines**:
  - **Production Hours**: 1-2 workers (minimal impact)
  - **Maintenance Windows**: 4-8 workers (high performance)
  - **Emergency Operations**: 1 worker (maximum safety)


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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on the project repository.