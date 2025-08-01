# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. This tool provides safe, efficient, and controlled reindexing operations with intelligent resource management and comprehensive logging.

## Purpose

PostgreSQL's native `REINDEX` command is powerful but lacks the orchestration capabilities needed for production environments. This Rust-based tool bridges that gap by providing:

- **Safe Concurrent Operations**: Thread-safe reindexing with built-in conflict detection
- **Granular Control**: Target specific schemas, tables, or individual indexes
- **Resource Management**: Configurable threading for optimal performance vs. resource usage
- **Production Safety**: Built-in safeguards against conflicts with other maintenance operations
- **Replication Safety**: Advanced checks for replication slots and sync connections
- **Comprehensive Logging**: Track all reindex operations with detailed before/after metrics

## Great Features

### 🎯 **Granular Control for Maintenance Categorization**
- **Schema-level operations**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Table-level precision**: Target specific tables for focused maintenance cycles
- **Categorized maintenance**: Organize maintenance processes by business logic (e.g., user tables, order tables, reporting tables)
- **Selective reindexing**: Choose exactly which indexes to maintain based on your maintenance strategy

### 🔧 **Smart Index Selection**
- **B-tree focus**: Optimized for the most common index type in PostgreSQL
- **Constraint awareness**: Automatically skips primary keys and unique constraints (which can't use CONCURRENTLY)
- **Size filtering**: Exclude oversized indexes that could impact system performance
- **Safe operations**: Only processes indexes that can be safely reindexed concurrently

### ⚡ **Performance Optimization Through Threading**
- **Configurable concurrency**: Control how fast you want to complete tasks (1-16+ threads)
- **Resource management**: Balance speed vs. system resource consumption
- **Adaptive processing**: Scale up for maintenance windows, scale down for production hours
- **Progress tracking**: Real-time monitoring of concurrent operations

### 🛡️ **Built for Safety and Reliability**
- **Non-blocking operations**: Uses `REINDEX INDEX CONCURRENTLY` to minimize downtime
- **Conflict detection**: Automatically skips operations when vacuums or other processes are active
- **Replication safety**: Built-in checks for inactive replication slots and sync replication connections
- **Production controls**: Configurable safety overrides for maintenance windows and emergency operations
- **Index validation**: Automatic integrity checks after each reindex operation
- **Dry run mode**: Preview operations before execution


### Index Integrity Validation

The tool automatically validates each index after reindexing to ensure the operation was successful:

- **Automatic Validation**: Every reindexed index undergoes an integrity check
- **Safety Delay**: 5-second delay ensures PostgreSQL metadata is fully updated before validation
- **Comprehensive Checks**: Validates index state, readiness, and liveliness
- **Failure Tracking**: Failed validations are logged with detailed status information
- **Status Recording**: Validation results are saved to the reindex logbook

### Production Safety Controls

The tool includes advanced safety controls for production environments with replication:

#### Replication Safety Checks

- **Inactive Replication Slots Detection** (`-i`, `--skip-inactive-replication-slots`):
  - Automatically detects inactive replication slots that could cause replication lag
  - When enabled, skips reindexing operations if inactive slots are detected
  - Prevents WAL accumulation that could impact downstream replicas
  - Default: `false` (safety check enabled)

- **Sync Replication Connection Detection** (`-r`, `--skip-sync-replication-connection`):
  - Monitors for synchronous replication connections
  - Skips reindexing when sync replication is active to prevent blocking
  - Ensures high-availability setups remain unaffected
  - Default: `false` (safety check enabled)

## Installation

### Prerequisites

- Rust 1.70+ 
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

## Usage

### Basic Reindex Operations

```bash
# Reindex all indexes in a schema
./pg-reindexer -s public

# Reindex indexes for a specific table
./pg-reindexer -s public -t users

# Dry run to preview operations
./pg-reindexer -s public --dry-run

# High-performance reindexing with 8 threads
./pg-reindexer -s public -n 8 -v
```

### Command Line Arguments

| Argument | Short | Long | Description | Default |
|----------|-------|------|-------------|---------|
| Schema | `-s` | `--schema` | Schema name to reindex | Required |
| Table | `-t` | `--table` | Table name to reindex (optional) | None |
| Host | `-H` | `--host` | PostgreSQL host | localhost |
| Port | `-p` | `--port` | PostgreSQL port | 5432 |
| Database | `-d` | `--database` | Database name | postgres |
| Username | `-U` | `--username` | Username | postgres |
| Password | `-P` | `--password` | Password | None |
| Dry Run | `-f` | `--dry-run` | Show what would be reindexed | false |
| Threads | `-n` | `--threads` | Number of concurrent threads | 2 |
| Verbose | `-v` | `--verbose` | Verbose output | false |
| Max Size | `-m` | `--max-size-gb` | Maximum index size in GB | 1024 |
| Skip Inactive Replication Slots | `-i` | `--skip-inactive-replication-slots` | Skip reindexing when inactive replication slots detected | false |
| Skip Sync Replication Connection | `-r` | `--skip-sync-replication-connection` | Skip reindexing when sync replication connections detected | false |
| Log File | `-l` | `--log-file` | Log file path (all output will be logged to this file) | reindexer.log |

### Environment Variables

Configure the tool using environment variables for seamless integration:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=myapp
export PG_USER=postgres
export PG_PASSWORD=mypassword
```

### Examples

#### Schema-Wide Reindexing
```bash
./pg-reindexer -s public -v
```

#### Table-Specific Reindexing
```bash
./pg-reindexer -s public -t orders -v
```

#### Preview Operations (Dry Run)
```bash
./pg-reindexer -s public --dry-run
```

#### High-Performance Reindexing
```bash
./pg-reindexer -s public -n 8 -v
```

#### Production Safety Examples
```bash
# Safe production reindexing with all safety checks enabled
./pg-reindexer -s public -v

# Reindex during maintenance window (replication temporarily disabled)
./pg-reindexer -s public -i -r -v

# Emergency reindexing (use with extreme caution)
./pg-reindexer -s public -i -r -n 1 -v

# Reindex with default logging (reindexer.log)
./pg-reindexer -s public -v

# Reindex with custom log file
./pg-reindexer -s public -v -l custom.log

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