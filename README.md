# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. This tool provides safe, efficient, and controlled reindexing operations with intelligent resource management and comprehensive logging.

## Why This Tool?


### 🎯 **Granular Control for Maintenance Categorization**
- **Schema-level operations**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Table-level precision**: Target specific tables for focused maintenance cycles
- **Categorized maintenance**: Organize maintenance processes by business logic (e.g., user tables, order tables, reporting tables)
- **Selective reindexing**: Choose exactly which indexes to maintain based on your maintenance strategy

See [Maintenance Categorization Examples](#maintenance-categorization-examples) for practical usage patterns.

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

See [Thread Management & Resource Control](#thread-management--resource-control) for more details.

### 🛡️ **Built for Safety and Reliability**
- **Rust-powered**: Memory-safe, thread-safe, and crash-resistant
- **Memory Safety**: Zero-cost abstractions with guaranteed memory safety
- **Thread Safety**: Built-in concurrency primitives prevent race conditions
- **Crash Resistance**: Rust's ownership system prevents common programming errors
- **High Performance**: Compiled to native code for maximum efficiency
- **Non-blocking operations**: Uses `REINDEX INDEX CONCURRENTLY` to minimize downtime
- **Conflict detection**: Automatically skips operations when vacuums or other processes are active
- **Dry run mode**: Preview operations before execution
- **Error handling**: Comprehensive error reporting and logging
- **Conflict resolution**: Intelligent handling of concurrent operations

See [Safety Features](#safety-features) for more details.

## Purpose

PostgreSQL's native `REINDEX` command is powerful but lacks the orchestration capabilities needed for production environments. This Rust-based tool bridges that gap by providing:

- **Safe Concurrent Operations**: Thread-safe reindexing with built-in conflict detection
- **Granular Control**: Target specific schemas, tables, or individual indexes
- **Resource Management**: Configurable threading for optimal performance vs. resource usage
- **Production Safety**: Built-in safeguards against conflicts with other maintenance operations
- **Comprehensive Logging**: Track all reindex operations with detailed before/after metrics



## Installation

### Prerequisites

- Rust 1.70+ 
- PostgreSQL 12+ (for concurrent reindex support)
- Access to PostgreSQL database

### Optional Pre-requirements

The tool automatically creates the required database schema and tables for logging reindex operations. However, you can optionally create them manually before execution for better control:

```sql
-- Create the reindexer schema (optional)
CREATE SCHEMA IF NOT EXISTS reindexer;

-- Create the reindex_logbook table (optional)
CREATE TABLE IF NOT EXISTS reindexer.reindex_logbook (
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

**Note**: If you don't create the schema manually, the tool will automatically create it during execution.

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

This will show the exact SQL commands that would be executed:
```
INFO: Dry Run Mode - No indexes will be reindexed
INFO: The following reindex commands would be executed:

============================================================
[1/3] REINDEX INDEX CONCURRENTLY "public"."users_email_idx"
[2/3] REINDEX INDEX CONCURRENTLY "public"."users_created_at_idx"
[3/3] REINDEX INDEX CONCURRENTLY "public"."orders_status_idx"
============================================================

INFO: Total indexes to reindex: 3
HINT: To actually reindex, run without --dry-run flag
```

#### High-Performance Reindexing
```bash
./pg-reindexer -s public -n 8 -v
```

#### Custom Size Limits
```bash
# Reindex indexes up to 2TB in size
./pg-reindexer -s public -m 2048 -v

# Only reindex small indexes (up to 100GB)
./pg-reindexer -s public -m 100 -v
```



## Database Schema

The tool automatically creates a `reindexer` schema with a `reindex_logbook` table to track reindexing operations. You can also create these manually before execution (see Optional Pre-requirements section above):

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

- `success`: Index was successfully reindexed
- `skipped`: Index was skipped due to active vacuum or other pgreindexer processes



## Concurrent Reindexing

The tool uses PostgreSQL's `REINDEX INDEX CONCURRENTLY` command by default:

- **Requirements**: PostgreSQL 12+
- **Benefits**: Non-blocking reindexing that doesn't lock the table
- **Limitations**: Cannot be used on primary keys or unique constraints (automatically filtered)

## Thread Management & Resource Control

Control the concurrency of reindex operations to match your environment and maintenance strategy:

- **Default**: 2 concurrent threads (balanced for most environments)
- **Configurable**: Use `-n` flag to adjust from 1 to 16+ threads
- **Adaptive Usage**: Scale up during maintenance windows, down during peak hours
- **Resource Balance**: Control how fast you want to complete tasks vs. system resource consumption
- **Safety**: Automatic conflict detection prevents resource contention
- **Monitoring**: Real-time progress reporting for each thread

### Maintenance Categorization Examples

```bash
# User-related tables (fast, frequent maintenance)
./pg-reindexer -s public -t users -n 4 -v

# Order processing tables (medium priority)
./pg-reindexer -s public -t orders -n 2 -v

# Reporting tables (low priority, can be slower)
./pg-reindexer -s public -t reporting -n 1 -v

# Comprehensive schema maintenance (maintenance window)
./pg-reindexer -s public -n 8 -v
```

## Safety Features

Built-in safeguards for production environments:

- **Dry Run Mode**: Preview operations without making changes
- **Active Process Detection**: Skips reindexing when vacuum or other pgreindexer processes are active
- **Size Limits**: Automatically excludes very large indexes
- **Error Handling**: Comprehensive error reporting and logging
- **Conflict Resolution**: Intelligent handling of concurrent operations

## Performance Considerations

Optimize your reindexing strategy:

- **Thread Count**: Adjust `-n` based on your system resources (default: 2)
- **Concurrent Mode**: Always uses concurrent reindexing to minimize downtime
- **Verbose Mode**: Use `-v` for detailed progress reporting
- **Table Targeting**: Use `-t` to reindex specific tables for faster execution
- **Batch Scheduling**: Plan reindex jobs during low-traffic periods



## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure the PostgreSQL user has necessary permissions
2. **Connection Issues**: Check host, port, and authentication settings
3. **Schema Not Found**: Verify the schema name exists
4. **Table Not Found**: Ensure the table exists in the specified schema

### Error Messages

- `Failed to connect to PostgreSQL`: Check connection parameters
- `No indexes found`: Verify schema/table exists and contains indexes
- `Active vacuum detected`: Wait for vacuum to complete or use different timing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

The MIT License is a permissive license that allows:
- ✅ Commercial use
- ✅ Modification
- ✅ Distribution
- ✅ Private use
- ✅ No liability for the authors

This means you can use, modify, and distribute this software freely, including for commercial purposes.

## Support

For issues and questions, please open an issue on the project repository.