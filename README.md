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
  - [Size-based Ordering](#-size-based-ordering)
  - [Production Scenarios](#-production-scenarios)
- [Configuration File](#configuration-file)
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
pg-reindexer --database mydb --schema public --dry-run

# Reindex all indexes in a schema
pg-reindexer --database mydb --schema public

# Reindex indexes from multiple schemas (comma-separated, max 512)
pg-reindexer --database mydb --schema public,app_schema,analytics_schema

# Reindex indexes across multiple databases
pg-reindexer --database db1,db2,db3 --schema public

# Discover all schemas in the database and reindex indexes in all of them
pg-reindexer --database mydb --discover-all-schemas

# Reindex indexes for a specific table
pg-reindexer --database mydb --schema public --table users

# Reindex only regular b-tree indexes (default)
pg-reindexer --database mydb --schema public --index-type btree

# Reindex only primary keys and unique constraints
pg-reindexer --database mydb --schema public --index-type constraint

# Exclude specific indexes from reindexing
pg-reindexer --database mydb --schema public --exclude-indexes "idx_users_email,idx_orders_created_at"

# Resume from a previous interrupted reindexing session
pg-reindexer --database mydb --schema public --resume

# Silence mode - only log to file, minimal terminal output
pg-reindexer --database mydb --schema public --silence-mode

# High-performance reindexing
pg-reindexer --database mydb --schema public --threads 8

# Order indexes by size (smallest first)
pg-reindexer --database mydb --schema public --order-by-size asc

# Order indexes by size (largest first)
pg-reindexer --database mydb --schema public --order-by-size desc

# Ask for confirmation before proceeding with reindexing
pg-reindexer --database mydb --schema public --ask-confirmation

# SSL connection to remote PostgreSQL server
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl

# SSL connection with invalid certificate acceptance (testing only)
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl --ssl-self-signed

# Using TOML configuration file
pg-reindexer --config config.toml

# Using config file with CLI overrides (CLI takes precedence)
pg-reindexer --config config.toml --threads 8 --schema public
```

## Usage Examples

### 🚀 **Basic Operations**

```bash
# Dry run (always test first!)
pg-reindexer --database mydb --schema public --dry-run

# Reindex indexes from multiple schemas
pg-reindexer --database mydb --schema public,app_schema,analytics_schema

# Reindex indexes across multiple databases
pg-reindexer --database db1,db2,db3 --schema public

# Reindex multiple databases with multiple schemas
pg-reindexer --database db1,db2 --schema public,app_schema

# Discover all schemas and reindex indexes in all discovered schemas
pg-reindexer --database mydb --discover-all-schemas

# Discover all schemas across multiple databases
pg-reindexer --database db1,db2 --discover-all-schemas

# Discover all schemas with specific index type filtering
pg-reindexer --database mydb --discover-all-schemas --index-type btree

# Reindex only b-tree indexes
pg-reindexer --database mydb --schema public --index-type btree

# Reindex only constraints
pg-reindexer --database mydb --schema public --index-type constraint

# Reindex constraints from multiple schemas
pg-reindexer --database mydb --schema public,app_schema --index-type constraint

# Reindex constraints across multiple databases
pg-reindexer --database db1,db2 --schema public --index-type constraint

# Specific table reindexing
pg-reindexer --database mydb --schema public --table users

# Specific table reindexing across multiple databases
pg-reindexer --database db1,db2 --schema public --table users
```

### 🔧 **Thread Count Variations**

```bash
# Production safe (1 thread)
pg-reindexer --database mydb --schema public --threads 1 

# High performance (8 threads)
pg-reindexer --database mydb --schema public --threads 8 

# Maximum threads (32)
pg-reindexer --database mydb --schema public --threads 32 
```

### 💾 **Memory and Performance Settings**

```bash
# Conservative settings
pg-reindexer --database mydb --schema public --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1

# High performance settings
pg-reindexer --database mydb --schema public --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4

# Use PostgreSQL defaults
pg-reindexer --database mydb --schema public --max-parallel-maintenance-workers 0

# High IO concurrency
pg-reindexer --database mydb --schema public --maintenance-io-concurrency 256

# Set lock timeout to 30 seconds
pg-reindexer --database mydb --schema public --lock-timeout-seconds 30

# Disable lock timeout (default)
pg-reindexer --database mydb --schema public --lock-timeout-seconds 0
```

### 📊 **Size Filtering**

```bash
# Only medium indexes (max 10GB)
pg-reindexer --database mydb --schema public --max-size-gb 10

# Large indexes only (min 100GB)
pg-reindexer --database mydb --schema public --min-size-gb 100

# Size range filtering (1GB to 50GB)
pg-reindexer --database mydb --schema public --min-size-gb 1 --max-size-gb 50

# Size filtering across multiple schemas
pg-reindexer --database mydb --schema public,app_schema --min-size-gb 1 --max-size-gb 50
```

**Note**: The tool will log the index size limits being applied for clarity:
```
Index size limits: minimum 1 GB, maximum 50 GB
```

### 🎯 **Index Type Filtering**

```bash
# Reindex only regular b-tree indexes (default)
pg-reindexer --database mydb --schema public --index-type btree

# Reindex only primary keys and unique constraints
pg-reindexer --database mydb --schema public --index-type constraint

# Reindex b-tree indexes from multiple schemas
pg-reindexer --database mydb --schema public,app_schema --index-type btree

# Combine with size filtering
pg-reindexer --database mydb --schema public --index-type constraint --min-size-gb 1 --max-size-gb 10

# Combine with table filtering
pg-reindexer --database mydb --schema public --table users --index-type btree
```

**Index Type Options**:
- `btree` (default): Regular b-tree indexes (excludes primary keys and unique constraints)
- `constraint`: Primary keys and unique constraints only

### 🎯 **Bloat-based Reindexing**

```bash
# Only reindex indexes with bloat ratio >= 15%
pg-reindexer --database mydb --schema public --reindex-only-bloated 15

# Bloat-based reindexing across multiple schemas
pg-reindexer --database mydb --schema public,app_schema,analytics_schema --reindex-only-bloated 15
```

### 🔍 **Automatic Schema Discovery**

```bash
# Discover all user schemas and reindex indexes in all of them
pg-reindexer --database mydb --discover-all-schemas

# Discover all schemas with dry-run to see what would be reindexed
pg-reindexer --database mydb --discover-all-schemas --dry-run

# Discover all schemas with specific filters
pg-reindexer --database mydb --discover-all-schemas --index-type btree --min-size-gb 1

# Discover all schemas and resume from previous session
pg-reindexer --database mydb --discover-all-schemas --resume
```

**Note**: The `--discover-all-schemas` option automatically discovers all user schemas in the database, excluding system schemas (pg_catalog, information_schema, pg_toast, etc.) and the tool-managed `reindexer` schema. This is useful for comprehensive database maintenance when you want to reindex indexes across all user schemas without manually specifying each one.

### 🗄️ **Multiple Database Support**

The tool supports processing multiple databases in a single execution. This is useful for maintaining multiple databases on the same PostgreSQL instance or cluster.

```bash
# Reindex indexes across multiple databases
pg-reindexer --database db1,db2,db3 --schema public

# Multiple databases with multiple schemas
pg-reindexer --database db1,db2 --schema public,app_schema

# Multiple databases with schema discovery
pg-reindexer --database db1,db2,db3 --discover-all-schemas

# Multiple databases with specific table
pg-reindexer --database db1,db2 --schema public --table users

# Multiple databases with all features
pg-reindexer --database db1,db2,db3 --schema public --threads 4 --maintenance-work-mem-gb 2

# Using environment variable for multiple databases
export PG_DATABASE=db1,db2,db3
pg-reindexer --schema public

# Command line argument overrides environment variable
export PG_DATABASE=db1,db2
pg-reindexer --database db3,db4 --schema public  # Uses db3,db4
```

**Key Points**:
- **Sequential Processing**: Databases are processed one at a time to avoid overwhelming the PostgreSQL instance
- **Independent State**: Each database maintains its own `reindexer` schema and state tables
- **Error Handling**: If one database fails, the tool logs the error and continues with the next database
- **Progress Logging**: The tool logs which database is being processed (e.g., "Processing database 1/3: db1")
- **Same Configuration**: All databases use the same reindexing configuration (threads, memory, etc.)
- **Whitespace Handling**: Database names are automatically trimmed (e.g., "db1 , db2" becomes "db1,db2")

**Use Cases**:
- **Multi-tenant Applications**: Reindex indexes across all tenant databases
- **Environment Maintenance**: Process development, staging, and production databases
- **Database Clusters**: Maintain multiple databases on the same PostgreSQL instance
- **Scheduled Maintenance**: Batch process multiple databases during maintenance windows

### 🔄 **Resume Interrupted Sessions**

```bash
# Resume reindexing from a previous interrupted session
pg-reindexer --database mydb --schema public --resume

# Resume with same performance settings as original run
pg-reindexer --database mydb --schema public --resume --threads 8 --maintenance-work-mem-gb 4

# Resume and clean orphaned indexes
pg-reindexer --database mydb --schema public --resume --clean-orphaned-indexes

# Resume for multiple schemas
pg-reindexer --database mydb --schema public,app_schema,analytics_schema --resume
```

**Note**: The `--resume` option will continue processing indexes that were pending, failed, or in progress from a previous session. Completed indexes are preserved and will not be reindexed again.

### 🔇 **Silence Mode**

```bash
# Run in silence mode - only startup and completion messages to terminal
pg-reindexer --database mydb --schema public --silence-mode

# Silence mode with other options
pg-reindexer --database mydb --schema public --silence-mode --threads 8 --maintenance-work-mem-gb 4

# Resume with silence mode
pg-reindexer --database mydb --schema public --resume --silence-mode
```

**Note**: In silence mode, all detailed logs are written to the log file but not printed to the terminal. Only the startup message and final completion summary are displayed. This is useful for automated scripts or when running in background processes.

### 🧹 **Orphaned Index Cleanup**

```bash
# Clean up orphaned _ccnew indexes before reindexing
pg-reindexer --database mydb --schema public --clean-orphaned-indexes

# Combine with other operations
pg-reindexer --database mydb --schema public --clean-orphaned-indexes --threads 4 --maintenance-work-mem-gb 2
```

### 🔒 **SSL/TLS Connections**

```bash
# Secure connection to remote PostgreSQL server
pg-reindexer --database mydb --schema public --host your-postgres-server.com --port 5432 --ssl

# SSL connection with custom credentials
pg-reindexer --database mydb --schema public --host your-postgres-server.com --username myuser --password mypass --ssl

# SSL connection for testing (allows self-signed certificates)
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl --ssl-self-signed

# SSL connection with custom CA certificate
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl --ssl-ca-cert /path/to/ca-cert.pem

# SSL connection with client certificate authentication
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl \
  --ssl-client-cert /path/to/client-cert.pem \
  --ssl-client-key /path/to/client-key.pem

# SSL connection with both CA and client certificates
pg-reindexer --database mydb --schema public --host your-postgres-server.com --ssl \
  --ssl-ca-cert /path/to/ca-cert.pem \
  --ssl-client-cert /path/to/client-cert.pem \
  --ssl-client-key /path/to/client-key.pem

# Production SSL connection with environment variables
export PG_HOST=your-postgres-server.com
export PG_USER=myuser
export PG_PASSWORD=mypass
pg-reindexer --database mydb --schema public --ssl

# Local connection without SSL (default)
pg-reindexer --database mydb --schema public --host localhost
```

### 🛡️ **Production Scenarios**

```bash
# Production hours (minimal impact)
pg-reindexer --database mydb --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1

# Maintenance window (high performance)
pg-reindexer --database mydb --schema public --threads 8 --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4 --maintenance-io-concurrency 256

# Reindex multiple schemas during maintenance window
pg-reindexer --database mydb --schema public,app_schema,analytics_schema --threads 8 --maintenance-work-mem-gb 4

# Reindex multiple databases during maintenance window
pg-reindexer --database db1,db2,db3 --schema public --threads 8 --maintenance-work-mem-gb 4

# Emergency operation (maximum safety)
pg-reindexer --database mydb --schema public --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --skip-active-vacuums

# Production with lock timeout protection (automatic retries on lock timeout)
pg-reindexer --database mydb --schema public --threads 2 --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2 --lock-timeout-seconds 60

# Production with SSL connection
pg-reindexer --database mydb --schema public --host prod-db.company.com --ssl --threads 2 --maintenance-work-mem-gb 2

# Multiple schemas with SSL connection
pg-reindexer --database mydb --schema public,app_schema --host prod-db.company.com --ssl --threads 4 --maintenance-work-mem-gb 2

# Multiple databases with SSL connection
pg-reindexer --database db1,db2 --host prod-db.company.com --ssl --threads 4 --maintenance-work-mem-gb 2

# Discover all schemas and reindex during maintenance window
pg-reindexer --database mydb --discover-all-schemas --threads 8 --maintenance-work-mem-gb 4

# Discover all schemas across multiple databases during maintenance window
pg-reindexer --database db1,db2 --discover-all-schemas --threads 8 --maintenance-work-mem-gb 4
```


## Configuration File

Instead of providing all settings via command-line arguments, you can use a TOML configuration file. This is especially useful for:
- **Reproducible configurations**: Share settings across environments
- **Complex setups**: Avoid long command-line invocations
- **Team consistency**: Standardize reindexing parameters
- **CI/CD pipelines**: Version-controlled configuration

### Basic Usage

```bash
# Use a configuration file
pg-reindexer --config config.toml

# CLI arguments override config file values
pg-reindexer --config config.toml --threads 8 --schema public
```

### Configuration File Format

All fields in the configuration file are **optional**. Only specify what you need. CLI arguments always take precedence over config file values.

Here's an example configuration file (`config.example.toml`):

```toml
# PostgreSQL Reindexer Configuration File
# CLI arguments take precedence over values in this file

# Connection settings
host = "localhost"
port = 5432
database = "mydb"
username = "postgres"
password = "mypassword"  # Consider using environment variables or .pgpass for security

# Schema settings
schema = "public"  # Can be a single schema or comma-separated list
# discover-all-schemas = false  # Alternative to schema
# table = "users"  # Optional: reindex indexes for a specific table only

# Operation settings
dry-run = false
threads = 4

# Skip checks
skip-inactive-replication-slots = false
skip-sync-replication-connection = false
skip-active-vacuums = false

# Size filtering
max-size-gb = 1024
min-size-gb = 0

# Index type: "btree", "constraint", or "all"
index-type = "btree"

# Maintenance settings
maintenance-work-mem-gb = 2
max-parallel-maintenance-workers = 4
maintenance-io-concurrency = 10
lock-timeout-seconds = 0

# Logging
log-file = "reindexer.log"

# Optional settings
# reindex-only-bloated = 15  # Only reindex indexes with bloat >= 15%
concurrently = true
clean-orphaned-indexes = false

# SSL settings
ssl = false
ssl-self-signed = false
# ssl-ca-cert = "/path/to/ca-cert.pem"
# ssl-client-cert = "/path/to/client-cert.pem"
# ssl-client-key = "/path/to/client-key.pem"

# Other settings
# exclude-indexes = "idx_users_email,idx_orders_created_at"
resume = false
silence-mode = false
# order-by-size = "asc"  # "asc" or "desc"
ask-confirmation = false
```

### Configuration File Examples

```bash
# Minimal config file (only required fields)
pg-reindexer --config minimal.toml
# minimal.toml:
# schema = "public"
# database = "mydb"

# Production config for maintenance window
pg-reindexer --config production.toml
# production.toml:
# schema = "public,app_schema,analytics_schema"
# threads = 8
# maintenance-work-mem-gb = 4
# max-parallel-maintenance-workers = 4
# maintenance-io-concurrency = 256
# concurrently = true

# Development config with dry-run
pg-reindexer --config dev.toml
# dev.toml:
# schema = "public"
# dry-run = true
# threads = 2
# log-file = "dev-reindexer.log"

# Override config file values via CLI
pg-reindexer --config production.toml --threads 16  # Uses 16 instead of config value
```

### Configuration File Advantages

- **All Settings Supported**: Every CLI argument can be specified in the config file
- **Partial Configuration**: Only specify the fields you need to override
- **Comments**: TOML supports comments for documentation
- **Type Safety**: TOML enforces correct data types
- **Version Control**: Store configurations in git for reproducible operations

### Configuration Precedence

The tool resolves configuration values in the following order (highest to lowest priority):

1. **CLI arguments** (highest priority)
2. **Configuration file values**
3. **Environment variables** (for connection parameters)
4. **Default values** (lowest priority)

Example:
```bash
# config.toml has: threads = 4
# CLI specifies: --threads 8
# Result: threads = 8 (CLI wins)

# config.toml has: threads = 4
# CLI doesn't specify threads
# Result: threads = 4 (config file wins)

# config.toml doesn't specify threads
# CLI doesn't specify threads
# Result: threads = 2 (default value)
```

### Configuration File Best Practices

1. **Security**: Never commit passwords to version control. Use environment variables or `.pgpass` files instead:
   ```toml
   # In config.toml (don't commit passwords)
   host = "localhost"
   database = "mydb"
   username = "postgres"
   # password = ""  # Leave empty, use PG_PASSWORD env var or .pgpass
   ```

2. **Environment-Specific Configs**: Create separate config files for different environments:
   - `config.dev.toml` - Development settings
   - `config.staging.toml` - Staging settings
   - `config.production.toml` - Production settings

3. **Comments**: Use comments to document why specific settings were chosen:
   ```toml
   # Conservative settings for production hours
   threads = 1
   maintenance-work-mem-gb = 1
   
   # Aggressive settings for maintenance window
   # threads = 8
   # maintenance-work-mem-gb = 4
   ```

4. **Team Sharing**: Store commonly used configurations in shared repositories for consistency across team members.

See `config.example.toml` in the repository for a complete example configuration file with all available options.

## Environment Variables

```bash
# Basic connection parameters
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=postgres
export PG_USER=postgres
export PG_PASSWORD=mypassword

# Then run without connection parameters (uses PG_DATABASE from environment)
pg-reindexer --database mydb --schema public

# Multiple schemas with environment variables
pg-reindexer --database mydb --schema public,app_schema,analytics_schema 

# Multiple databases via environment variable (comma-separated)
export PG_DATABASE=db1,db2,db3
pg-reindexer --schema public

# Multiple databases via command line (overrides environment variable)
export PG_DATABASE=db1,db2
pg-reindexer --database db3,db4 --schema public  # Uses db3,db4 (not db1,db2)

# SSL connection with environment variables
export PG_HOST=your-postgres-server.com
export PG_USER=myuser
export PG_PASSWORD=mypass
pg-reindexer --database mydb --schema public --ssl

# Multiple databases with SSL connection
export PG_DATABASE=db1,db2
pg-reindexer --schema public --ssl

# .pgpass file configuration
export PGPASSFILE=.pgpass
```

### SSL Environment Variables

When using SSL connections, you can combine environment variables with SSL flags:

```bash
# Set connection parameters via environment
export PG_HOST=your-postgres-server.com
export PG_USER=myuser
export PG_PASSWORD=mypass

# Use SSL connection
pg-reindexer --schema public --ssl

# For testing with self-signed certificates
pg-reindexer --schema public --ssl --ssl-self-signed
```

## Command Line Interface

```bash
PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table

Usage: pg-reindexer [OPTIONS] [--schema <SCHEMA> | --discover-all-schemas]

Note: Either --schema or --discover-all-schemas must be provided. The --schema parameter accepts a single schema name or a comma-separated list of schema names (maximum 512 schemas). The --discover-all-schemas option automatically discovers all user schemas in the database (excluding system schemas like pg_catalog, information_schema, etc.).

Options:
  -C, --config <FILE>                                   Path to TOML configuration file. Configuration file values are overridden by CLI arguments.
  -H, --host <HOST>                                    PostgreSQL host (can also be set via PG_HOST environment variable or config file)
  -p, --port <PORT>                                     PostgreSQL port (can also be set via PG_PORT environment variable or config file)
  -d, --database <DATABASE>                             Database name(s) to connect to. Can be a single database or comma-separated list of databases (can also be set via PG_DATABASE environment variable or config file)
  -U, --username <USERNAME>                             Username (can also be set via PG_USER environment variable or config file)
  -P, --password <PASSWORD>                             Password (can also be set via PG_PASSWORD environment variable or config file)
  -s, --schema <SCHEMA>                                 Schema name(s) to reindex. Can be a single schema or comma-separated list (max 512 schemas). Not required if --discover-all-schemas is used.
      --discover-all-schemas                             Discover all user schemas in the database and collect indexes for all discovered schemas. System schemas (pg_catalog, information_schema, etc.) are excluded.
  -t, --table <TABLE>                                   Table name to reindex (optional - if not provided, reindexes all indexes in schema)
  -f, --dry-run                                         Dry run - show what would be reindexed without actually doing it
  -n, --threads <THREADS>                               Number of concurrent threads for reindexing (default: 2, max: 32) [default: 2]
  -i, --skip-inactive-replication-slots                 Skip inactive replication slots check
  -r, --skip-sync-replication-connection                Skip sync replication connection check
      --skip-active-vacuums                              Skip active vacuum check
  -m, --max-size-gb <MAX_SIZE_GB>                      Maximum index size in GB. Indexes larger than this will be excluded from reindexing [default: 1024]
      --min-size-gb <MIN_SIZE_GB>                      Minimum index size in GB. Indexes smaller than this will be excluded from reindexing [default: 0]
      --order-by-size <ORDER>                          Order indexes by size: 'asc' for smallest first, 'desc' for largest first. If not specified, indexes are ordered ascending by default.
      --ask-confirmation                                Ask for final confirmation with a summary of indexes (count) before proceeding with reindexing
      --index-type <INDEX_TYPE>                        Index type to reindex: 'btree' for regular b-tree indexes, 'constraint' for primary keys and unique constraints [default: btree]
  -w, --maintenance-work-mem-gb <MAINTENANCE_WORK_MEM_GB>  Maximum maintenance work mem in GB (max: 32 GB) [default: 1]
  -x, --max-parallel-maintenance-workers <MAX_PARALLEL_MAINTENANCE_WORKERS>  Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety. Use 0 for PostgreSQL default (typically 2) [default: 2]
  -c, --maintenance-io-concurrency <MAINTENANCE_IO_CONCURRENCY>  Maintenance IO concurrency. Controls the number of concurrent I/O operations during maintenance operations [default: 10]
      --lock-timeout-seconds <LOCK_TIMEOUT_SECONDS>     Lock timeout in seconds. Set to 0 for no timeout (default). This controls how long to wait for locks before timing out. [default: 0]
  -l, --log-file <LOG_FILE>                             Log file path (default: reindexer.log in current directory) [default: reindexer.log]
      --reindex-only-bloated <PERCENTAGE>               Reindex only indexes with bloat ratio above this percentage (0-100). If not specified, all indexes will be reindexed
      --concurrently                                     Use REINDEX INDEX CONCURRENTLY for online reindexing. Set to false to use offline reindexing (REINDEX INDEX) [default: true]
      --clean-orphaned-indexes                            Drop orphaned _ccnew indexes (temporary concurrent reindex indexes) before starting the reindexing process. These indexes are created by PostgreSQL during REINDEX INDEX CONCURRENTLY operations and may be left behind if the operation was interrupted.
      --ssl                                              Enable SSL connection to PostgreSQL. When enabled, the connection will use SSL/TLS encryption.
      --ssl-self-signed                                   Allow self-signed or invalid SSL certificates. Useful for development environments or when connecting to servers with self-signed certificates.
      --ssl-ca-cert <SSL_CA_CERT>                        Path to CA certificate file (.pem) for SSL connection. If not provided, uses system default certificate store.
      --ssl-client-cert <SSL_CLIENT_CERT>                Path to client certificate file (.pem) for SSL connection. Requires --ssl-client-key.
      --ssl-client-key <SSL_CLIENT_KEY>                  Path to client private key file (.pem) for SSL connection. Requires --ssl-client-cert.
      --exclude-indexes <EXCLUDE_INDEXES>                Comma-separated list of index names to exclude from reindexing. These indexes will be skipped even if they match other selection criteria.
      --resume                                           Resume reindexing from previous state. If enabled, the tool will load pending/failed indexes from the reindex_state table and continue processing.
      --silence-mode                                     Silence mode: suppresses all terminal output except startup and completion messages. All logs are still written to the log file.
  -h, --help                                            Print help
  -V, --version                                         Print version
```

## Key Features

### 🎯 **Granular Maintenance Control**
- **Schema-level Reindexing**: Reindex all indexes in a specific schema for comprehensive maintenance
- **Automatic Schema Discovery**: Automatically discover all user schemas in the database and reindex indexes across all of them with `--discover-all-schemas`
- **Multiple Schema Support**: Reindex indexes across multiple schemas in a single operation (comma-separated list, max 512 schemas)
- **Multiple Database Support**: Reindex indexes across multiple databases in a single operation (comma-separated list). Each database is processed independently with its own connection and state management
- **Table-level Reindexing**: Target specific tables for focused maintenance cycles
- **Index Type Filtering**: Choose between regular b-tree indexes or primary keys/unique constraints
- **Index Exclusion**: Exclude specific indexes from reindexing using comma-separated lists
- **Size-based Ordering**: Control the order in which indexes are processed by their size (ascending or descending)
- **Flexible Scheduling**: Create different maintenance strategies for different schemas/tables/databases
- **B-tree Focus**: Optimized for the most common index type in PostgreSQL
- **Constraint Awareness**: Target primary keys and unique constraints separately from regular indexes

### ⚡ **Concurrent Operations with Safety**
- **Flexible Reindexing Modes**: Choose between online (`REINDEX INDEX CONCURRENTLY`) or offline (`REINDEX INDEX`) reindexing
- **Non-blocking Reindexing**: Uses `REINDEX INDEX CONCURRENTLY` by default to minimize downtime
- **Smart Threading**: Multiple threads for different tables, but protects same table from concurrent operations
- **Configurable Concurrency**: 1-32 threads with automatic validation against PostgreSQL limits
- **Proven Performance**: Significantly reduces total reindexing duration for databases with many indexes

### 🛡️ **Built-in Safety Checks**
- **Active Vacuum Detection**: Automatically skips reindexing when manual vacuum operations are active (excludes autovacuum)
- **Replication Safety**: Checks for inactive replication slots to prevent WAL size issues
- **Sync Replica Protection**: Stops operations when sync replication is detected to prevent primary unresponsiveness
- **Per-Thread Validation**: Each thread performs fresh safety checks when it starts (no stale data)
- **Orphaned Index Cleanup**: Automatically detects and optionally drops orphaned `_ccnew` indexes left behind by interrupted concurrent reindex operations

### 🔄 **Automatic Retry Logic for Transient Errors**
- **Intelligent Error Detection**: Automatically detects and retries transient errors that are safe to retry:
  - **Lock Timeout Errors**: Retries when a reindex operation fails due to lock timeout (when `--lock-timeout-seconds` is set)
  - **Deadlock Errors**: Retries when PostgreSQL detects a deadlock and rolls back the transaction
  - **Connection Errors**: Retries when connection issues occur (connection closed, lost, reset, etc.)
- **Automatic Retry Strategy**: Up to 3 additional retry attempts (4 total attempts) for transient errors
- **Smart Connection Management**: 
  - For connection errors: Creates a fresh database connection before retrying
  - For lock timeout/deadlock: Reuses the existing connection (still valid after these errors)
- **Configurable Delay**: 2-second delay between retry attempts to allow system recovery
- **Comprehensive Logging**: Each retry attempt is logged with error type, attempt number, and retry delay
- **Non-Retryable Errors**: Errors that are not transient (e.g., permission errors, invalid SQL) fail immediately without retries
- **Production Ready**: Handles transient database issues gracefully without manual intervention

### 📊 **Intelligent Bloat Detection**
- **Bloat Ratio Calculation**: Uses PostgreSQL's internal statistics to calculate index bloat percentage
- **Threshold-Based Filtering**: Only reindex indexes that exceed the specified bloat threshold
- **Efficient Maintenance**: Focus resources on indexes that actually need reindexing
- **Configurable Sensitivity**: Set bloat threshold from 0-100% to match your maintenance strategy

### 📊 **Size-based Index Ordering**
- **Flexible Ordering**: Order indexes by size in ascending (smallest first) or descending (largest first) order
- **Efficient Processing**: Process smaller indexes first to free up resources, or target large indexes for priority maintenance
- **SQL-level Sorting**: Sorting is performed at the database level for optimal performance
- **Size Tracking**: Index size information is stored with each index for efficient processing and reporting
- **Default Behavior**: Indexes are ordered ascending by size by default if no ordering is specified

### 🔄 **Resume Functionality**
- **Interrupted Session Recovery**: Resume reindexing from where it left off after an interruption or crash
- **State Preservation**: Automatically tracks and preserves completed work across sessions
- **Automatic Discovery**: Discovers any new indexes that weren't in the previous session
- **Failure Recovery**: Retries failed indexes from previous sessions when resuming
- **Per-Database State**: Each database maintains its own state table, allowing independent resume operations

### 🛑 **Graceful Cancellation**
- **Signal Handling**: Supports SIGINT (Ctrl+C), SIGTERM (`kill <pid>`), and SIGHUP (terminal close) for graceful shutdown
- **State Cleanup**: Automatically resets in-progress indexes to pending state on cancellation, enabling seamless resume
- **Timeout-Based Shutdown**: Allows current operations to complete within 30 seconds before forcing termination
- **No Data Loss**: Cancelled operations can be resumed with `--resume` flag without losing progress

### 🗄️ **Multiple Database Support**
- **Batch Processing**: Process multiple databases in a single command execution
- **Independent Processing**: Each database is processed sequentially with its own connection and state management
- **Error Isolation**: If one database fails, the tool continues processing the remaining databases
- **Flexible Configuration**: Use comma-separated database names via command line or environment variable
- **Progress Tracking**: Logs show which database is being processed when multiple databases are specified
- **State Management**: Each database maintains its own `reindexer` schema and state tables

### 🔇 **Silence Mode**
- **Minimal Terminal Output**: Suppresses all terminal output except startup and completion messages
- **Full File Logging**: All detailed logs are still written to the log file for later review
- **Script-Friendly**: Ideal for automated scripts and background processes
- **Progress Tracking**: Completion summary always displays for quick status checks

### ⚡ **Significant Performance Gains with Multi-Threading**

One of the tool's major strengths is its ability to dramatically reduce total reindexing duration through intelligent parallel processing. By utilizing multiple threads to process indexes on different tables concurrently, you can achieve substantial time savings.

**Real-World Performance Example:**
- **34 indexes with 1 thread**: 6.37 seconds
- **34 indexes with 3 threads**: 5.96 seconds
- **Time saved**: ~6.4% reduction in total duration

The performance improvement becomes even more significant with larger numbers of indexes. The tool's smart table-level locking ensures that indexes from the same table are never processed concurrently (preventing conflicts), while indexes from different tables can be reindexed in parallel for maximum throughput.

**Best Practices:**
- For databases with many indexes: Use 4-8 threads for optimal performance
- For production environments: Start with 2-4 threads to balance performance and system load
- For large maintenance windows: Use 8-16 threads with appropriate resource settings
- Monitor system resources and adjust thread count based on your hardware capabilities

### 🔧 **Performance Optimization**
- **Configurable GUCs**: Set PostgreSQL parameters for optimal performance:
  - `maintenance_work_mem`: Control memory allocation for index operations (max: 32 GB)
  - `maintenance_io_concurrency`: Manage concurrent I/O operations (max: 512)
  - `max_parallel_maintenance_workers`: Control parallel worker count
  - `lock_timeout`: Control how long to wait for locks before timing out (0 = no timeout). When set, the tool automatically retries on lock timeout errors
- **Resource Management**: Balance performance vs. system resource consumption
- **Smart Defaults**: Uses PostgreSQL defaults when parameters are set to 0
- **Parallel Processing**: Process indexes on different tables simultaneously for maximum throughput
- **Automatic Retry on Lock Timeout**: When `--lock-timeout-seconds` is configured, the tool automatically retries failed reindex operations up to 3 times, making it safe to use lock timeouts in production environments

### 🔒 **Secure Connections**
- **SSL/TLS Support**: Encrypted connections to PostgreSQL servers using industry-standard TLS
- **Certificate Validation**: Proper SSL certificate verification by default for secure connections
- **Custom CA Certificates**: Support for custom Certificate Authority certificates for self-signed or private PKI
- **Client Certificate Authentication**: Mutual TLS authentication using client certificates and private keys
- **Testing Mode**: Optional invalid certificate acceptance for development and testing environments
- **Flexible Configuration**: Combine SSL with environment variables and command-line parameters
- **Production Ready**: Secure by default with proper certificate validation

### 📝 **Configuration File Support**
- **TOML Configuration**: Use TOML files for persistent, version-controlled configurations
- **Flexible Configuration**: All CLI arguments can be specified in config files
- **Partial Configuration**: Only specify the fields you need to override
- **CLI Override**: Command-line arguments always take precedence over config file values
- **Team Consistency**: Share standardized configurations across team members
- **CI/CD Friendly**: Version-controlled configurations for automated pipelines

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
    size_change BIGINT,
    reindex_duration REAL
);
```

### Logbook Status Values

- `success`: Index was successfully reindexed and validated (may have been retried after transient errors)
- `validation_failed`: Index reindexing completed but validation failed
- `failed`: Index reindexing failed after all retry attempts were exhausted. This includes:
  - Non-retryable errors (e.g., permission errors, invalid SQL)
  - Transient errors that failed after 4 total attempts (1 initial + 3 retries)
- `skipped`: Index was skipped due to active vacuum, inactive replication slots, or sync replication
- `below_bloat_threshold`: Index was skipped because its bloat ratio was below the specified threshold
- `invalid_index`: Index was skipped because it was found to be invalid
- `excluded`: Index was excluded from reindexing via the `--exclude-indexes` parameter

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on the project repository.
