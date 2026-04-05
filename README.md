[![Pg-Reindexer CI](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/kylorend3r/pg-reindexer/actions/workflows/rust.yml)
[![Release](https://github.com/kylorend3r/pg-reindexer/actions/workflows/release.yml/badge.svg)](https://github.com/kylorend3r/pg-reindexer/actions/workflows/release.yml)
[![Security Audit](https://github.com/kylorend3r/pg-reindexer/actions/workflows/security.yml/badge.svg)](https://github.com/kylorend3r/pg-reindexer/actions/workflows/security.yml)

# PostgreSQL Reindexer

A high-performance, production-ready PostgreSQL index maintenance tool written in Rust. Safely reindex your PostgreSQL indexes with intelligent resource management and comprehensive logging.

![Reindexer](assets/reindex.gif)

## Why pg-reindexer?

- **No extensions, no dependencies** — works with any standard PostgreSQL installation (9.x+); no `pg_repack`, `pgstattuple`, or any other extension required
- **Zero-dependency binary** — single static binary; no runtime, no package manager, no install scripts
- **Zero-downtime reindexing** — uses `REINDEX INDEX CONCURRENTLY` by default so production traffic is unaffected
- **Safe by default** — pre-flight safety checks (active vacuums, inactive replication slots, sync replication) block operations that could cause issues
- **Resumable sessions** — interrupted runs are never wasted; `--resume` picks up exactly where it left off
- **Fine-grained control** — bloat filtering, size limits, per-index exclusions, configurable parallelism, and maintenance memory settings
- **Production-ready observability** — every operation logged to `reindexer.reindex_logbook` with before/after sizes and duration

## Who Is This For?

pg-reindexer is built for **DBAs, SREs, and backend engineers** who manage PostgreSQL in production and need a safe, automated way to keep indexes healthy — without downtime, without extensions, and without complex setup.

It's a particularly good fit if you are:

- **Running PostgreSQL on a managed cloud service** (AWS RDS, Aurora, Google Cloud SQL, Supabase, Neon, etc.) where `pg_repack` and other extension-based tools are unavailable or require elevated privileges you don't have. pg-reindexer uses only standard SQL and works out of the box on any managed Postgres.

- **Running PostgreSQL on-premises** and want a maintenance tool you can drop onto any server as a single binary — no runtime, no package manager, no install scripts. Copy it, run it, done. Full `.pgpass`, SSL/TLS, and custom CA certificate support covers the typical on-prem setup.

- **A small team without a dedicated DBA** who needs safe, automated index maintenance but doesn't have the expertise to write and maintain custom reindexing scripts. The pre-flight safety checks, retry logic, and replication awareness handle the hard parts for you.

- **An infrastructure or platform engineer** who wants to run index maintenance as a scheduled job or CI/CD pipeline step. The `--silence-mode`, `--dry-run`, JSON log output, and single-binary distribution make it straightforward to integrate into any automation stack.

- **Anyone hitting index bloat** on large tables where `REINDEX` without `CONCURRENTLY` would lock the table and take down your application. pg-reindexer defaults to `REINDEX INDEX CONCURRENTLY` so production traffic is never blocked.

## Table of Contents

- [Who Is This For?](#who-is-this-for)
- [Why pg-reindexer?](#why-pg-reindexer)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
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
wget https://github.com/kylorend3r/pg-reindexer/releases/latest/download/pg-reindexer-x86_64-unknown-linux-gnu
chmod +x pg-reindexer-x86_64-unknown-linux-gnu
```

## Usage Examples

```bash
# Dry run — always test first
pg-reindexer --database mydb --schema public --dry-run

# Reindex all indexes across multiple schemas with 4 threads
pg-reindexer --database mydb --schema public,app_schema --threads 4

# Maintenance window with high-performance settings
pg-reindexer --database mydb --schema public --threads 8 --maintenance-work-mem-gb 4 --max-parallel-maintenance-workers 4

# SSL connection to remote server with bloat-based filtering
pg-reindexer --database mydb --schema public --host prod-db.company.com --ssl --reindex-only-bloated 15

# Resume an interrupted session in silence mode
pg-reindexer --database mydb --schema public --resume --silence-mode
```

## Configuration File

Instead of providing all settings via CLI arguments, you can use a TOML configuration file:

```bash
pg-reindexer --config config.toml

# CLI arguments override config file values
pg-reindexer --config config.toml --threads 8
```

### Configuration File Format

```toml
# PostgreSQL Reindexer Configuration File
# CLI arguments take precedence over values in this file

# Connection settings
host = "localhost"
port = 5432
database = "mydb"
username = "postgres"
# password = ""  # Use environment variables or .pgpass for security

# Schema settings
schema = "public"
# discover-all-schemas = false
# table = "users"

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
# reindex-only-bloated = 15
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
# order-by-size = "asc"
ask-confirmation = false
```

### Configuration Precedence

1. **CLI arguments** (highest priority)
2. **Configuration file values**
3. **Environment variables**
4. **Default values** (lowest priority)

See `config.example.toml` in the repository for a complete example.

## Environment Variables

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=mydb
export PG_USER=postgres
export PG_PASSWORD=mypassword

# Multiple databases via environment variable
export PG_DATABASE=db1,db2,db3

# .pgpass file
export PGPASSFILE=.pgpass
```

## Command Line Interface

```
PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table

Usage: pg-reindexer [OPTIONS] [--schema <SCHEMA> | --discover-all-schemas]

Options:
  -C, --config <FILE>                                   Path to TOML configuration file
  -H, --host <HOST>                                     PostgreSQL host
  -p, --port <PORT>                                     PostgreSQL port
  -d, --database <DATABASE>                             Database name(s), comma-separated
  -U, --username <USERNAME>                             Username
  -P, --password <PASSWORD>                             Password
  -s, --schema <SCHEMA>                                 Schema name(s), comma-separated (max 512)
      --discover-all-schemas                            Discover all user schemas automatically
  -t, --table <TABLE>                                   Target a specific table
  -f, --dry-run                                         Show what would be reindexed without doing it
  -n, --threads <THREADS>                               Concurrent threads (default: 2, max: 32)
  -i, --skip-inactive-replication-slots                 Skip inactive replication slots check
  -r, --skip-sync-replication-connection                Skip sync replication connection check
      --skip-active-vacuums                             Skip active vacuum check
  -m, --max-size-gb <MAX_SIZE_GB>                       Maximum index size in GB [default: 1024]
      --min-size-gb <MIN_SIZE_GB>                       Minimum index size in GB [default: 0]
      --order-by-size <ORDER>                           Order by size: 'asc' or 'desc'
      --ask-confirmation                                Ask for confirmation before proceeding
      --index-type <INDEX_TYPE>                         'btree', 'constraint', or 'all' [default: btree]
  -w, --maintenance-work-mem-gb <GB>                    Maintenance work mem in GB (max: 32) [default: 1]
  -x, --max-parallel-maintenance-workers <N>            Parallel maintenance workers [default: 2]
  -c, --maintenance-io-concurrency <N>                  Maintenance IO concurrency [default: 10]
      --lock-timeout-seconds <N>                        Lock timeout in seconds, 0 = no timeout [default: 0]
  -l, --log-file <LOG_FILE>                             Log file path [default: reindexer.log]
      --log-format <FORMAT>                             Log format: 'text' or 'json' [default: text]
      --reindex-only-bloated <PERCENTAGE>               Only reindex indexes with bloat above this %
      --concurrently                                    Use REINDEX CONCURRENTLY (online) [default: true]
      --clean-orphaned-indexes                          Drop orphaned _ccnew indexes before starting
      --ssl                                             Enable SSL/TLS connection
      --ssl-self-signed                                 Allow self-signed certificates
      --ssl-ca-cert <FILE>                              Path to CA certificate (.pem)
      --ssl-client-cert <FILE>                          Path to client certificate (.pem)
      --ssl-client-key <FILE>                           Path to client private key (.pem)
      --exclude-indexes <INDEXES>                       Comma-separated index names to exclude
      --resume                                          Resume from previous state
      --silence-mode                                    Suppress terminal output except startup/completion
  -h, --help                                            Print help
  -V, --version                                         Print version
```

## Key Features

- **Multi-threading**: 1–32 configurable threads; same-table indexes are never processed concurrently
- **Non-blocking**: Uses `REINDEX INDEX CONCURRENTLY` by default to avoid downtime
- **Safety checks**: Detects active vacuums, inactive replication slots, and sync replication before proceeding
- **Retry logic**: Automatically retries transient errors (lock timeouts, deadlocks, connection issues) up to 3 times
- **Bloat detection**: Filter indexes by bloat ratio to target only indexes that need maintenance
- **Resume**: Tracks state in a `reindexer` schema; interrupted sessions can be resumed with `--resume`
- **Graceful shutdown**: SIGINT/SIGTERM resets in-progress indexes to pending for seamless resume
- **Multiple databases/schemas**: Comma-separated lists for batch operations across databases or schemas
- **SSL/TLS**: Full certificate support including custom CA and mutual TLS
- **Config file**: TOML configuration with CLI override support

## Database Schema

The tool automatically creates a `reindexer` schema with tables to track operations:

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

| Status | Description |
|--------|-------------|
| `success` | Successfully reindexed and validated |
| `validation_failed` | Reindexing completed but validation failed |
| `failed` | Failed after all retry attempts |
| `skipped` | Skipped due to active vacuum, replication slot, or sync replication |
| `below_bloat_threshold` | Bloat ratio below specified threshold |
| `invalid_index` | Index was found to be invalid |
| `excluded` | Excluded via `--exclude-indexes` |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on the project repository.
