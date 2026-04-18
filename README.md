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
- **Replica lag awareness** — adaptive throttling pauses index acquisition when replication lag exceeds a configured threshold, protecting standbys automatically
- **Pre-flight planning** — `plan` subcommand produces a ranked, read-only index worklist (JSON or CSV) so you can review what will be reindexed before touching the database
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
- [Plan Subcommand](#plan-subcommand)
- [Adaptive Replica Lag Throttling](#adaptive-replica-lag-throttling)
- [Pacing](#pacing)
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

# Preview ranked index worklist before reindexing (no DB writes)
pg-reindexer plan --database mydb --schema public --sort-by bloat,size --format json

# Throttle acquisition when replica lag exceeds 500 MB, stop after 30 minutes
pg-reindexer --database mydb --schema public --max-replica-lag-bytes 524288000 --max-replica-lag-wait-secs 1800

# Add 50ms pacing between acquisitions to reduce platform pressure
pg-reindexer --database mydb --schema public --pacing-ms 50
```

## Plan Subcommand

The `plan` subcommand connects to PostgreSQL, queries index metadata, and outputs a ranked worklist — **without writing anything to the database**. Use it to review which indexes will be reindexed and in what order before running the actual operation.

```bash
# JSON output ranked by bloat (default)
pg-reindexer plan --database mydb --schema public

# Multi-criteria ranking: bloat first, then size as tiebreaker
pg-reindexer plan --database mydb --schema public --sort-by bloat,size

# CSV output to file
pg-reindexer plan --database mydb --schema public --sort-by size --format csv --output /tmp/plan.csv

# Discover all schemas
pg-reindexer plan --database mydb --discover-all-schemas --sort-by age
```

### Sort Criteria

| Criterion | Description |
|-----------|-------------|
| `bloat` | Estimated bloat percentage (highest bloat ranked first) |
| `size` | Physical index size in bytes (largest first) |
| `scan-frequency` | Indexes scanned least frequently ranked first |
| `age` | Indexes not scanned for the longest time ranked first; never-scanned indexes rank highest |

Multiple criteria can be combined with a comma (`--sort-by bloat,size`). Each criterion is normalized to `[0, 1]` and the scores are summed to produce the final rank.

### Plan Output Shape (JSON)

```json
[
  {
    "rank": 1,
    "schema_name": "public",
    "table_name": "orders",
    "index_name": "orders_created_at_idx",
    "index_type": "btree",
    "size_bytes": 45678592,
    "size_pretty": "43 MB",
    "scan_count": 12,
    "last_scan": "2026-01-05T14:22:00Z",
    "estimated_bloat_percent": 38.5
  }
]
```

### Plan Options

```
  -H, --host <HOST>               PostgreSQL host
  -p, --port <PORT>               PostgreSQL port
      --database <DATABASE>       Database name
  -U, --username <USERNAME>       Username
  -P, --password <PASSWORD>       Password
  -s, --schema <SCHEMA>           Schema name(s), comma-separated
      --discover-all-schemas      Discover all user schemas automatically
      --sort-by <CRITERIA>        Comma-separated ranking criteria: bloat, size, scan-frequency, age [default: bloat]
      --format <FORMAT>           Output format: json or csv [default: json]
      --output <FILE>             Write output to file instead of stdout
      --ssl                       Enable SSL/TLS connection
      --ssl-self-signed           Allow self-signed certificates
      --ssl-ca-cert <FILE>        Path to CA certificate
      --ssl-client-cert <FILE>    Path to client certificate
      --ssl-client-key <FILE>     Path to client private key
      --config <FILE>             Path to TOML configuration file
```

## Adaptive Replica Lag Throttling

When `REINDEX INDEX CONCURRENTLY` runs, it generates substantial WAL traffic. On systems with replication standbys, this can cause replica lag to spike. With `--max-replica-lag-bytes`, workers pause acquiring new indexes whenever lag (measured via `pg_stat_replication`) exceeds the threshold — then automatically resume once lag drops back down.

```bash
# Pause acquisition when any standby lags more than 1 GB
pg-reindexer --database mydb --schema public \
  --max-replica-lag-bytes 1073741824

# Same, but stop workers entirely if lag persists for more than 10 minutes
pg-reindexer --database mydb --schema public \
  --max-replica-lag-bytes 1073741824 \
  --max-replica-lag-wait-secs 600
```

**Behavior:**
- Lag is checked before each index acquisition attempt
- When lag exceeds the threshold, the worker sleeps for 5 seconds and rechecks
- A warning is logged at first detection and every 60 seconds thereafter
- Once lag drops below the threshold, acquisition resumes automatically
- If lag persists beyond `--max-replica-lag-wait-secs` (default: 1800), workers stop gracefully and remaining indexes stay `pending` in the state table so `--resume` can pick them up in the next run

| Flag | Default | Description |
|------|---------|-------------|
| `--max-replica-lag-bytes` | disabled | Lag threshold in bytes; not set means no throttling |
| `--max-replica-lag-wait-secs` | `1800` | Hard limit (seconds) before workers stop waiting |

## Pacing

The `--pacing-ms` flag inserts a short sleep before each index acquisition attempt. This is useful when running maintenance alongside live traffic and you want to voluntarily reduce the rate at which the tool acquires locks, even under normal conditions.

```bash
# 50ms pause before each acquisition (reduces lock acquisition rate)
pg-reindexer --database mydb --schema public --pacing-ms 50

# Disable pacing entirely
pg-reindexer --database mydb --schema public --pacing-ms 0
```

The default is 10ms, which is negligible under normal conditions. Raise it (e.g. 100–500ms) during peak traffic windows when any additional database pressure should be minimized.

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

# Replica lag throttling
# max-replica-lag-bytes = 1073741824   # pause acquisition when any standby lags > 1 GB
# max-replica-lag-wait-secs = 1800     # stop workers if lag persists beyond this many seconds

# Pacing
# pacing-ms = 10   # sleep between acquisition attempts (0 to disable)

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
      --max-replica-lag-bytes <BYTES>                   Pause acquisition when replica lag exceeds this threshold
      --max-replica-lag-wait-secs <SECS>                Stop workers if lag persists beyond this limit [default: 1800]
      --pacing-ms <MS>                                  Sleep before each acquisition attempt [default: 10]
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
- **Plan subcommand**: Read-only ranked index worklist (JSON/CSV) with multi-criteria scoring — review before you run
- **Replica lag throttling**: Adaptive acquisition pause when standby lag exceeds a threshold; hard time limit prevents indefinite waiting
- **Pacing**: Configurable inter-acquisition sleep to reduce platform pressure during live traffic

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
