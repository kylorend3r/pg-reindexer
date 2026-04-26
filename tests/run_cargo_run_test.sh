#!/bin/bash

# PostgreSQL Reindexer Test Commands
# This script contains various test commands for different scenarios

echo "=== PostgreSQL Reindexer Test Commands ==="
echo ""

# Set default connection parameters (modify as needed)
export PG_DATABASE=benchmark
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=superuser
export SCHEMA=public
export PG_PASSWORD="test123"
export PGPASSFILE=src/.pgpass


# SSL connection parameters (uncomment and modify as needed)
# export PG_HOST=your-postgres-server.com
# export PG_USER=myuser
# export PG_PASSWORD=mypass

echo "=== Basic Operations ==="
echo ""

echo "# 1. Basic schema reindexing"
cargo run -- --schema $SCHEMA
echo ""

echo "# 2. Basic schema reindexing with verbose output"
cargo run -- --schema $SCHEMA
echo ""

echo "# 3. Specific table reindexing"
cargo run -- --schema $SCHEMA --table users
echo ""

echo "# 4. Single schema reindexing"
cargo run -- --schema public
echo ""

echo "=== Thread Count Variations ==="
echo ""

echo "# 5. Single thread (safe for production)"
cargo run -- --schema $SCHEMA --threads 1
echo ""

echo "# 6. Default threads (2)"
cargo run -- --schema $SCHEMA --threads 2
echo ""

echo "# 7. High concurrency (4 threads)"
cargo run -- --schema $SCHEMA --threads 4
echo ""

echo "# 8. Maximum threads (8 threads - safe limit)"
cargo run -- --schema $SCHEMA --threads 8
echo ""

echo "=== Memory and Worker Variations ==="
echo ""

echo "# 10. Conservative memory settings"
cargo run -- --schema $SCHEMA --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1
echo ""

echo "# 11. High performance settings"
cargo run -- --schema $SCHEMA --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2
echo ""

echo "# 12. Use PostgreSQL defaults for maintenance workers"
cargo run -- --schema $SCHEMA --max-parallel-maintenance-workers 0
echo ""

echo "# 13. High IO concurrency"
cargo run -- --schema $SCHEMA --maintenance-io-concurrency 128
echo ""

echo "# 14. Maximum IO concurrency"
cargo run -- --schema $SCHEMA --maintenance-io-concurrency 256
echo ""

echo "=== Size Filtering ==="
echo ""

echo "# 15. Only small indexes (max 1GB)"
cargo run -- --schema $SCHEMA --max-size-gb 1
echo ""

echo "# 16. Only medium indexes (max 10GB)"
cargo run -- --schema $SCHEMA --max-size-gb 10
echo ""

echo "# 17. Size range filtering (1GB to 50GB)"
cargo run -- --schema $SCHEMA --min-size-gb 1 --max-size-gb 50
echo ""

echo "=== Size-based Ordering ==="
echo ""

echo "# 17a. Order indexes by size (smallest first)"
cargo run -- --schema $SCHEMA --order-by-size asc
echo ""

echo "# 17b. Order indexes by size (largest first)"
cargo run -- --schema $SCHEMA --order-by-size desc
echo ""

echo "# 17c. Order indexes ascending (case-insensitive)"
cargo run -- --schema $SCHEMA --order-by-size ASC
echo ""

echo "# 17d. Order indexes descending (case-insensitive)"
cargo run -- --schema $SCHEMA --order-by-size DESC
echo ""

echo "# 17e. Combine ordering with size filtering (smallest indexes in range first)"
cargo run -- --schema $SCHEMA --min-size-gb 1 --max-size-gb 50 --order-by-size asc
echo ""

echo "# 17f. Combine ordering with size filtering (largest indexes in range first)"
cargo run -- --schema $SCHEMA --min-size-gb 1 --max-size-gb 50 --order-by-size desc
echo ""

echo "# 17g. Combine ordering with index type filtering (btree, smallest first)"
cargo run -- --schema $SCHEMA --index-type btree --order-by-size asc
echo ""

echo "# 17h. Combine ordering with index type filtering (constraints, largest first)"
cargo run -- --schema $SCHEMA --index-type constraint --order-by-size desc
echo ""

echo "# 17i. Order indexes across multiple schemas (smallest first)"
cargo run -- --schema $SCHEMA --order-by-size asc
echo ""

echo "# 17j. Order indexes with table filter (smallest first)"
cargo run -- --schema $SCHEMA --table users --order-by-size asc
echo ""

echo "# 17k. Order indexes with bloat filtering (largest bloated indexes first)"
cargo run -- --schema $SCHEMA --reindex-only-bloated 15 --order-by-size desc
echo ""

echo "# 17l. Order indexes with high performance settings (smallest first)"
cargo run -- --schema $SCHEMA --order-by-size asc --threads 4 --maintenance-work-mem-gb 2
echo ""

echo "=== Bloat-based Reindexing ==="
echo ""

echo "# 18. Only reindex indexes with bloat ratio >= 15%"
cargo run -- --schema $SCHEMA --reindex-only-bloated 15
echo ""

echo "# 19. Only reindex severely bloated indexes (>= 30%)"
cargo run -- --schema $SCHEMA --reindex-only-bloated 30
echo ""

echo "# 20. Only reindex critically bloated indexes (>= 50%)"
cargo run -- --schema $SCHEMA --reindex-only-bloated 50
echo ""

echo "=== Replication Safety Variations ==="
echo ""

echo "# 22. Skip inactive replication slots check"
cargo run -- --schema $SCHEMA --skip-inactive-replication-slots
echo ""

echo "# 23. Skip sync replication connection check"
cargo run -- --schema $SCHEMA --skip-sync-replication-connection
echo ""

echo "# 24. Skip both replication checks"
cargo run -- --schema $SCHEMA --skip-inactive-replication-slots --skip-sync-replication-connection
echo ""

echo "=== Connection Variations ==="
echo ""

echo "# 25. Using environment variables (default)"
cargo run -- --schema $SCHEMA
echo ""

echo "# 26. Explicit connection parameters"
cargo run -- --host $PG_HOST --port $PG_PORT --database $PG_DATABASE --username $PG_USER --schema $SCHEMA
echo ""

echo "=== Logging Variations ==="
echo ""

echo "# 28. Custom log file"
cargo run -- --schema $SCHEMA --log-file test_reindex.log
echo ""

echo "# 29. Silence mode (logs only to file)"
cargo run -- --schema $SCHEMA --silence-mode --log-file silent_reindex.log
echo ""

echo "=== Index Type Filtering ==="
echo ""

echo "# 30. Reindex only B-tree indexes (default)"
cargo run -- --schema $SCHEMA --index-type btree
echo ""

echo "# 31. Reindex only constraints (primary keys and unique)"
cargo run -- --schema $SCHEMA --index-type constraint
echo ""

echo "# 32. Reindex all index types"
cargo run -- --schema $SCHEMA --index-type all
echo ""

echo "=== Production Scenarios ==="
echo ""

echo "# 36. Production-safe (minimal impact)"
cargo run -- --schema $SCHEMA --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1
echo ""

echo "# 37. Maintenance window (high performance)"
cargo run -- --schema $SCHEMA --threads 4 --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2
echo ""

echo "# 38. Emergency operation (maximum safety)"
cargo run -- --schema $SCHEMA --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --skip-active-vacuums
echo ""

echo "=== Advanced Features ==="
echo ""

echo "# 40. Clean orphaned indexes before reindexing"
cargo run -- --schema $SCHEMA --clean-orphaned-indexes
echo ""

echo "# 41. Concurrent reindexing (online, no downtime)"
cargo run -- --schema $SCHEMA --concurrently
echo ""

echo "# 42. Exclude specific indexes"
cargo run -- --schema $SCHEMA --exclude-indexes "idx_users_email_unique,idx_products_sku_unique"
echo ""

echo "# 43. Resume from previous session"
cargo run -- --schema $SCHEMA --resume
echo ""

echo "# 44. Lock timeout setting"
cargo run -- --schema $SCHEMA --lock-timeout-seconds 30
echo ""

echo "=== Post-Reindex ANALYZE Scenarios ==="
echo ""

echo "# 44a. Dry-run with --analyze-tables (ANALYZE phase should not run in dry-run)"
cargo run -- --schema $SCHEMA --dry-run --analyze-tables users
echo ""

echo "# 44b. Dry-run with schema-qualified and plain table names"
cargo run -- --schema $SCHEMA --dry-run --analyze-tables public.users,orders
echo ""

echo "# 44c. Live run with post-reindex ANALYZE targets"
cargo run -- --schema $SCHEMA --analyze-tables public.users,orders
echo ""

echo "=== Combined Scenarios ==="
echo ""

echo "# 45. High-performance with safety checks"
cargo run -- --schema $SCHEMA --threads 4 --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2 --maintenance-io-concurrency 128
echo ""

echo "# 46. Conservative with all safety checks"
cargo run -- --schema $SCHEMA --threads 2 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1
echo ""

echo "# 47. Specific table with high performance"
cargo run -- --schema $SCHEMA --table users --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1
echo ""

echo "# 48. Partitioned table reindexing"
cargo run -- --schema public --table transaction_log --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1
echo ""

echo "# 49. Multiple schemas with bloat filtering"
cargo run -- --schema public,ecommerce --reindex-only-bloated 20 --threads 2
echo ""

echo "# 50. Complex scenario: constraints only, high performance"
cargo run -- --schema $SCHEMA --index-type constraint --threads 4 --maintenance-work-mem-gb 2 --concurrently
echo ""

echo "=== Schema Discovery ==="
echo ""

echo "# 51. Discover all schemas and reindex indexes in all of them"
cargo run -- --discover-all-schemas
echo ""

echo "# 52. Discover all schemas with dry-run (see what would be reindexed)"
cargo run -- --discover-all-schemas --dry-run
echo ""

echo "# 53. Discover all schemas with specific index type"
cargo run -- --discover-all-schemas --index-type btree
echo ""

echo "# 54. Discover all schemas with constraints only"
cargo run -- --discover-all-schemas --index-type constraint
echo ""

echo "# 55. Discover all schemas with high performance settings"
cargo run -- --discover-all-schemas --threads 4 --maintenance-work-mem-gb 2
echo ""

echo "# 56. Discover all schemas with size filtering"
cargo run -- --discover-all-schemas --min-size-gb 1 --max-size-gb 50
echo ""

echo "# 56a. Discover all schemas with size ordering (smallest first)"
cargo run -- --discover-all-schemas --order-by-size asc
echo ""

echo "# 56b. Discover all schemas with size ordering (largest first)"
cargo run -- --discover-all-schemas --order-by-size desc
echo ""

echo "# 56c. Discover all schemas with size filtering and ordering"
cargo run -- --discover-all-schemas --min-size-gb 1 --max-size-gb 50 --order-by-size desc
echo ""

echo "# 57. Discover all schemas with bloat filtering"
cargo run -- --discover-all-schemas --reindex-only-bloated 15
echo ""

echo "# 58. Discover all schemas with specific table filter"
cargo run -- --discover-all-schemas --table users
echo ""

echo "# 59. Discover all schemas with silence mode"
cargo run -- --discover-all-schemas --silence-mode --log-file discover_all.log
echo ""

echo "# 60. Discover all schemas and resume from previous session"
cargo run -- --discover-all-schemas --resume
echo ""

echo "=== Schema-Specific Tests ==="
echo ""

echo "# 61. Single schema: public"
cargo run -- --schema public
echo ""

echo "# 62. Single schema: ecommerce"
cargo run -- --schema ecommerce
echo ""

echo "# 63. Single schema: analytics"
cargo run -- --schema analytics
echo ""

echo "# 64. Single schema: inventory"
cargo run -- --schema inventory
echo ""

echo "# 65. Single schema: reporting"
cargo run -- --schema reporting
echo ""

echo "# 66. Single schema: audit"
cargo run -- --schema audit
echo ""

echo "# 67. Two schemas: public and ecommerce"
cargo run -- --schema public,ecommerce
echo ""

echo "# 68. All schemas with specific table"
cargo run -- --schema $SCHEMA --table transaction_log
echo ""

echo "=== Help and Version ==="
echo ""

echo "# 69. Show help"
cargo run -- --help
echo ""

echo "# 70. Show version"
cargo run -- --version
echo ""

echo "=== Complete Production-Ready Examples ==="
echo ""

echo "# 71. Complete production example"
cargo run -- \
  --host $PG_HOST \
  --port $PG_PORT \
  --database $PG_DATABASE \
  --username $PG_USER \
  --schema $SCHEMA \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --max-parallel-maintenance-workers 2 \
  --maintenance-io-concurrency 128 \
  --log-file production_reindex.log \
echo ""

echo "# 72. Complete example with all features"
cargo run -- \
  --schema $SCHEMA \
  --table users \
  --threads 2 \
  --maintenance-work-mem-gb 1 \
  --max-parallel-maintenance-workers 1 \
echo ""

echo "# 73. Bloat-based reindexing with high performance"
cargo run -- \
  --schema $SCHEMA \
  --reindex-only-bloated 15 \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --max-parallel-maintenance-workers 2 \
echo ""

echo "# 74. Conservative bloat-based reindexing"
cargo run -- \
  --schema $SCHEMA \
  --reindex-only-bloated 30 \
  --threads 2 \
  --maintenance-work-mem-gb 1 \
  --max-parallel-maintenance-workers 1
echo ""

echo "# 74a. Size-ordered reindexing with all features (smallest first)"
cargo run -- \
  --schema $SCHEMA \
  --order-by-size desc \
  --min-size-gb 0 \
  --max-size-gb 50 \
  --index-type constraint \
  --threads 4 \
  --maintenance-work-mem-gb 2 
echo ""

echo "# 74b. Size-ordered reindexing (largest first, high performance)"
cargo run -- \
  --schema $SCHEMA \
  --order-by-size desc \
  --threads 8 \
  --maintenance-work-mem-gb 4 \
  --max-parallel-maintenance-workers 4 \
  --maintenance-io-concurrency 256
echo ""

echo "# 75. Concurrent reindexing with all safety features"
cargo run -- \
  --schema $SCHEMA \
  --concurrently \
  --threads 2 \
  --maintenance-work-mem-gb 1 \
  --max-parallel-maintenance-workers 1 \
  --skip-inactive-replication-slots \
  --skip-sync-replication-connection \
  --skip-active-vacuums \
  --log-file concurrent_reindex.log \
echo ""

echo "# 76. Discover all schemas - complete production example"
cargo run -- \
  --discover-all-schemas \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --max-parallel-maintenance-workers 2 \
  --maintenance-io-concurrency 128 \
  --log-file discover_all_production.log \
echo ""

echo "# 77. Discover all schemas with bloat filtering and high performance"
cargo run -- \
  --discover-all-schemas \
  --reindex-only-bloated 15 \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --max-parallel-maintenance-workers 2
echo ""

echo "# 77a. Discover all schemas with size ordering (smallest first)"
cargo run -- \
  --discover-all-schemas \
  --order-by-size asc \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --log-file discover_all_ordered_asc.log
echo ""

echo "# 77b. Discover all schemas with size ordering and filtering (largest first)"
cargo run -- \
  --discover-all-schemas \
  --order-by-size desc \
  --min-size-gb 1 \
  --max-size-gb 50 \
  --threads 4 \
  --maintenance-work-mem-gb 2 \
  --log-file discover_all_ordered_desc.log
echo ""

echo "# 78. Discover all schemas - conservative production settings"
cargo run -- \
  --discover-all-schemas \
  --threads 2 \
  --maintenance-work-mem-gb 1 \
  --max-parallel-maintenance-workers 1 \
  --concurrently \
  --log-file discover_all_conservative.log \
echo ""

echo "=== ACTUAL REINDEXING COMMANDS (Remove # to execute) ==="
echo ""
echo "WARNING: The following commands will actually reindex your database!"
echo "Only run these after testing with --dry-run first!"
echo ""

echo "# 79. Production-safe reindexing (ACTUAL - no dry-run)"
echo "# cargo run -- --schema $SCHEMA --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --concurrently --log-file production_reindex.log"
echo ""

echo "# 80. Single table reindexing (ACTUAL)"
echo "# cargo run -- --schema $SCHEMA --table users --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --concurrently --log-file production_reindex.log"
echo ""

echo "# 81. High-performance reindexing (ACTUAL)"
echo "# cargo run -- --schema $SCHEMA --threads 4 --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2 --concurrently --log-file production_reindex.log"
echo ""

echo "# 82. Discover all schemas - production-safe (ACTUAL)"
echo "# cargo run -- --discover-all-schemas --threads 1 --maintenance-work-mem-gb 1 --max-parallel-maintenance-workers 1 --skip-inactive-replication-slots --skip-sync-replication-connection --concurrently --log-file discover_all_production.log"
echo ""

echo "# 83. Discover all schemas - high performance (ACTUAL)"
echo "# cargo run -- --discover-all-schemas --threads 4 --maintenance-work-mem-gb 2 --max-parallel-maintenance-workers 2 --concurrently --log-file discover_all_high_perf.log"
echo ""

echo "=== Notes ==="
echo ""
echo "Remember to:"
echo "1. All commands will actually reindex your database!"
echo "2. Ensure PostgreSQL is running and accessible"
echo "3. Have appropriate permissions for the target schemas"
echo "4. Monitor system resources during high-performance operations"
echo "5. Environment variables are set at the top of this script"
echo "6. Current schema list: $SCHEMA"
echo "7. Use --dry-run flag if you want to test without executing"
echo ""
