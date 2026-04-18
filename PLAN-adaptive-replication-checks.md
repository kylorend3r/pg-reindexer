# Plan: Adaptive Replication Checks (`--max-replica-lag-bytes`)

## Context

`REINDEX CONCURRENTLY` generates large volumes of WAL. On systems with replicas, this can cause replica lag to spike, creating operational risk (failover delays, standby OOM). The tool currently checks for sync replication connections and inactive slots at startup but has no runtime awareness of replica lag during the reindex run itself.

This feature adds reactive throttling: workers poll `pg_stat_replication` before acquiring each new index, and pause acquisition when any replica's lag exceeds the configured byte threshold. In-progress `REINDEX CONCURRENTLY` operations are never interrupted — only the pickup of the **next** index is held back, giving replicas time to catch up.

---

## Branch

```bash
git checkout main
git checkout -b feat/adaptive-replication-checks
```

---

## Files to Modify

| File | Change |
|---|---|
| `src/queries.rs` | Add `GET_MAX_REPLICA_LAG_BYTES` |
| `src/checks.rs` | Add `get_max_replica_lag_bytes()` |
| `src/types.rs` | Add `mark_all_pending_skipped()` to `IndexMemoryTable` |
| `src/orchestrator.rs` | Add `max_replica_lag_bytes`, `max_replica_lag_wait_secs` to `WorkerConfig`; call `mark_all_pending_skipped()` after workers finish |
| `src/index_operations.rs` | Add lag check + timed backoff with hard limit in `worker_with_memory_table` loop |
| `src/main.rs` | Add `--max-replica-lag-bytes` and `--max-replica-lag-wait` to `Args`, `Config`, `merge_config`, `WorkerConfig` construction |

---

## Step 1 — `src/queries.rs`: New Query

Append after `GET_SYNC_REPLICATION_CONNECTION_COUNT`:

```rust
/// Returns the maximum replication lag in bytes across all connected standbys.
/// Uses pg_wal_lsn_diff(sent_lsn, replay_lsn) which reflects unacknowledged WAL on each replica.
/// Returns 0 when there are no replicas or all replay_lsn values are NULL (not yet reported).
pub const GET_MAX_REPLICA_LAG_BYTES: &str = r#"
    SELECT COALESCE(
        MAX(pg_wal_lsn_diff(sent_lsn, replay_lsn)),
        0
    )::bigint
    FROM pg_stat_replication
    WHERE replay_lsn IS NOT NULL
"#;
```

Note: `sent_lsn - replay_lsn` is the right metric — it measures WAL sent but not yet replayed by the standby, which is the lag a REINDEX WAL burst would create.

---

## Step 2 — `src/checks.rs`: New Check Function

```rust
/// Returns the maximum replica lag in bytes across all standbys, or 0 if no replicas exist.
pub async fn get_max_replica_lag_bytes(client: &Client) -> Result<i64> {
    let rows = client
        .query(crate::queries::GET_MAX_REPLICA_LAG_BYTES, &[])
        .await
        .context("Failed to query replica lag from pg_stat_replication")?;
    Ok(rows.first().map(|r| r.get::<_, i64>(0)).unwrap_or(0))
}
```

---

## Step 3 — `src/types.rs`: New `mark_all_pending_skipped()` on `IndexMemoryTable`

Add to `IndexMemoryTable` impl:

```rust
/// Marks all remaining Pending entries as Skipped.
/// Called by the orchestrator after all workers exit to clean up indexes
/// that were never acquired due to replica lag timeout.
pub fn mark_all_pending_skipped(&mut self) {
    for entry in self.indexes.values_mut() {
        if entry.status == IndexStatus::Pending {
            entry.status = IndexStatus::Skipped;
        }
    }
}
```

This is safe to call only after all workers have returned (no active writers on the memory table).

## Step 3b — `src/orchestrator.rs`: Extend `WorkerConfig` + call `mark_all_pending_skipped`

### Add two fields to `WorkerConfig`:

```rust
pub max_replica_lag_bytes: Option<i64>,
pub max_replica_lag_wait_secs: Option<u64>,
```

Position: after `session_id`, as the last two fields.

### In `finalize_and_log_completion` (or end of `collect_worker_results`):

After all worker tasks have completed, call `mark_all_pending_skipped()` on the shared memory table before reading statistics. This converts any unprocessed Pending entries to Skipped so the final summary accurately reflects what happened:

```rust
// After workers finish, mark any unacquired indexes as Skipped
{
    let mut table = memory_table.lock().await;
    table.mark_all_pending_skipped();
}
```

Note: the database `reindex_state` table is intentionally **not** updated to Skipped — those rows stay Pending so `--resume` can pick them up in a subsequent run once lag has cleared.

---

## Step 4 — `src/index_operations.rs`: Throttle in Worker Loop

### New constants (alongside existing `DEFAULT_RETRY_DELAY_MS`):

```rust
const REPLICA_LAG_BACKOFF_MS: u64 = 5_000;          // sleep between lag re-checks
const REPLICA_LAG_WARN_INTERVAL_SECS: u64 = 60;     // log a warning every 60 s of continuous throttle
const DEFAULT_REPLICA_LAG_WAIT_SECS: u64 = 1_800;   // default hard limit: 30 minutes
```

### Logic in `worker_with_memory_table`

The current loop structure is:
```
while has_pending_indexes:
    [1] check cancellation
    [2] try_acquire_index_for_worker()
    [3] if acquired: process
    [4] if not acquired: sleep 100ms
```

Insert the lag check **between [1] and [2]**. `lag_throttle_start: Option<Instant>` declared before the loop tracks when continuous throttling began:

```rust
// Declared once before the main loop:
let mut lag_throttle_start: Option<std::time::Instant> = None;

// Inside the loop, between [1] and [2]:
if let Some(max_lag) = config.max_replica_lag_bytes {
    match checks::get_max_replica_lag_bytes(&client).await {
        Ok(lag) if lag > max_lag => {
            let started = lag_throttle_start.get_or_insert_with(std::time::Instant::now);
            let waited_secs = started.elapsed().as_secs();

            // Hard limit: stop worker and let orchestrator mark remaining as Skipped
            let hard_limit = config.max_replica_lag_wait_secs
                .unwrap_or(DEFAULT_REPLICA_LAG_WAIT_SECS);
            if waited_secs >= hard_limit {
                logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "[worker {}] replica lag has exceeded threshold for {}s (limit: {}s) — \
                         stopping worker; remaining indexes will be marked skipped",
                        worker_id, waited_secs, hard_limit
                    ),
                );
                break; // Exit loop; orchestrator handles marking pending as Skipped
            }

            // Log immediately on first pause, then every REPLICA_LAG_WARN_INTERVAL_SECS
            if waited_secs == 0 || waited_secs % REPLICA_LAG_WARN_INTERVAL_SECS == 0 {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "[worker {}] replica lag {} bytes exceeds threshold {} bytes — \
                         acquisition paused (waited {}s / {}s limit)",
                        worker_id, lag, max_lag, waited_secs, hard_limit
                    ),
                );
            }

            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(REPLICA_LAG_BACKOFF_MS)) => {}
                _ = cancel_rx.changed() => { break; }
            }
            continue; // Re-evaluate loop; do NOT acquire
        }
        Ok(_) => {
            lag_throttle_start = None; // Lag cleared — reset the timer
        }
        Err(e) => {
            lag_throttle_start = None;
            logger.log(
                logging::LogLevel::Warning,
                &format!(
                    "[worker {}] could not read replica lag: {} — proceeding without throttle",
                    worker_id, e
                ),
            );
        }
    }
}
```

The `Err` branch is intentionally non-fatal: if `pg_stat_replication` is unavailable (e.g., no superuser rights), the tool degrades gracefully and continues reindexing without throttling.

---

## Step 5 — `src/main.rs`: Wire Both Flags End-to-End

### 5a. Add to `Args` struct (after `skip_active_vacuums`):

```rust
#[arg(
    long,
    value_name = "BYTES",
    help = "Pause index acquisition when any replica lag exceeds this threshold in bytes. Disabled by default."
)]
max_replica_lag_bytes: Option<i64>,

#[arg(
    long,
    value_name = "SECONDS",
    default_value = "1800",
    help = "How long to wait for replica lag to drop before stopping workers and marking remaining indexes as skipped (default: 1800s = 30 min). Only applies when --max-replica-lag-bytes is set."
)]
max_replica_lag_wait_secs: Option<u64>,
```

### 5b. Add to `Config` struct:

```rust
max_replica_lag_bytes: Option<i64>,
max_replica_lag_wait_secs: Option<u64>,
```

### 5c. Add to `merge_config`:

```rust
if args.max_replica_lag_bytes.is_none() {
    args.max_replica_lag_bytes = config_file.max_replica_lag_bytes;
}
if args.max_replica_lag_wait_secs.is_none() {
    args.max_replica_lag_wait_secs = config_file.max_replica_lag_wait_secs;
}
```

### 5d. Add to `WorkerConfig` construction:

```rust
max_replica_lag_bytes: args.max_replica_lag_bytes,
max_replica_lag_wait_secs: args.max_replica_lag_wait_secs,
```

---

## Behaviour Summary

| Scenario | Result |
|---|---|
| `--max-replica-lag-bytes` not set | No lag polling; behaviour identical to today |
| Flag set, no replicas | Query returns 0; lag always ≤ threshold; no pause |
| Flag set, lag ≤ threshold | Normal acquisition; lag checked every loop iteration |
| Flag set, lag > threshold | Workers pause 5 s per cycle; warn every 60 s showing waited/limit |
| Lag persists beyond `--max-replica-lag-wait` (default 1800 s) | Workers exit loop; orchestrator calls `mark_all_pending_skipped()`; final stats show Skipped count; `reindex_state` table rows stay Pending for `--resume` |
| `pg_stat_replication` unreadable | Warning logged; acquisition proceeds without throttling (graceful degradation) |

---

## Verification

```bash
git checkout main && git checkout -b feat/adaptive-replication-checks

# implement changes…

cargo build

# Help check — new flag visible
./target/debug/pg-reindexer --help | grep max-replica-lag

# Backward compat — existing invocations unaffected
./target/debug/pg-reindexer --database mydb --schema public --dry-run

# Flag parsing
./target/debug/pg-reindexer --database mydb --schema public \
  --max-replica-lag-bytes 104857600 --dry-run   # 100 MB threshold

cargo test
```
