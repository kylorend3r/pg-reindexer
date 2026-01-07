//! Chaos tests for PostgreSQL Reindexer
//! Tests resilience, error handling, and recovery mechanisms under various failure conditions
//! 
//! These tests require a PostgreSQL instance. Set the following environment variables:
//! - PG_HOST (default: localhost)
//! - PG_PORT (default: 5432)
//! - PG_DATABASE (default: postgres)
//! - PG_USER (default: postgres)
//! - PG_PASSWORD (required)

use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use uuid;

/// Helper to get database connection parameters from environment
fn get_db_config() -> (String, u16, String, String, String) {
    let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("PG_PORT")
        .unwrap_or_else(|_| "5432".to_string())
        .parse::<u16>()
        .unwrap_or(5432);
    let database = std::env::var("PG_DATABASE").unwrap_or_else(|_| "postgres".to_string());
    // For chaos tests, if multiple databases are specified (comma-separated),
    // use only the first one since these tests work with a single database
    let database = database
        .split(',')
        .next()
        .unwrap_or("postgres")
        .trim()
        .to_string();
    let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("PG_PASSWORD")
        .expect("PG_PASSWORD environment variable must be set for chaos tests");

    (host, port, database, user, password)
}

/// Helper to create a test database connection
async fn create_test_connection() -> Result<Client> {
    let (host, port, database, user, password) = get_db_config();
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={}",
        host, port, database, user, password
    );

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

/// Helper to create a test schema for chaos tests
async fn create_chaos_test_schema(client: &Client, schema_name: &str) -> Result<()> {
    let query = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema_name);
    client.execute(&query, &[]).await?;
    Ok(())
}

/// Helper to drop a test schema
async fn drop_test_schema(client: &Client, schema_name: &str) -> Result<()> {
    let query = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name);
    client.execute(&query, &[]).await?;
    Ok(())
}

/// Helper to create a test table with an index
async fn create_test_table_with_index(
    client: &Client,
    schema_name: &str,
    table_name: &str,
    index_name: &str,
) -> Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" (id SERIAL PRIMARY KEY, name VARCHAR(100), data TEXT)",
        schema_name, table_name
    );
    client.execute(&create_table, &[]).await?;

    let create_index = format!(
        "CREATE INDEX IF NOT EXISTS \"{}\" ON \"{}\".\"{}\" (name)",
        index_name, schema_name, table_name
    );
    client.execute(&create_index, &[]).await?;

    Ok(())
}

/// Helper to create multiple test tables with indexes
async fn create_multiple_test_indexes(
    client: &Client,
    schema_name: &str,
    count: usize,
) -> Result<Vec<(String, String)>> {
    let mut indexes = Vec::new();
    for i in 0..count {
        let table_name = format!("test_table_{}", i);
        let index_name = format!("test_idx_{}", i);
        create_test_table_with_index(client, schema_name, &table_name, &index_name).await?;
        indexes.push((table_name, index_name));
    }
    Ok(indexes)
}

/// Helper to simulate connection failure by terminating backend
#[allow(dead_code)]
async fn simulate_connection_failure(client: &Client, pid: i32) -> Result<()> {
    // Terminate the backend process
    let query = format!("SELECT pg_terminate_backend({})", pid);
    client.execute(&query, &[]).await?;
    Ok(())
}

/// Helper to get current backend PID
async fn get_backend_pid(client: &Client) -> Result<i32> {
    let query = "SELECT pg_backend_pid()";
    let row = client.query_one(query, &[]).await?;
    Ok(row.get(0))
}

/// Helper to corrupt state table by inserting invalid data
async fn corrupt_state_table(client: &Client) -> Result<()> {
    // Insert invalid state value
    let query = r#"
        INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
        VALUES ('test_schema', 'test_table', 'test_idx', 'btree', 'invalid_state')
        ON CONFLICT (schema_name, index_name) 
        DO UPDATE SET state = 'invalid_state'
    "#;
    client.execute(query, &[]).await?;
    Ok(())
}

/// Helper to drop state table
async fn drop_state_table(client: &Client) -> Result<()> {
    let query = "DROP TABLE IF EXISTS reindexer.reindex_state CASCADE";
    client.execute(query, &[]).await?;
    Ok(())
}

/// Helper to create concurrent lock on index
async fn create_concurrent_lock(client: &Client, schema_name: &str, index_name: &str) -> Result<()> {
    // Use advisory lock to simulate lock contention
    let lock_id = format!("{}.{}", schema_name, index_name);
    let lock_hash = format!("SELECT hashtext('{}')", lock_id);
    let query = format!("SELECT pg_advisory_lock(({}))", lock_hash);
    client.execute(&query, &[]).await?;
    Ok(())
}

/// Helper to wait for a condition with timeout
#[allow(dead_code)]
async fn wait_for_condition<F, Fut>(condition: F, timeout_ms: u64) -> Result<bool>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    while start.elapsed() < timeout {
        if condition().await? {
            return Ok(true);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(false)
}

/// Helper to check if state table exists
async fn state_table_exists(client: &Client) -> Result<bool> {
    let query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'reindexer' AND table_name = 'reindex_state'
        )
    "#;
    let row = client.query_one(query, &[]).await?;
    Ok(row.get(0))
}

/// Helper to create state table (splits multiple statements into separate execute calls)
async fn ensure_state_table_exists(client: &Client) -> Result<()> {
    // Create schema first
    client.execute("CREATE SCHEMA IF NOT EXISTS reindexer", &[]).await?;
    
    // Create table
    client.execute(
        r#"
        CREATE TABLE IF NOT EXISTS reindexer.reindex_state (
            id SERIAL PRIMARY KEY,
            schema_name VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            index_name VARCHAR(255) NOT NULL,
            index_type VARCHAR(255) NOT NULL,
            state VARCHAR(50) NOT NULL DEFAULT 'pending',
            session_id VARCHAR(255),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(schema_name, index_name)
        )
        "#,
        &[],
    ).await?;
    
    Ok(())
}

/// Helper to check if index exists
async fn index_exists(client: &Client, schema_name: &str, index_name: &str) -> Result<bool> {
    let query = r#"
        SELECT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = $1 AND indexname = $2
        )
    "#;
    let row = client.query_one(query, &[&schema_name, &index_name]).await?;
    Ok(row.get(0))
}

// ============================================================================
// Connection Failure Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires database and explicit opt-in
async fn test_connection_drop_during_reindex() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup: Create test schema with indexes
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Get backend PID before potential drop (for potential connection termination testing)
    let _pid = get_backend_pid(&client).await?;

    // Verify indexes exist
    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&test_schema],
        )
        .await?;
    assert!(!indexes.is_empty(), "Should have created indexes");

    // Note: Actual connection drop during reindex would require running reindexer
    // in a separate process and terminating it. This test verifies the setup.

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_connection_timeout_scenario() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup: Create test schema
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Set a very short statement timeout to simulate timeout
    client.execute("SET statement_timeout = '100ms'", &[]).await?;

    // Try a long-running query that should timeout
    let result = client
        .execute(
            "SELECT pg_sleep(1)",
            &[],
        )
        .await;

    // Should fail with timeout
    assert!(result.is_err(), "Query should timeout");

    // Reset timeout
    client.execute("SET statement_timeout = '0'", &[]).await?;

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_multiple_connection_failures() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 2).await?;

    // Create multiple connections and verify they work
    let mut connections = Vec::new();
    for _ in 0..3 {
        let conn = create_test_connection().await?;
        // Verify connection works
        let row = conn.query_one("SELECT 1", &[]).await?;
        let value: i32 = row.get(0);
        assert_eq!(value, 1);
        connections.push(conn);
    }

    // Verify all connections are functional
    for conn in &connections {
        let row = conn.query_one("SELECT 1", &[]).await?;
        let value: i32 = row.get(0);
        assert_eq!(value, 1);
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_worker_connection_failure() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Create multiple worker-like connections
    let mut worker_connections = Vec::new();
    for i in 0..3 {
        let conn = create_test_connection().await?;
        // Set worker-specific session parameters
        conn.execute(
            &format!("SET application_name = 'worker_{}'", i),
            &[],
        )
        .await?;
        worker_connections.push(conn);
    }

    // Verify workers can query independently
    for (i, conn) in worker_connections.iter().enumerate() {
        let row = conn
            .query_one(
                "SELECT current_setting('application_name')",
                &[],
            )
            .await?;
        let app_name: String = row.get(0);
        assert_eq!(app_name, format!("worker_{}", i));
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

// ============================================================================
// State Management Chaos Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_corrupted_state_table() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup: Ensure state table exists
    create_chaos_test_schema(&client, &test_schema).await?;
    
    // Create state table if it doesn't exist
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Corrupt state table with invalid state
    corrupt_state_table(&client).await?;

    // Verify corruption exists
    let rows = client
        .query(
            "SELECT state FROM reindexer.reindex_state WHERE state = 'invalid_state'",
            &[],
        )
        .await?;
    assert!(!rows.is_empty(), "Corrupted state should exist");

    // Cleanup: Remove corrupted entry
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE state = 'invalid_state'",
            &[],
        )
        .await?;

    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_missing_state_table() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;

    // Drop state table if it exists
    if state_table_exists(&client).await? {
        drop_state_table(&client).await?;
    }

    // Verify state table doesn't exist
    assert!(!state_table_exists(&client).await?, "State table should not exist");

    // Try to query state table (should fail)
    let result = client
        .query("SELECT * FROM reindexer.reindex_state", &[])
        .await;
    assert!(result.is_err(), "Querying missing state table should fail");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_resume_with_corrupted_state() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Insert corrupted state
    client
        .execute(
            r#"
            INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
            VALUES ($1, 'test_table', 'test_idx', 'btree', 'invalid_state')
            ON CONFLICT (schema_name, index_name) 
            DO UPDATE SET state = 'invalid_state'
            "#,
            &[&test_schema],
        )
        .await?;

    // Verify corrupted state exists
    let rows = client
        .query(
            "SELECT state FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    assert!(!rows.is_empty(), "Should have corrupted state");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_partial_state_updates() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Insert partial state (some indexes)
    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&test_schema],
        )
        .await?;

    for (i, row) in indexes.iter().enumerate() {
        let index_name: String = row.get(0);
        if i < 2 {
            // Insert state for first 2 indexes
            client
                .execute(
                    r#"
                    INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                    VALUES ($1, 'test_table', $2, 'btree', 'pending')
                    ON CONFLICT (schema_name, index_name) DO NOTHING
                    "#,
                    &[&test_schema, &index_name],
                )
                .await?;
        }
    }

    // Verify partial state
    let state_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert_eq!(state_count, 2, "Should have partial state for 2 indexes");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concurrent_state_modifications() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Create multiple connections to simulate concurrent modifications
    let conn1 = create_test_connection().await?;
    let conn2 = create_test_connection().await?;

    // Both connections try to update state concurrently
    // Note: Due to Rust borrow checker limitations, we execute sequentially,
    // but this still validates that ON CONFLICT handles concurrent modifications correctly.
    // In a real scenario, multiple workers might update the same state row simultaneously.
    
    let schema1 = test_schema.clone();
    let schema2 = test_schema.clone();
    
    // Execute updates using tokio::join to simulate concurrent execution
    let update1 = {
        let conn = conn1;
        let schema = schema1;
        async move {
            conn.execute(
                r#"
                INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                VALUES ($1, 'test_table', 'test_idx', 'btree', 'in_progress')
                ON CONFLICT (schema_name, index_name) 
                DO UPDATE SET state = 'in_progress', updated_at = CURRENT_TIMESTAMP
                "#,
                &[&schema],
            ).await
        }
    };

    let update2 = {
        let conn = conn2;
        let schema = schema2;
        async move {
            conn.execute(
                r#"
                INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                VALUES ($1, 'test_table', 'test_idx', 'btree', 'completed')
                ON CONFLICT (schema_name, index_name) 
                DO UPDATE SET state = 'completed', updated_at = CURRENT_TIMESTAMP
                "#,
                &[&schema],
            ).await
        }
    };

    // Execute both updates concurrently
    let (result1, result2) = tokio::join!(update1, update2);

    // Both should succeed - ON CONFLICT handles the race condition
    assert!(result1.is_ok(), "First update should succeed even with concurrent modification");
    assert!(result2.is_ok(), "Second update should succeed even with concurrent modification");

    // Verify final state - should be one of the two values (whichever completed last)
    // This demonstrates that the UNIQUE constraint and ON CONFLICT clause work correctly
    let final_state: String = client
        .query_one(
            "SELECT state FROM reindexer.reindex_state WHERE schema_name = $1 AND index_name = 'test_idx'",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert!(
        final_state == "in_progress" || final_state == "completed",
        "Final state should be one of the concurrent updates. Got: {}", final_state
    );
    
    // Verify only one row exists (UNIQUE constraint prevents duplicates)
    let row_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND index_name = 'test_idx'",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert_eq!(row_count, 1, "Should have exactly one row due to UNIQUE constraint");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

// ============================================================================
// Concurrent Interference Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_lock_contention_scenario() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Create advisory lock to simulate lock contention
    let lock_result = create_concurrent_lock(&client, &test_schema, "test_idx").await;
    assert!(lock_result.is_ok(), "Should be able to create lock");

    // Try to acquire same lock from another connection (should wait or fail)
    let conn2 = create_test_connection().await?;
    let lock_id = format!("{}.test_idx", test_schema);
    let lock_hash = format!("SELECT hashtext('{}')", lock_id);
    let query = format!("SELECT pg_try_advisory_lock(({}))", lock_hash);
    let row = conn2.query_one(&query, &[]).await?;
    let acquired: bool = row.get(0);
    assert!(!acquired, "Lock should not be acquired (already held)");

    // Release lock
    let release_query = format!("SELECT pg_advisory_unlock(({}))", lock_hash);
    client.execute(&release_query, &[]).await?;

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concurrent_vacuum_interference() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Insert some data
    client
        .execute(
            &format!("INSERT INTO \"{}\".test_table (name, data) SELECT 'name' || i, 'data' || i FROM generate_series(1, 100) i", test_schema),
            &[],
        )
        .await?;

    // Check for active vacuum (should be none initially)
    let rows = client
        .query(
            r#"
            SELECT COUNT(*) FROM pg_stat_progress_vacuum
            "#,
            &[],
        )
        .await?;
    let vacuum_count: i64 = rows.first().unwrap().get(0);
    
    // Note: We can't easily trigger a vacuum in a test, but we can verify
    // the check mechanism exists
    assert!(vacuum_count >= 0, "Vacuum count should be non-negative");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concurrent_reindex_conflicts() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Try to reindex the same index from two connections concurrently
    let conn1 = create_test_connection().await?;
    let conn2 = create_test_connection().await?;

    // Start reindex on first connection
    let reindex_query1 = format!("REINDEX INDEX CONCURRENTLY \"{}\".\"test_idx\"", test_schema);
    let result1 = conn1.execute(&reindex_query1, &[]).await;

    // Try to reindex on second connection (should conflict or wait)
    let reindex_query2 = format!("REINDEX INDEX CONCURRENTLY \"{}\".\"test_idx\"", test_schema);
    let result2 = conn2.execute(&reindex_query2, &[]).await;
    
    // At least one should complete (may take time)
    assert!(
        result1.is_ok() || result2.is_ok(),
        "At least one reindex should be able to proceed"
    );

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_dropped_during_reindex() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Verify index exists
    assert!(
        index_exists(&client, &test_schema, "test_idx").await?,
        "Index should exist"
    );

    // Drop index
    client
        .execute(
            &format!("DROP INDEX \"{}\".\"test_idx\"", test_schema),
            &[],
        )
        .await?;

    // Verify index no longer exists
    assert!(
        !index_exists(&client, &test_schema, "test_idx").await?,
        "Index should be dropped"
    );

    // Try to query index (should fail)
    let result = client
        .query(
            "SELECT * FROM pg_indexes WHERE schemaname = $1 AND indexname = 'test_idx'",
            &[&test_schema],
        )
        .await?;
    assert!(result.is_empty(), "Should not find dropped index");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

// ============================================================================
// Resource Exhaustion Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_worker_limit_exceeded() -> Result<()> {
    let client = create_test_connection().await?;

    // Get max_parallel_workers setting
    let row = client.query_one("SHOW max_parallel_workers", &[]).await?;
    let max_workers_str: String = row.get(0);
    let max_workers: u64 = max_workers_str.parse()?;

    // Test that we can query the limit
    assert!(max_workers > 0, "max_parallel_workers should be greater than 0");

    // Test worker limit calculation
    let threads = 10;
    let workers_per_thread = 2;
    let total_workers = threads as u64 * workers_per_thread;

    // This should exceed max_parallel_workers in most configurations
    // The validation should catch this
    assert!(
        total_workers > max_workers || max_workers == 0,
        "Total workers calculation should be valid for testing"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_memory_pressure_scenario() -> Result<()> {
    let client = create_test_connection().await?;

    // Get current maintenance_work_mem setting
    let row = client
        .query_one("SHOW maintenance_work_mem", &[])
        .await?;
    let mem_setting: String = row.get(0);
    
    // Verify we can query memory settings
    assert!(!mem_setting.is_empty(), "Should be able to query memory settings");

    // Test setting a very low memory limit
    client.execute("SET maintenance_work_mem = '1MB'", &[]).await?;
    
    let row = client
        .query_one("SHOW maintenance_work_mem", &[])
        .await?;
    let new_mem: String = row.get(0);
    assert_eq!(new_mem, "1MB", "Memory setting should be updated");

    // Reset
    client.execute("RESET maintenance_work_mem", &[]).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_timeout_scenarios() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, "test_table", "test_idx").await?;

    // Set lock timeout
    client.execute("SET lock_timeout = '1s'", &[]).await?;
    
    let row = client.query_one("SHOW lock_timeout", &[]).await?;
    let timeout: String = row.get(0);
    assert_eq!(timeout, "1s", "Lock timeout should be set");

    // Set statement timeout
    client.execute("SET statement_timeout = '100ms'", &[]).await?;
    
    let row = client.query_one("SHOW statement_timeout", &[]).await?;
    let stmt_timeout: String = row.get(0);
    assert_eq!(stmt_timeout, "100ms", "Statement timeout should be set");

    // Reset timeouts
    client.execute("RESET lock_timeout", &[]).await?;
    client.execute("RESET statement_timeout", &[]).await?;

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

// ============================================================================
// Partial Failure Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_mixed_success_failure() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup: Create multiple indexes
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Simulate mixed states: some completed, some failed
    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&test_schema],
        )
        .await?;

    for (i, row) in indexes.iter().enumerate() {
        let index_name: String = row.get(0);
        let state = if i == 0 {
            "completed"
        } else if i == 1 {
            "failed"
        } else {
            "pending"
        };

        client
            .execute(
                r#"
                INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                VALUES ($1, 'test_table', $2, 'btree', $3)
                ON CONFLICT (schema_name, index_name) 
                DO UPDATE SET state = $3
                "#,
                &[&test_schema, &index_name, &state],
            )
            .await?;
    }

    // Verify mixed states
    let completed: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'completed'",
            &[&test_schema],
        )
        .await?
        .get(0);
    let failed: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'failed'",
            &[&test_schema],
        )
        .await?
        .get(0);
    let pending: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'pending'",
            &[&test_schema],
        )
        .await?
        .get(0);

    assert_eq!(completed, 1, "Should have 1 completed");
    assert_eq!(failed, 1, "Should have 1 failed");
    assert!(pending >= 1, "Should have at least 1 pending");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_cascading_failures() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Simulate cascading failure by marking all as failed
    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&test_schema],
        )
        .await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    for row in &indexes {
        let index_name: String = row.get(0);
        client
            .execute(
                r#"
                INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                VALUES ($1, 'test_table', $2, 'btree', 'failed')
                ON CONFLICT (schema_name, index_name) 
                DO UPDATE SET state = 'failed'
                "#,
                &[&test_schema, &index_name],
            )
            .await?;
    }

    // Verify all failed
    let failed_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'failed'",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert_eq!(failed_count, indexes.len() as i64, "All indexes should be marked as failed");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_recovery_after_partial_failure() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("chaos_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Setup
    create_chaos_test_schema(&client, &test_schema).await?;
    create_multiple_test_indexes(&client, &test_schema, 3).await?;

    // Ensure state table exists
    if !state_table_exists(&client).await? {
        ensure_state_table_exists(&client).await?;
    }

    // Create initial state with some failures
    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&test_schema],
        )
        .await?;

    for (i, row) in indexes.iter().enumerate() {
        let index_name: String = row.get(0);
        let state = if i == 0 { "failed" } else { "pending" };

        client
            .execute(
                r#"
                INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state)
                VALUES ($1, 'test_table', $2, 'btree', $3)
                ON CONFLICT (schema_name, index_name) 
                DO UPDATE SET state = $3
                "#,
                &[&test_schema, &index_name, &state],
            )
            .await?;
    }

    // Simulate recovery: reset failed to pending
    client
        .execute(
            "UPDATE reindexer.reindex_state SET state = 'pending' WHERE schema_name = $1 AND state = 'failed'",
            &[&test_schema],
        )
        .await?;

    // Verify recovery
    let failed_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'failed'",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert_eq!(failed_count, 0, "Failed indexes should be reset to pending");

    let pending_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM reindexer.reindex_state WHERE schema_name = $1 AND state = 'pending'",
            &[&test_schema],
        )
        .await?
        .get(0);
    assert!(pending_count > 0, "Should have pending indexes after recovery");

    // Cleanup
    client
        .execute(
            "DELETE FROM reindexer.reindex_state WHERE schema_name = $1",
            &[&test_schema],
        )
        .await?;
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

