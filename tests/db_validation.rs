//! Database integration tests
//! Tests schema/table validation, thread/worker limits, and index selection
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
    let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("PG_PASSWORD")
        .expect("PG_PASSWORD environment variable must be set for database tests");

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

/// Helper to create a test schema
/// Note: PostgreSQL identifiers should be quoted if they contain special characters
async fn create_test_schema(client: &Client, schema_name: &str) -> Result<()> {
    // Use quoted identifier to handle any special characters
    let query = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema_name);
    client.execute(&query, &[]).await?;
    Ok(())
}

/// Helper to drop a test schema
async fn drop_test_schema(client: &Client, schema_name: &str) -> Result<()> {
    // Use quoted identifier to handle any special characters
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
    // Create table with quoted identifiers
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" (id SERIAL PRIMARY KEY, name VARCHAR(100))",
        schema_name, table_name
    );
    client.execute(&create_table, &[]).await?;

    // Create index with quoted identifiers
    let create_index = format!(
        "CREATE INDEX IF NOT EXISTS \"{}\" ON \"{}\".\"{}\" (name)",
        index_name, schema_name, table_name
    );
    client.execute(&create_index, &[]).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Ignore by default - requires database connection
async fn test_validate_schema_exists() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Create test schema
    create_test_schema(&client, &test_schema).await?;

    // Test that schema exists
    let query = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)";
    let row = client.query_one(query, &[&test_schema]).await?;
    let exists: bool = row.get(0);
    assert!(exists, "Schema should exist");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_validate_schema_not_exists() -> Result<()> {
    let client = create_test_connection().await?;
    let non_existent_schema = format!("non_existent_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Test that schema does not exist
    let query = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)";
    let row = client.query_one(query, &[&non_existent_schema]).await?;
    let exists: bool = row.get(0);
    assert!(!exists, "Schema should not exist");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_validate_table_exists() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, test_table, "test_idx").await?;

    // Test that table exists
    let query = "SELECT EXISTS(
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = $1 AND table_name = $2
    )";
    let row = client.query_one(query, &[&test_schema, &test_table]).await?;
    let exists: bool = row.get(0);
    assert!(exists, "Table should exist");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_validate_table_not_exists() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let non_existent_table = "non_existent_table";

    // Create test schema but not table
    create_test_schema(&client, &test_schema).await?;

    // Test that table does not exist
    let query = "SELECT EXISTS(
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = $1 AND table_name = $2
    )";
    let row = client.query_one(query, &[&test_schema, &non_existent_table]).await?;
    let exists: bool = row.get(0);
    assert!(!exists, "Table should not exist");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_max_parallel_workers() -> Result<()> {
    let client = create_test_connection().await?;

    // Query max_parallel_workers setting
    let query = "SHOW max_parallel_workers";
    let row = client.query_one(query, &[]).await?;
    let max_workers_str: String = row.get(0);
    let max_workers: u64 = max_workers_str.parse()?;

    // Should be a valid positive number
    assert!(max_workers > 0, "max_parallel_workers should be greater than 0");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_thread_worker_validation_logic() -> Result<()> {
    let client = create_test_connection().await?;

    // Get max_parallel_workers
    let query = "SHOW max_parallel_workers";
    let row = client.query_one(query, &[]).await?;
    let max_workers_str: String = row.get(0);
    let max_parallel_workers: u64 = max_workers_str.parse()?;

    // Test that threads * workers should not exceed max_parallel_workers
    // This is a logic test - we're testing the validation logic, not the actual validation function
    let threads = 2;
    let max_parallel_maintenance_workers = 2;
    let effective_workers = if max_parallel_maintenance_workers == 0 {
        2 // Default PostgreSQL value
    } else {
        max_parallel_maintenance_workers
    };

    let total_workers = threads as u64 * effective_workers;
    
    // This should be less than or equal to max_parallel_workers for a valid configuration
    // We're just testing that we can calculate this correctly
    assert!(
        total_workers <= max_parallel_workers || max_parallel_workers == 0,
        "Total workers calculation should be valid"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_indexes_in_schema() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";
    let test_index = "test_idx";

    // Create test schema, table, and index
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, test_table, test_index).await?;

    // Query for indexes in the schema
    let query = "
        SELECT 
            schemaname,
            indexname,
            tablename,
            indexdef
        FROM pg_indexes
        WHERE schemaname = $1
        ORDER BY indexname
    ";
    let rows = client.query(query, &[&test_schema]).await?;

    // Should find at least the index we created
    assert!(!rows.is_empty(), "Should find at least one index");
    
    let found_index = rows.iter().any(|row| {
        let index_name: String = row.get(1);
        index_name == test_index
    });
    assert!(found_index, "Should find the test index");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_size_filtering() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, test_table, "test_idx").await?;

    // Query index sizes
    let query = "
        SELECT 
            schemaname,
            relname,
            pg_size_pretty(pg_relation_size(indexrelid::regclass)) as size,
            pg_relation_size(indexrelid::regclass) as size_bytes
        FROM pg_stat_user_indexes
        WHERE schemaname = $1
    ";
    let rows = client.query(query, &[&test_schema]).await?;

    // Should be able to query index sizes
    assert!(!rows.is_empty(), "Should find index size information");

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_type_filtering() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, test_table, "test_idx").await?;

    // Query index types
    let query = "
        SELECT 
            schemaname,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = $1
    ";
    let rows = client.query(query, &[&test_schema]).await?;

    // Should find indexes and be able to determine their types
    assert!(!rows.is_empty(), "Should find indexes");

    // Check that we can identify btree indexes (most common)
    for row in &rows {
        let indexdef: String = row.get(2);
        // Most indexes are btree by default
        assert!(
            indexdef.contains("USING btree") || indexdef.contains("btree"),
            "Should be able to identify index type"
        );
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

