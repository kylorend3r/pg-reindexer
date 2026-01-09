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
    // For database validation tests, if multiple databases are specified (comma-separated),
    // use only the first one since these tests work with a single database
    let database = database
        .split(',')
        .next()
        .unwrap_or("postgres")
        .trim()
        .to_string();
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

#[tokio::test]
#[ignore]
async fn test_multiple_schemas_validation() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema1 = format!("test_schema1_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_schema2 = format!("test_schema2_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_schema3 = format!("test_schema3_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Create multiple test schemas
    create_test_schema(&client, &test_schema1).await?;
    create_test_schema(&client, &test_schema2).await?;
    create_test_schema(&client, &test_schema3).await?;

    // Create tables and indexes in each schema
    create_test_table_with_index(&client, &test_schema1, "table1", "idx1").await?;
    create_test_table_with_index(&client, &test_schema2, "table2", "idx2").await?;
    create_test_table_with_index(&client, &test_schema3, "table3", "idx3").await?;

    // Test that all schemas exist
    let schemas = vec![&test_schema1, &test_schema2, &test_schema3];
    for schema in &schemas {
        let query = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)";
        let row = client.query_one(query, &[schema]).await?;
        let exists: bool = row.get(0);
        assert!(exists, "Schema {} should exist", schema);
    }

    // Test that we can query indexes from multiple schemas
    let query = "
        SELECT 
            n.nspname as schema_name,
            i.relname as index_name,
            t.relname as table_name
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = ANY($1)
        AND i.relkind = 'i'
        ORDER BY n.nspname, i.relname
    ";
    let rows = client.query(query, &[&schemas]).await?;

    // Should find indexes from all three schemas
    assert!(rows.len() >= 3, "Should find at least 3 indexes (one from each schema)");

    // Verify we have indexes from each schema
    let mut found_schemas = std::collections::HashSet::new();
    for row in &rows {
        let schema_name: String = row.get(0);
        found_schemas.insert(schema_name);
    }

    assert!(found_schemas.contains(&test_schema1), "Should find index from schema 1");
    assert!(found_schemas.contains(&test_schema2), "Should find index from schema 2");
    assert!(found_schemas.contains(&test_schema3), "Should find index from schema 3");

    // Cleanup
    drop_test_schema(&client, &test_schema1).await?;
    drop_test_schema(&client, &test_schema2).await?;
    drop_test_schema(&client, &test_schema3).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_indexes_from_multiple_schemas() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema1 = format!("test_schema1_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_schema2 = format!("test_schema2_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));

    // Create test schemas
    create_test_schema(&client, &test_schema1).await?;
    create_test_schema(&client, &test_schema2).await?;

    // Create multiple indexes in each schema
    create_test_table_with_index(&client, &test_schema1, "table1", "idx1_schema1").await?;
    create_test_table_with_index(&client, &test_schema1, "table2", "idx2_schema1").await?;
    create_test_table_with_index(&client, &test_schema2, "table1", "idx1_schema2").await?;
    create_test_table_with_index(&client, &test_schema2, "table2", "idx2_schema2").await?;

    // Query indexes from both schemas using the same pattern as get_indexes_in_schemas
    let query = "
        SELECT 
            n.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            ROUND(pg_relation_size(i.oid)::numeric/(1024*1024*1024), 2) || ' GB' as index_size,
            am.amname as index_type
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        WHERE n.nspname = ANY($1)
        AND i.relkind = 'i'
        AND x.indisprimary = false
        AND x.indisunique = false
        AND pg_relation_size(i.oid) >= ($2::bigint*1024*1024*1024)
        AND pg_relation_size(i.oid) < ($3::bigint*1024*1024*1024)
        ORDER BY pg_relation_size(i.oid) ASC
    ";
    let schemas = vec![&test_schema1, &test_schema2];
    let rows = client.query(query, &[&schemas, &(0i64), &(1024i64)]).await?;

    // Should find all 4 indexes (2 from each schema)
    assert!(rows.len() >= 4, "Should find at least 4 indexes from both schemas");

    // Verify indexes from both schemas are present
    let mut schema1_count = 0;
    let mut schema2_count = 0;
    for row in &rows {
        let schema_name: String = row.get(0);
        if schema_name == test_schema1 {
            schema1_count += 1;
        } else if schema_name == test_schema2 {
            schema2_count += 1;
        }
    }

    assert!(schema1_count >= 2, "Should find at least 2 indexes from schema 1");
    assert!(schema2_count >= 2, "Should find at least 2 indexes from schema 2");

    // Cleanup
    drop_test_schema(&client, &test_schema1).await?;
    drop_test_schema(&client, &test_schema2).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_multiple_schemas_max_limit() -> Result<()> {
    let client = create_test_connection().await?;
    
    // Test that we can handle multiple schemas (testing with 10 to keep it fast)
    let num_schemas = 10;
    let mut test_schemas = Vec::new();

    // Create multiple test schemas
    for i in 0..num_schemas {
        let schema_name = format!("test_schema_{}_{}", i, uuid::Uuid::new_v4().to_string().replace("-", "_"));
        create_test_schema(&client, &schema_name).await?;
        create_test_table_with_index(&client, &schema_name, "test_table", "test_idx").await?;
        test_schemas.push(schema_name);
    }

    // Verify all schemas exist
    for schema in &test_schemas {
        let query = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)";
        let row = client.query_one(query, &[schema]).await?;
        let exists: bool = row.get(0);
        assert!(exists, "Schema {} should exist", schema);
    }

    // Query indexes from all schemas
    let query = "
        SELECT 
            n.nspname as schema_name,
            i.relname as index_name
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = ANY($1)
        AND i.relkind = 'i'
    ";
    let rows = client.query(query, &[&test_schemas]).await?;

    // Should find at least one index from each schema
    assert!(rows.len() >= num_schemas, "Should find at least {} indexes (one from each schema)", num_schemas);

    // Verify we have indexes from all schemas
    let mut found_schemas = std::collections::HashSet::new();
    for row in &rows {
        let schema_name: String = row.get(0);
        found_schemas.insert(schema_name);
    }

    for schema in &test_schemas {
        assert!(found_schemas.contains(schema), "Should find index from schema {}", schema);
    }

    // Cleanup
    for schema in &test_schemas {
        drop_test_schema(&client, schema).await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_discover_all_user_schemas() -> Result<()> {
    let client = create_test_connection().await?;
    
    // Create multiple test schemas
    let test_schemas = vec![
        format!("test_discover_schema1_{}", uuid::Uuid::new_v4().to_string().replace("-", "_")),
        format!("test_discover_schema2_{}", uuid::Uuid::new_v4().to_string().replace("-", "_")),
        format!("test_discover_schema3_{}", uuid::Uuid::new_v4().to_string().replace("-", "_")),
    ];

    // Create test schemas
    for schema in &test_schemas {
        create_test_schema(&client, schema).await?;
    }

    // Test schema discovery function
    let discovered_schemas = pg_reindexer::schema::discover_all_user_schemas(&client).await?;

    // Verify that our test schemas are in the discovered list
    for schema in &test_schemas {
        assert!(
            discovered_schemas.contains(schema),
            "Discovered schemas should include {}",
            schema
        );
    }

    // Verify that system schemas are NOT in the discovered list
    let system_schemas = vec!["pg_catalog", "information_schema", "pg_toast"];
    for system_schema in system_schemas {
        assert!(
            !discovered_schemas.contains(&system_schema.to_string()),
            "Discovered schemas should NOT include system schema {}",
            system_schema
        );
    }

    // Verify that reindexer schema is NOT in the discovered list (if it exists)
    if discovered_schemas.contains(&"reindexer".to_string()) {
        // This is okay if reindexer schema was created by previous tests
        // But ideally it should be excluded
    }

    // Cleanup
    for schema in &test_schemas {
        drop_test_schema(&client, schema).await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_discover_all_schemas_finds_indexes() -> Result<()> {
    let client = create_test_connection().await?;
    
    // Create test schemas with indexes
    let test_schemas = vec![
        format!("test_discover_idx1_{}", uuid::Uuid::new_v4().to_string().replace("-", "_")),
        format!("test_discover_idx2_{}", uuid::Uuid::new_v4().to_string().replace("-", "_")),
    ];

    // Create test schemas and tables with indexes
    for schema in &test_schemas {
        create_test_schema(&client, schema).await?;
        create_test_table_with_index(&client, schema, "test_table", "test_idx").await?;
    }

    // Discover all schemas
    let discovered_schemas = pg_reindexer::schema::discover_all_user_schemas(&client).await?;

    // Verify our test schemas are discovered
    for schema in &test_schemas {
        assert!(
            discovered_schemas.contains(schema),
            "Discovered schemas should include {}",
            schema
        );
    }

    // Get indexes from discovered schemas
    let indexes = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &discovered_schemas.iter()
            .filter(|s| test_schemas.contains(s))
            .cloned()
            .collect::<Vec<_>>(),
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::All,
        None, // No ordering specified
    )
    .await?;

    // Should find at least one index from each test schema
    assert!(
        indexes.len() >= test_schemas.len(),
        "Should find at least {} indexes (one from each test schema)",
        test_schemas.len()
    );

    // Cleanup
    for schema in &test_schemas {
        drop_test_schema(&client, schema).await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_discover_all_schemas_excludes_reindexer_schema() -> Result<()> {
    let client = create_test_connection().await?;
    
    // Ensure reindexer schema exists (it might be created by the tool)
    let create_reindexer_schema = "CREATE SCHEMA IF NOT EXISTS reindexer";
    client.execute(create_reindexer_schema, &[]).await?;

    // Discover all schemas
    let discovered_schemas = pg_reindexer::schema::discover_all_user_schemas(&client).await?;

    // Verify that reindexer schema is NOT in the discovered list
    assert!(
        !discovered_schemas.contains(&"reindexer".to_string()),
        "Discovered schemas should NOT include the reindexer schema"
    );

    Ok(())
}

/// Helper to create a test table with multiple indexes of different sizes
async fn create_test_table_with_multiple_indexes(
    client: &Client,
    schema_name: &str,
    table_name: &str,
) -> Result<()> {
    // Create table with multiple columns for creating different sized indexes
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(255),
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )",
        schema_name, table_name
    );
    client.execute(&create_table, &[]).await?;

    // Insert some data to make indexes have different sizes
    for i in 1..=100 {
        let insert = format!(
            "INSERT INTO \"{}\".\"{}\" (name, email, description) VALUES ($1, $2, $3)",
            schema_name, table_name
        );
        client
            .execute(
                &insert,
                &[
                    &format!("name_{}", i),
                    &format!("email_{}@example.com", i),
                    &format!("Description for record {}", i),
                ],
            )
            .await?;
    }

    // Create indexes - these will have different sizes based on the data
    // Index on name (smaller)
    let create_idx1 = format!(
        "CREATE INDEX IF NOT EXISTS \"idx_{}_name\" ON \"{}\".\"{}\" (name)",
        table_name, schema_name, table_name
    );
    client.execute(&create_idx1, &[]).await?;

    // Index on email (medium)
    let create_idx2 = format!(
        "CREATE INDEX IF NOT EXISTS \"idx_{}_email\" ON \"{}\".\"{}\" (email)",
        table_name, schema_name, table_name
    );
    client.execute(&create_idx2, &[]).await?;

    // Index on description (larger)
    let create_idx3 = format!(
        "CREATE INDEX IF NOT EXISTS \"idx_{}_description\" ON \"{}\".\"{}\" (description)",
        table_name, schema_name, table_name
    );
    client.execute(&create_idx3, &[]).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_ordering_by_size_asc() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table with multiple indexes
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_multiple_indexes(&client, &test_schema, test_table).await?;

    // Get indexes ordered by size ascending
    let indexes = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &[test_schema.clone()],
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::Btree,
        Some("asc"),
    )
    .await?;

    // Should find at least some indexes
    assert!(!indexes.is_empty(), "Should find indexes");

    // Verify that indexes are ordered by size (ascending)
    let mut previous_size: Option<i64> = None;
    for index in &indexes {
        if let Some(size) = index.size_bytes {
            if let Some(prev) = previous_size {
                assert!(
                    size >= prev,
                    "Indexes should be ordered by size ascending. Index {} has size {} but previous was {}",
                    index.index_name,
                    size,
                    prev
                );
            }
            previous_size = Some(size);
        }
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_ordering_by_size_desc() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table with multiple indexes
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_multiple_indexes(&client, &test_schema, test_table).await?;

    // Get indexes ordered by size descending
    let indexes = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &[test_schema.clone()],
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::Btree,
        Some("desc"),
    )
    .await?;

    // Should find at least some indexes
    assert!(!indexes.is_empty(), "Should find indexes");

    // Verify that indexes are ordered by size (descending)
    let mut previous_size: Option<i64> = None;
    for index in &indexes {
        if let Some(size) = index.size_bytes {
            if let Some(prev) = previous_size {
                assert!(
                    size <= prev,
                    "Indexes should be ordered by size descending. Index {} has size {} but previous was {}",
                    index.index_name,
                    size,
                    prev
                );
            }
            previous_size = Some(size);
        }
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_index_size_bytes_field_populated() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table with index
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_index(&client, &test_schema, test_table, "test_idx").await?;

    // Get indexes without ordering
    let indexes = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &[test_schema.clone()],
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::Btree,
        None,
    )
    .await?;

    // Should find at least one index
    assert!(!indexes.is_empty(), "Should find at least one index");

    // Verify that size_bytes field is populated for all indexes
    for index in &indexes {
        assert!(
            index.size_bytes.is_some(),
            "Index {} should have size_bytes field populated",
            index.index_name
        );
        assert!(
            index.size_bytes.unwrap() >= 0,
            "Index {} should have non-negative size",
            index.index_name
        );
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_default_ordering_asc() -> Result<()> {
    let client = create_test_connection().await?;
    let test_schema = format!("test_schema_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    let test_table = "test_table";

    // Create test schema and table with multiple indexes
    create_test_schema(&client, &test_schema).await?;
    create_test_table_with_multiple_indexes(&client, &test_schema, test_table).await?;

    // Get indexes without specifying ordering (should default to ASC)
    let indexes_no_order = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &[test_schema.clone()],
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::Btree,
        None,
    )
    .await?;

    // Get indexes with explicit ASC ordering
    let indexes_asc = pg_reindexer::index_operations::get_indexes_in_schemas(
        &client,
        &[test_schema.clone()],
        None,
        0,
        1024,
        pg_reindexer::types::IndexFilterType::Btree,
        Some("asc"),
    )
    .await?;

    // Both should return the same number of indexes
    assert_eq!(
        indexes_no_order.len(),
        indexes_asc.len(),
        "Default ordering and explicit ASC should return same number of indexes"
    );

    // If we have multiple indexes, verify they're in ascending order
    if indexes_no_order.len() > 1 {
        let mut previous_size: Option<i64> = None;
        for index in &indexes_no_order {
            if let Some(size) = index.size_bytes {
                if let Some(prev) = previous_size {
                    assert!(
                        size >= prev,
                        "Default ordering should be ascending. Index {} has size {} but previous was {}",
                        index.index_name,
                        size,
                        prev
                    );
                }
                previous_size = Some(size);
            }
        }
    }

    // Cleanup
    drop_test_schema(&client, &test_schema).await?;

    Ok(())
}
