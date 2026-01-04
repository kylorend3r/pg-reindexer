use anyhow::{Context, Result};
use tokio_postgres::Client;

pub async fn create_index_info_table(client: &Client) -> Result<()> {
    // Check if the schema exists first to avoid potential hangs
    let schema_already_exists = schema_exists(client, "reindexer").await?;
    
    // Create the reindexer schema if it doesn't exist
    if !schema_already_exists {
        let create_schema_query = "CREATE SCHEMA reindexer";
        client
            .execute(create_schema_query, &[])
            .await
            .context("Failed to create reindexer schema. Please ensure the database user has CREATE privilege on the database.")?;
    }

    // Create the table if it doesn't exist
    let create_table_query = r#"
        CREATE TABLE IF NOT EXISTS reindexer.reindex_logbook (
            schema_name VARCHAR(255) NOT NULL,
            index_name VARCHAR(255) NOT NULL,
            index_type VARCHAR(255) NOT NULL,
            reindex_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reindex_status VARCHAR(255) NOT NULL,
            before_size BIGINT,
            after_size BIGINT,
            size_change REAL,
            reindex_duration REAL
        );
    "#;

    client
        .execute(create_table_query, &[])
        .await
        .context("Failed to create reindex_logbook table")?;

    Ok(())
}

pub async fn create_reindex_state_table(client: &Client) -> Result<()> {
    // Check if the schema exists first to avoid potential hangs
    let schema_already_exists = schema_exists(client, "reindexer").await?;
    
    // Create the reindexer schema if it doesn't exist
    if !schema_already_exists {
        let create_schema_query = "CREATE SCHEMA reindexer";
        client
            .execute(create_schema_query, &[])
            .await
            .context("Failed to create reindexer schema. Please ensure the database user has CREATE privilege on the database.")?;
    }

    // Create the state table if it doesn't exist
    let create_table_query = r#"
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
        );
    "#;

    client
        .execute(create_table_query, &[])
        .await
        .context("Failed to create reindex_state table")?;

    // Create index on state for faster queries
    let create_index_query = r#"
        CREATE INDEX IF NOT EXISTS idx_reindex_state_state 
        ON reindexer.reindex_state(state);
    "#;

    client
        .execute(create_index_query, &[])
        .await
        .context("Failed to create index on reindex_state")?;

    // Create index on session_id for faster queries
    let create_session_index_query = r#"
        CREATE INDEX IF NOT EXISTS idx_reindex_state_session_id 
        ON reindexer.reindex_state(session_id);
    "#;

    client
        .execute(create_session_index_query, &[])
        .await
        .context("Failed to create session_id index on reindex_state")?;

    Ok(())
}

/// Check if a schema exists in the database
pub async fn schema_exists(client: &Client, schema_name: &str) -> Result<bool> {
    let rows = client
        .query(crate::queries::CHECK_SCHEMA_EXISTS, &[&schema_name])
        .await
        .context("Failed to check if schema exists")?;

    if let Some(row) = rows.first() {
        let exists: bool = row.get(0);
        Ok(exists)
    } else {
        Ok(false)
    }
}

/// Check if a table exists in a specific schema
pub async fn table_exists(client: &Client, schema_name: &str, table_name: &str) -> Result<bool> {
    let rows = client
        .query(
            crate::queries::CHECK_TABLE_EXISTS,
            &[&table_name, &schema_name],
        )
        .await
        .context("Failed to check if table exists")?;

    if let Some(row) = rows.first() {
        let exists: bool = row.get(0);
        Ok(exists)
    } else {
        Ok(false)
    }
}

/// Discover all user schemas in the database (excluding system schemas)
pub async fn discover_all_user_schemas(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(crate::queries::GET_ALL_USER_SCHEMAS, &[])
        .await
        .context("Failed to discover user schemas")?;

    let mut schemas = Vec::new();
    for row in rows {
        let schema_name: String = row.get(0);
        schemas.push(schema_name);
    }

    Ok(schemas)
}
