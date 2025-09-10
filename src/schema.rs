use anyhow::{Context, Result};
use tokio_postgres::Client;

pub async fn create_index_info_table(client: &Client) -> Result<()> {
    // Create the reindexer schema if it doesn't exist
    let create_schema_query = "CREATE SCHEMA IF NOT EXISTS reindexer";
    client
        .execute(create_schema_query, &[])
        .await
        .context("Failed to create reindexer schema")?;

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
            size_change BIGINT
        );
    "#;

    client
        .execute(create_table_query, &[])
        .await
        .context("Failed to create reindex_logbook table")?;

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
