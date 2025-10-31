use crate::types::IndexInfo;
use anyhow::{Context, Result};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum ReindexState {
    #[allow(dead_code)]
    Pending,
    #[allow(dead_code)]
    InProgress,
    Completed,
    Failed,
    Skipped,
}

impl std::fmt::Display for ReindexState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReindexState::Pending => write!(f, "pending"),
            ReindexState::InProgress => write!(f, "in_progress"),
            ReindexState::Completed => write!(f, "completed"),
            ReindexState::Failed => write!(f, "failed"),
            ReindexState::Skipped => write!(f, "skipped"),
        }
    }
}

// Removed from_str as it's not currently used

/// Generate a new session ID
pub fn generate_session_id() -> String {
    Uuid::new_v4().to_string()
}

/// Initialize state table with indexes for a new session
pub async fn initialize_state_table(
    client: &Client,
    indexes: &[IndexInfo],
    session_id: &str,
) -> Result<()> {
    // Clear any existing in_progress states (from previous crashed session)
    client
        .execute(
            "UPDATE reindexer.reindex_state SET state = 'pending', session_id = NULL WHERE state = 'in_progress'",
            &[],
        )
        .await
        .context("Failed to reset in_progress states")?;

    // Insert or update indexes
    for index in indexes {
        let query = r#"
            INSERT INTO reindexer.reindex_state (schema_name, table_name, index_name, index_type, state, session_id)
            VALUES ($1, $2, $3, $4, 'pending', $5)
            ON CONFLICT (schema_name, index_name) 
            DO UPDATE SET 
                state = CASE 
                    WHEN reindexer.reindex_state.state IN ('completed', 'skipped') THEN reindexer.reindex_state.state
                    ELSE 'pending'
                END,
                session_id = CASE 
                    WHEN reindexer.reindex_state.state IN ('completed', 'skipped') THEN reindexer.reindex_state.session_id
                    ELSE $5
                END,
                updated_at = CURRENT_TIMESTAMP,
                table_name = EXCLUDED.table_name,
                index_type = EXCLUDED.index_type
        "#;

        client
            .execute(
                query,
                &[
                    &index.schema_name,
                    &index.table_name,
                    &index.index_name,
                    &index.index_type,
                    &session_id,
                ],
            )
            .await
            .context("Failed to insert index state")?;
    }

    Ok(())
}

/// Update state for a specific index
pub async fn update_index_state(
    client: &Client,
    schema_name: &str,
    index_name: &str,
    state: &ReindexState,
) -> Result<()> {
    let query = r#"
        UPDATE reindexer.reindex_state
        SET state = $1, updated_at = CURRENT_TIMESTAMP
        WHERE schema_name = $2 AND index_name = $3
    "#;

    client
        .execute(query, &[&state.to_string(), &schema_name, &index_name])
        .await
        .context("Failed to update index state")?;

    Ok(())
}

/// Mark index as in_progress
pub async fn mark_index_in_progress(
    client: &Client,
    schema_name: &str,
    index_name: &str,
    session_id: &str,
) -> Result<()> {
    let query = r#"
        UPDATE reindexer.reindex_state
        SET state = 'in_progress', session_id = $1, updated_at = CURRENT_TIMESTAMP
        WHERE schema_name = $2 AND index_name = $3
    "#;

    client
        .execute(query, &[&session_id, &schema_name, &index_name])
        .await
        .context("Failed to mark index as in_progress")?;

    Ok(())
}

// Removed load_pending_indexes as it's not currently used (resume now uses initialize_state_table directly)

// Removed clear_completed_states as it's not currently used

/// Clear all state for a schema (start fresh)
pub async fn clear_schema_state(client: &Client, schema_name: &str) -> Result<()> {
    let query = r#"
        DELETE FROM reindexer.reindex_state
        WHERE schema_name = $1
    "#;

    client
        .execute(query, &[&schema_name])
        .await
        .context("Failed to clear schema state")?;

    Ok(())
}

// Removed get_state_statistics as it's not currently used

/// Check if there are any pending indexes in the state table
pub async fn has_pending_indexes(client: &Client, session_id: Option<&str>) -> Result<bool> {
    let query = if session_id.is_some() {
        "SELECT COUNT(*) FROM reindexer.reindex_state WHERE session_id = $1 AND state IN ('pending', 'failed', 'in_progress')"
    } else {
        "SELECT COUNT(*) FROM reindexer.reindex_state WHERE state IN ('pending', 'failed', 'in_progress')"
    };

    let rows = if let Some(sid) = session_id {
        client.query(query, &[&sid]).await
    } else {
        client.query(query, &[]).await
    }
    .context("Failed to check for pending indexes")?;

    if let Some(row) = rows.first() {
        let count: i64 = row.get(0);
        Ok(count > 0)
    } else {
        Ok(false)
    }
}

