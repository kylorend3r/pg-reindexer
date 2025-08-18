use crate::types::ReindexingCheckResults;
use anyhow::{Context, Result};
use tokio_postgres::Client;


// check the active vacuums
pub async fn get_active_vacuum(client: &Client) -> Result<bool> {
    let rows = client
        .query(crate::queries::GET_ACTIVE_VACUUM, &[])
        .await
        .context("Failed to query active vacuums")?;
    Ok(rows.len() > 0)
}

// check the inactive replication slots
pub async fn get_inactive_replication_slots(client: &Client) -> Result<bool> {
    let rows = client
        .query(crate::queries::GET_INACTIVE_REPLICATION_SLOT_COUNT, &[])
        .await
        .context("Failed to query inactive replication slots")?;
    let inactive_replication_slot_count: i64 = rows.first().unwrap().get(0);
    Ok(inactive_replication_slot_count > 0)
}

// check the sync replication connection
pub async fn get_sync_replication_connection(client: &Client) -> Result<bool> {
    let rows = client
        .query(crate::queries::GET_SYNC_REPLICATION_CONNECTION_COUNT, &[])
        .await
        .context("Failed to query sync replication connection")?;
    let sync_replication_connection_count: i64 = rows.first().unwrap().get(0);
    Ok(sync_replication_connection_count > 0)
}

// Perform all reindexing checks once and return results
pub async fn perform_reindexing_checks(client: &Client) -> Result<ReindexingCheckResults> {
    let active_vacuum = get_active_vacuum(client).await?;
    let inactive_replication_slots = get_inactive_replication_slots(client).await?;
    let sync_replication_connection = get_sync_replication_connection(client).await?;

    Ok(ReindexingCheckResults {
        active_vacuum,
        active_pgreindexer,
        inactive_replication_slots,
        sync_replication_connection,
    })
}
