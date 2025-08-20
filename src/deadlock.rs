use crate::logging;
use crate::types::SharedTableTracker;
use anyhow::{Context, Result};
use std::sync::Arc;

// Deadlock detection constants
const MAX_RETRIES: u32 = 12; // 1 hour max (12 * 5 minutes)
const DEADLOCK_WAIT_SECONDS: u64 = 300; // 5 minutes wait when deadlock detected

pub async fn get_table_name_for_index(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<String> {
    let query = r#"
        SELECT tablename 
        FROM pg_indexes 
        WHERE schemaname = $1 AND indexname = $2
    "#;

    let rows = client
        .query(query, &[&schema_name, &index_name])
        .await
        .context("Failed to query table name for index")?;

    if let Some(row) = rows.first() {
        let table_name: String = row.get(0);
        Ok(table_name)
    } else {
        Err(anyhow::anyhow!(
            "Table name not found for index {}.{}",
            schema_name,
            index_name
        ))
    }
}

pub async fn check_and_handle_deadlock_risk(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
    shared_tracker: &Arc<tokio::sync::Mutex<SharedTableTracker>>,
    logger: &logging::Logger,
) -> Result<()> {
    // Get the table name for this index
    let table_name = match get_table_name_for_index(client, schema_name, index_name).await {
        Ok(name) => name,
        Err(e) => {
            logger.log(
                logging::LogLevel::Warning,
                &format!("[DEBUG] Failed to get table name for index {}.{}: {}. Proceeding without deadlock check.", 
                    schema_name, index_name, e),
            );
            return Ok(());
        }
    };
    let full_table_name = format!("{}.{}", schema_name, table_name);

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Checking if the table {}.{} is being reindexed by another thread.",
            schema_name, full_table_name
        ),
    );

    let mut retry_count = 0;

    loop {
        // Atomically check and insert if not present
        let should_wait = {
            let mut tracker = shared_tracker.lock().await;
            
            // Check if our table is already being reindexed
            if tracker.tables_being_reindexed.contains(&full_table_name) {
                true // Need to wait
            } else {
                // No conflict, add our table to the tracker
                tracker.tables_being_reindexed.insert(full_table_name.clone());
                logger.log(
                    logging::LogLevel::Info,
                    &format!("[DEBUG] Added table {} to active reindex table", full_table_name),
                );
                false // Can proceed
            }
        };

        if should_wait {
            retry_count += 1;
            if retry_count > MAX_RETRIES {
                logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "[DEBUG] Maximum retries exceeded for {}.{}. Proceeding anyway.",
                        schema_name, index_name
                    ),
                );
                break;
            }

            logger.log(
                logging::LogLevel::Warning,
                &format!("[DEBUG] Potential deadlock detected! Table {} is already being reindexed. Waiting {} seconds... (retry {}/{})", 
                    full_table_name, DEADLOCK_WAIT_SECONDS, retry_count, MAX_RETRIES),
            );

            // Wait 5 minutes
            tokio::time::sleep(tokio::time::Duration::from_secs(DEADLOCK_WAIT_SECONDS)).await;

            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "[DEBUG] Retrying reindex for {}.{} after {} second wait",
                    schema_name, index_name, DEADLOCK_WAIT_SECONDS
                ),
            );

            // Continue the loop to check the tracker again after waiting
            continue;
        }

        // Successfully added to tracker, break out of the loop
        break;
    }

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] No deadlock risk detected for {}.{}",
            schema_name, index_name
        ),
    );

    Ok(())
}

pub async fn remove_table_from_tracker(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
    shared_tracker: &Arc<tokio::sync::Mutex<SharedTableTracker>>,
    logger: &logging::Logger,
) -> Result<()> {
    // Get the table name for this index
    let table_name = match get_table_name_for_index(client, schema_name, index_name).await {
        Ok(name) => name,
        Err(e) => {
            logger.log(
                logging::LogLevel::Warning,
                &format!("[DEBUG] Failed to get table name for index {}.{}: {}. Cannot remove from tracker.", 
                    schema_name, index_name, e),
            );
            return Ok(());
        }
    };
    let full_table_name = format!("{}.{}", schema_name, table_name);

    // Remove from shared tracker
    {
        let mut tracker = shared_tracker.lock().await;
        tracker.tables_being_reindexed.remove(&full_table_name);
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "[DEBUG] Removed table {} from reindex tracker",
                full_table_name
            ),
        );
    }

    Ok(())
}
