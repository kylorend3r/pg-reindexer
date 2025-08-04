use anyhow::{Context, Result};
use std::sync::Arc;
use crate::types::{IndexInfo, ReindexingCheckResults, SharedTableTracker};
use crate::logging;
use crate::deadlock::{check_and_handle_deadlock_risk, remove_table_from_tracker};

pub async fn get_indexes_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    max_size_gb: u64,
) -> Result<Vec<IndexInfo>> {
    let query = if let Some(_table) = table_name {
        crate::queries::GET_INDEXES_IN_SCHEMA_WITH_TABLE
    } else {
        crate::queries::GET_INDEXES_IN_SCHEMA
    };

    let rows = if let Some(table) = table_name {
        client
            .query(query, &[&schema_name, &table, &(max_size_gb as i64)])
            .await
    } else {
        client
            .query(query, &[&schema_name, &(max_size_gb as i64)])
            .await
    }
    .context("Failed to query indexes in schema")?;

    let mut indexes = Vec::new();

    for row in rows {
        let index = IndexInfo {
            schema_name: row.get(0),
            index_name: row.get(2),
            index_type: row.get(4),
        };
        indexes.push(index);
    }

    Ok(indexes)
}

pub async fn get_index_size(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<i64> {
    let rows = client
        .query(crate::queries::GET_INDEX_SIZE, &[&schema_name, &index_name])
        .await
        .context("Failed to query index size")?;

    if let Some(row) = rows.first() {
        let size: i64 = row.get(0);
        Ok(size)
    } else {
        Err(anyhow::anyhow!(
            "Index {}.{} not found",
            schema_name,
            index_name
        ))
    }
}

pub async fn validate_index_integrity(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<bool> {
    let rows = client
        .query(
            crate::queries::VALIDATE_INDEX_INTEGRITY,
            &[&schema_name, &index_name],
        )
        .await
        .context("Failed to query index integrity")?;

    if let Some(row) = rows.first() {
        let is_valid: bool = row.get(2);
        let is_ready: bool = row.get(3);
        let is_live: bool = row.get(4);

        // Index is considered healthy if it's valid, ready, and live
        Ok(is_valid && is_ready && is_live)
    } else {
        // If no rows returned, the index doesn't exist or there's an issue
        Ok(false)
    }
}

pub async fn reindex_index_with_client(
    client: Arc<tokio_postgres::Client>,
    schema_name: String,
    index_name: String,
    index_type: String,
    index_num: usize,
    total_indexes: usize,
    verbose: bool,
    skip_inactive_replication_slots: bool,
    skip_sync_replication_connection: bool,
    reindexing_results: Arc<ReindexingCheckResults>,
    shared_tracker: Arc<tokio::sync::Mutex<SharedTableTracker>>,
    logger: Arc<logging::Logger>,
) -> Result<()> {
    logger.log_index_start(
        index_num,
        total_indexes,
        &schema_name,
        &index_name,
        &index_type,
    );

    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Starting reindex process for {}.{}", schema_name, index_name),
    );

    // Get before size
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Getting before size for {}.{}", schema_name, index_name),
    );
    let before_size = get_index_size(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Before size for {}.{}: {} bytes", schema_name, index_name, before_size),
    );

    let reindex_sql = format!(
        "REINDEX INDEX CONCURRENTLY \"{}\".\"{}\"",
        schema_name, index_name
    );
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] SQL to execute: {}", reindex_sql),
    );

    // check if the index is invalid before reindexing
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Validating index integrity for {}.{}", schema_name, index_name),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Index {}.{} validity: {}", schema_name, index_name, index_is_valid),
    );

    // if the index is invalid, skip the reindexing.since reindexing an invalid index will cause duplicate entries in the index.
    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!(
                "Index is invalid, skipping reindexing {}.{}",
                schema_name, index_name
            ),
        );
        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "invalid_index".to_string(),
            before_size: None,
            after_size: None,
            size_change: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    if reindexing_results.active_vacuum
        || reindexing_results.active_pgreindexer
        || (reindexing_results.inactive_replication_slots && !skip_inactive_replication_slots)
        || (reindexing_results.sync_replication_connection && !skip_sync_replication_connection)
    {
        logger.log(
            logging::LogLevel::Info,
            &format!("[DEBUG] Skipping {}.{} due to reindexing conditions", schema_name, index_name),
        );
        logger.log_index_skipped(
            &schema_name,
            &index_name,
            "Active vacuum, pgreindexer or inactive replication slots detected",
        );

        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "skipped".to_string(),
            before_size: None,
            after_size: None,
            size_change: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Executing reindex for {}.{}", schema_name, index_name),
    );
    
    // Check for potential deadlock before executing reindex
    check_and_handle_deadlock_risk(&client, &schema_name, &index_name, &shared_tracker, &logger).await?;
    
    let start_time = std::time::Instant::now();
    let result = client.execute(&reindex_sql, &[]).await;
    let duration = start_time.elapsed();
    
    match &result {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Info,
                &format!("[DEBUG] Reindex SQL executed successfully for {}.{} in {:?}", 
                    schema_name, index_name, duration),
            );
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!("[DEBUG] Reindex SQL failed for {}.{} after {:?}: {}", 
                    schema_name, index_name, duration, e),
            );
        }
    }
    
    result.context(format!(
        "Failed to reindex index {}.{}",
        schema_name, index_name
    ))?;

    // Get after size
    let after_size = get_index_size(&client, &schema_name, &index_name).await?;
    let size_change = after_size - before_size;

    if verbose {
        logger.log_index_size_info(before_size, after_size, size_change);
    }

    // Additional check: validate index integrity before saving
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Waiting 5 seconds for index record to be written to table before validation for {}.{}", schema_name, index_name),
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Validating index integrity before saving for {}.{}", schema_name, index_name),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Final validation result for {}.{}: {}", schema_name, index_name, index_is_valid),
    );

    if !index_is_valid {
        logger.log(
            logging::LogLevel::Info,
            &format!("[DEBUG] Index validation failed for {}.{}", schema_name, index_name),
        );
        logger.log_index_validation_failed(&schema_name, &index_name);

        // Save failed validation record
        let index_data = crate::save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "validation_failed".to_string(),
            before_size: Some(before_size),
            after_size: Some(after_size),
            size_change: Some(size_change),
        };
        crate::save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    // save the index info
    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Saving success record for {}.{}", schema_name, index_name),
    );
    let index_data = crate::save::IndexData {
        schema_name: schema_name.clone(),
        index_name: index_name.clone(),
        index_type: index_type.clone(),
        reindex_status: "success".to_string(),
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(size_change),
    };
    crate::save::save_index_info(&client, &index_data).await?;

    logger.log(
        logging::LogLevel::Info,
        &format!("[DEBUG] Successfully completed reindex for {}.{}", schema_name, index_name),
    );
    logger.log_index_success(&schema_name, &index_name);

    // Remove table from shared tracker
    remove_table_from_tracker(&client, &schema_name, &index_name, &shared_tracker, &logger).await?;

    Ok(())
} 