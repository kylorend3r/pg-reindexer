use crate::logging;
use crate::types::IndexInfo;
use crate::memory_table::SharedIndexMemoryTable;
use anyhow::{Context, Result};
use std::sync::Arc;

pub async fn get_indexes_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: &str,
) -> Result<Vec<IndexInfo>> {
    let query = match (table_name, index_type) {
        (Some(_), "btree") => crate::queries::GET_BTREE_INDEXES_IN_SCHEMA_WITH_TABLE,
        (Some(_), "constraint") => crate::queries::GET_CONSTRAINT_INDEXES_IN_SCHEMA_WITH_TABLE,
        (Some(_), _) => crate::queries::GET_INDEXES_IN_SCHEMA_WITH_TABLE, // fallback to original behavior
        (None, "btree") => crate::queries::GET_BTREE_INDEXES_IN_SCHEMA,
        (None, "constraint") => crate::queries::GET_CONSTRAINT_INDEXES_IN_SCHEMA,
        (None, _) => crate::queries::GET_INDEXES_IN_SCHEMA, // fallback to original behavior
    };

    let rows = if let Some(table) = table_name {
        client
            .query(query, &[&schema_name, &table, &(min_size_gb as i64), &(max_size_gb as i64)])
            .await
    } else {
        client
            .query(query, &[&schema_name, &(min_size_gb as i64), &(max_size_gb as i64)])
            .await
    }
    .context("Failed to query indexes in schema")?;

    let mut indexes = Vec::new();

    for row in rows {
        let index = IndexInfo {
            schema_name: row.get(0),
            index_name: row.get(2),
            index_type: row.get(4),
            table_name: row.get(1),
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

pub async fn get_index_bloat_ratio(
    client: &tokio_postgres::Client,
    index_name: &str,
) -> Result<f64> {
    let rows = client
        .query(crate::queries::GET_INDEX_BLOAT_RATIO, &[&index_name])
        .await
        .context("Failed to query index bloat ratio")?;

    if let Some(row) = rows.first() {
        // Handle numeric type conversion by getting as string and parsing
        let bloat_percentage_str: String = row.get(0);
        let bloat_percentage_f64 = bloat_percentage_str.parse::<f64>().unwrap_or(0.0);
        Ok(bloat_percentage_f64)
    } else {
        // If no rows returned, assume no bloat
        Ok(0.0)
    }
}


/// Worker function that processes indexes using the memory table for concurrency control
pub async fn worker_with_memory_table(
    worker_id: usize,
    connection_string: String,
    memory_table: Arc<SharedIndexMemoryTable>,
    logger: Arc<logging::Logger>,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
    skip_inactive_replication_slots: bool,
    skip_sync_replication_connection: bool,
    skip_active_vacuums: bool,
    bloat_threshold: Option<u8>,
    concurrently: bool,
    use_ssl: bool,
    accept_invalid_certs: bool,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    user_index_type: String,
) -> Result<()> {
    logger.log(
        logging::LogLevel::Info,
        &format!("Worker {} started", worker_id),
    );

    // Create a connection for this worker
    let client = crate::connection::create_connection_with_session_parameters_ssl(
        &connection_string,
        maintenance_work_mem_gb,
        max_parallel_maintenance_workers,
        maintenance_io_concurrency,
        lock_timeout_seconds,
        use_ssl,
        accept_invalid_certs,
        ssl_ca_cert,
        ssl_client_cert,
        ssl_client_key,
        &logger,
    )
    .await?;

    let client = Arc::new(client);

    // Process indexes until none are available
    while memory_table.has_pending_indexes().await {
        // Try to acquire an index
        if let Some(index_info) = memory_table.try_acquire_index_for_worker(worker_id, &logger).await {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Worker {} processing index {}.{}",
                    worker_id, index_info.schema_name, index_info.index_name
                ),
            );

            // Process the index
            let result = reindex_index_with_memory_table(
                client.clone(),
                index_info.clone(),
                worker_id,
                skip_inactive_replication_slots,
                skip_sync_replication_connection,
                skip_active_vacuums,
                logger.clone(),
                bloat_threshold,
                concurrently,
                user_index_type.clone(),
            ).await;

            // Handle the result
            match result {
                Ok(status) => {
                    match status {
                        crate::types::ReindexStatus::Success => {
                            memory_table.release_index_and_mark_completed(
                                &index_info.schema_name,
                                &index_info.index_name,
                                &index_info.table_name,
                                worker_id,
                                &logger,
                            ).await;
                        }
                        crate::types::ReindexStatus::Skipped | 
                        crate::types::ReindexStatus::InvalidIndex | 
                        crate::types::ReindexStatus::BelowBloatThreshold => {
                            memory_table.release_index_and_mark_skipped(
                                &index_info.schema_name,
                                &index_info.index_name,
                                &index_info.table_name,
                                worker_id,
                                &logger,
                            ).await;
                        }
                        _ => {
                            memory_table.release_index_and_mark_failed(
                                &index_info.schema_name,
                                &index_info.index_name,
                                &index_info.table_name,
                                worker_id,
                                &logger,
                            ).await;
                        }
                    }
                }
                Err(e) => {
                    logger.log(
                        logging::LogLevel::Error,
                        &format!(
                            "Worker {} failed to process index {}.{}: {}",
                            worker_id, index_info.schema_name, index_info.index_name, e
                        ),
                    );
                    memory_table.release_index_and_mark_failed(
                        &index_info.schema_name,
                        &index_info.index_name,
                        &index_info.table_name,
                        worker_id,
                        &logger,
                    ).await;
                }
            }
        } else {
            // No available indexes, wait a bit before trying again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("Worker {} completed", worker_id),
    );

    Ok(())
}

/// Reindex an index using the memory table (simplified version without shared tracker)
pub async fn reindex_index_with_memory_table(
    client: Arc<tokio_postgres::Client>,
    index_info: IndexInfo,
    worker_id: usize,
    skip_inactive_replication_slots: bool,
    skip_sync_replication_connection: bool,
    skip_active_vacuums: bool,
    logger: Arc<logging::Logger>,
    bloat_threshold: Option<u8>,
    concurrently: bool,
    user_index_type: String,
) -> Result<crate::types::ReindexStatus> {
    logger.log_index_start(
        worker_id,
        0, // We don't know total count in this context
        &index_info.schema_name,
        &index_info.index_name,
        &index_info.index_type,
    );

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting pre-reindex checks for {}.{}",
            index_info.schema_name, index_info.index_name
        ),
    );

    // Get before size
    let before_size = get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Before size for {}.{}: {} bytes",
            index_info.schema_name, index_info.index_name, before_size
        ),
    );

    let reindex_sql = if concurrently {
        format!(
            "REINDEX INDEX CONCURRENTLY \"{}\".\"{}\"",
            index_info.schema_name, index_info.index_name
        )
    } else {
        format!(
            "REINDEX INDEX \"{}\".\"{}\"",
            index_info.schema_name, index_info.index_name
        )
    };

    // Check if the index is invalid before reindexing
    let index_is_valid = validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!(
                "Index is invalid, skipping reindexing {}.{}",
                index_info.schema_name, index_info.index_name
            ),
        );
        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.clone(),
            reindex_status: crate::types::ReindexStatus::InvalidIndex,
            before_size: None,
            after_size: None,
            size_change: None,
            reindex_duration: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::InvalidIndex);
    }

    // Check bloat ratio if threshold is specified
    if let Some(threshold) = bloat_threshold {
        let bloat_ratio = get_index_bloat_ratio(&client, &index_info.index_name).await?;
        if bloat_ratio < threshold as f64 {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Index bloat ratio ({}%) is below threshold ({}%), skipping reindexing {}.{}",
                    bloat_ratio, threshold, index_info.schema_name, index_info.index_name
                ),
            );
            // Save skipped record to logbook
            let index_data = crate::save::IndexData {
                schema_name: index_info.schema_name.clone(),
                index_name: index_info.index_name.clone(),
                index_type: user_index_type.clone(),
                reindex_status: crate::types::ReindexStatus::BelowBloatThreshold,
                before_size: None,
                after_size: None,
                size_change: None,
                reindex_duration: None,
            };
            crate::save::save_index_info(&client, &index_data).await?;
            return Ok(crate::types::ReindexStatus::BelowBloatThreshold);
        }
    }

    // Perform fresh reindexing checks
    let reindexing_results = crate::checks::perform_reindexing_checks(&client).await?;
    
    // Determine specific skip reason
    let mut skip_reasons = Vec::new();
    if reindexing_results.active_vacuum && !skip_active_vacuums {
        skip_reasons.push("active vacuum");
    }
    if reindexing_results.inactive_replication_slots && !skip_inactive_replication_slots {
        skip_reasons.push("inactive replication slots");
    }
    if reindexing_results.sync_replication_connection && !skip_sync_replication_connection {
        skip_reasons.push("sync replication connection");
    }

    if !skip_reasons.is_empty() {
        let skip_reason = skip_reasons.join(", ");
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Skipping {}.{} due to: {}",
                index_info.schema_name, index_info.index_name, skip_reason
            ),
        );

        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.clone(),
            reindex_status: crate::types::ReindexStatus::Skipped,
            before_size: None,
            after_size: None,
            size_change: None,
            reindex_duration: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::Skipped);
    }

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting reindex operation for {}.{}",
            index_info.schema_name, index_info.index_name
        ),
    );


    let start_time = std::time::Instant::now();
    let result = client.execute(&reindex_sql, &[]).await;
    let duration = start_time.elapsed();

    match &result {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Reindex SQL executed successfully for {}.{} in {:?}",
                    index_info.schema_name, index_info.index_name, duration
                ),
            );
        }
        Err(e) => {
            logger.log_index_failed(&index_info.schema_name, &index_info.index_name, &format!("SQL execution failed after {:?}: {}", duration, e));
            
            // Save failed reindex record
            let index_data = crate::save::IndexData {
                schema_name: index_info.schema_name.clone(),
                index_name: index_info.index_name.clone(),
                index_type: user_index_type.clone(),
                reindex_status: crate::types::ReindexStatus::Failed,
                before_size: Some(before_size),
                after_size: None,
                size_change: None,
                reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
            };
            crate::save::save_index_info(&client, &index_data).await?;
            
            return Ok(crate::types::ReindexStatus::Failed);
        }
    }

    // Get after size
    let after_size = get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;
    let size_change = after_size - before_size;

    
    // Final validation
    let index_is_valid = validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
    if !index_is_valid {
        logger.log_index_validation_failed(&index_info.schema_name, &index_info.index_name);

        // Save failed validation record
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.clone(),
            reindex_status: crate::types::ReindexStatus::ValidationFailed,
            before_size: Some(before_size),
            after_size: Some(after_size),
            size_change: Some(size_change),
            reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::ValidationFailed);
    }

    // Save success record
    let index_data = crate::save::IndexData {
        schema_name: index_info.schema_name.clone(),
        index_name: index_info.index_name.clone(),
        index_type: user_index_type.clone(),
        reindex_status: crate::types::ReindexStatus::Success,
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(size_change),
        reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
    };
    crate::save::save_index_info(&client, &index_data).await?;

    logger.log_index_success(&index_info.schema_name, &index_info.index_name);
    Ok(crate::types::ReindexStatus::Success)
}

/// Drop a single orphaned _ccnew index after validating it's safe to drop
pub async fn clean_orphaned_ccnew_index(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
    logger: &logging::Logger,
) -> Result<()> {
    // First validate the index integrity to ensure it's safe to drop
    logger.log(
        logging::LogLevel::Info,
        &format!("Validating index integrity before dropping orphaned index: {}.{}", schema_name, index_name),
    );
    
    let index_is_valid = validate_index_integrity(client, schema_name, index_name).await?;
    
    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!("Index {}.{} is invalid, proceeding with drop operation", schema_name, index_name),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!("Index {}.{} is valid, proceeding with drop operation", schema_name, index_name),
        );
    }
    
    let drop_sql = format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", schema_name, index_name);
    
    logger.log(
        logging::LogLevel::Info,
        &format!("Dropping orphaned index: {}.{}", schema_name, index_name),
    );

    match client.execute(&drop_sql, &[]).await {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Success,
                &format!("Successfully dropped orphaned index: {}.{}", schema_name, index_name),
            );
            Ok(())
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!("Failed to drop orphaned index {}.{}: {}", schema_name, index_name, e),
            );
            Err(anyhow::anyhow!("Failed to drop orphaned index {}.{}: {}", schema_name, index_name, e))
        }
    }
}
