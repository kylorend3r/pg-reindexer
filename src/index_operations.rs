use crate::config::DEFAULT_RETRY_DELAY_MS;
use crate::logging;
use crate::memory_table::SharedIndexMemoryTable;
use crate::orchestrator::WorkerConfig;
use crate::types::{IndexInfo, IndexFilterType};
use anyhow::{Context, Result};
use std::sync::Arc;

/// Check if an index name matches PostgreSQL's temporary concurrent reindex pattern
/// This matches names like "_ccnew", "_ccnew1", "_ccnew2", etc.
pub fn is_temporary_concurrent_reindex_index(index_name: &str) -> bool {
    if let Some(ccnew_pos) = index_name.find("_ccnew") {
        // Check if the part after "_ccnew" is either empty or consists only of digits
        let after_ccnew = &index_name[ccnew_pos + 6..];
        after_ccnew.is_empty() || after_ccnew.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

pub async fn get_indexes_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
) -> Result<Vec<IndexInfo>> {
    let has_table_filter = table_name.is_some();
    let query = crate::queries::build_indexes_query(has_table_filter, index_type);

    let rows = if let Some(table) = table_name {
        client
            .query(
                &query,
                &[
                    &schema_name,
                    &table,
                    &(min_size_gb as i64),
                    &(max_size_gb as i64),
                ],
            )
            .await
    } else {
        client
            .query(
                &query,
                &[&schema_name, &(min_size_gb as i64), &(max_size_gb as i64)],
            )
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

/// Get indexes from multiple schemas
pub async fn get_indexes_in_schemas(
    client: &tokio_postgres::Client,
    schema_names: &[String],
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
) -> Result<Vec<IndexInfo>> {
    let mut all_indexes = Vec::new();

    for schema_name in schema_names {
        let indexes = get_indexes_in_schema(
            client,
            schema_name,
            table_name,
            min_size_gb,
            max_size_gb,
            index_type,
        )
        .await?;
        all_indexes.extend(indexes);
    }

    Ok(all_indexes)
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
    config: WorkerConfig,
) -> Result<()> {
    logger.log(
        logging::LogLevel::Info,
        &format!("Worker {} started", worker_id),
    );

    // Create a connection for this worker
    let client = crate::connection::create_connection_with_session_parameters_ssl(
        &connection_string,
        config.maintenance_work_mem_gb,
        config.max_parallel_maintenance_workers,
        config.maintenance_io_concurrency,
        config.lock_timeout_seconds,
        config.use_ssl,
        config.accept_invalid_certs,
        config.ssl_ca_cert.clone(),
        config.ssl_client_cert.clone(),
        config.ssl_client_key.clone(),
        &logger,
    )
    .await?;

    let client = Arc::new(client);

    // Process indexes until none are available
    while memory_table.has_pending_indexes().await {
        // Try to acquire an index
        if let Some(index_info) = memory_table
            .try_acquire_index_for_worker(worker_id, &logger)
            .await
        {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Worker {} processing index {}.{}",
                    worker_id, index_info.schema_name, index_info.index_name
                ),
            );

            // Mark index as in_progress in state table if session_id is provided
            if let Some(ref sid) = config.session_id {
                if let Err(e) = crate::state::mark_index_in_progress(
                    &client,
                    &index_info.schema_name,
                    &index_info.index_name,
                    sid,
                )
                .await
                {
                    logger.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Failed to mark index {}.{} as in_progress in state table: {}",
                            index_info.schema_name, index_info.index_name, e
                        ),
                    );
                }
            }

            // Process the index
            let result = reindex_index_with_memory_table(
                client.clone(),
                index_info.clone(),
                worker_id,
                config.skip_inactive_replication_slots,
                config.skip_sync_replication_connection,
                config.skip_active_vacuums,
                logger.clone(),
                config.bloat_threshold,
                config.concurrently,
                config.user_index_type,
                config.session_id.clone(),
            )
            .await;

            // Handle the result
            match result {
                Ok(status) => {
                    // Update state table based on result
                    let state = match status {
                        crate::types::ReindexStatus::Success => {
                            crate::state::ReindexState::Completed
                        }
                        crate::types::ReindexStatus::Skipped
                        | crate::types::ReindexStatus::InvalidIndex
                        | crate::types::ReindexStatus::BelowBloatThreshold => {
                            crate::state::ReindexState::Skipped
                        }
                        _ => crate::state::ReindexState::Failed,
                    };

                    if let Some(ref _sid) = config.session_id {
                        if let Err(e) = crate::state::update_index_state(
                            &client,
                            &index_info.schema_name,
                            &index_info.index_name,
                            &state,
                        )
                        .await
                        {
                            logger.log(
                                logging::LogLevel::Warning,
                                &format!(
                                    "Failed to update state for index {}.{}: {}",
                                    index_info.schema_name, index_info.index_name, e
                                ),
                            );
                        }
                    }

                    match status {
                        crate::types::ReindexStatus::Success => {
                            memory_table
                                .release_index_and_mark_completed(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                        crate::types::ReindexStatus::Skipped
                        | crate::types::ReindexStatus::InvalidIndex
                        | crate::types::ReindexStatus::BelowBloatThreshold => {
                            memory_table
                                .release_index_and_mark_skipped(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                        _ => {
                            memory_table
                                .release_index_and_mark_failed(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                    }
                },
                Err(e) => {
                    logger.log(
                        logging::LogLevel::Error,
                        &format!(
                            "Worker {} failed to process index {}.{}: {}",
                            worker_id, index_info.schema_name, index_info.index_name, e
                        ),
                    );
                    
                    // Update state table
                    if let Some(ref _sid) = config.session_id {
                        if let Err(err) = crate::state::update_index_state(
                            &client,
                            &index_info.schema_name,
                            &index_info.index_name,
                            &crate::state::ReindexState::Failed,
                        )
                        .await
                        {
                            logger.log(
                                logging::LogLevel::Warning,
                                &format!(
                                    "Failed to update state for index {}.{}: {}",
                                    index_info.schema_name, index_info.index_name, err
                                ),
                            );
                        }
                    }

                    memory_table
                        .release_index_and_mark_failed(
                            &index_info.schema_name,
                            &index_info.index_name,
                            &index_info.table_name,
                            worker_id,
                            &logger,
                        )
                        .await;
                }
            }
        } else {
            // No available indexes, wait a bit before trying again
            tokio::time::sleep(tokio::time::Duration::from_millis(DEFAULT_RETRY_DELAY_MS)).await;
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
    user_index_type: IndexFilterType,
    _session_id: Option<String>,
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
    let before_size =
        get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;
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
    let index_is_valid =
        validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
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
            index_type: user_index_type.to_string(),
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
                index_type: user_index_type.to_string(),
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
            index_type: user_index_type.to_string(),
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
            logger.log_index_failed(
                &index_info.schema_name,
                &index_info.index_name,
                &format!("SQL execution failed after {:?}: {}", duration, e),
            );

            // Save failed reindex record
            let index_data = crate::save::IndexData {
                schema_name: index_info.schema_name.clone(),
                index_name: index_info.index_name.clone(),
                index_type: user_index_type.to_string(),
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
    let after_size =
        get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;

    // Calculate percentage reduction
    let percentage_reduction = if before_size > 0 {
        ((before_size - after_size) as f64 / before_size as f64) * 100.0
    } else {
        0.0
    };
    // Log after size with percentage reduction
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "After size for {}.{}: {} bytes, {:.1}% reduced",
            index_info.schema_name, index_info.index_name, after_size, percentage_reduction
        ),
    );
    // Final validation
    let index_is_valid =
        validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
    if !index_is_valid {
        logger.log_index_validation_failed(&index_info.schema_name, &index_info.index_name);

        // Save failed validation record
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.to_string(),
            reindex_status: crate::types::ReindexStatus::ValidationFailed,
            before_size: Some(before_size),
            after_size: Some(after_size),
            size_change: Some(((percentage_reduction * 10.0).round() / 10.0) as f32),
            reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::ValidationFailed);
    }

    // Save success record
    let index_data = crate::save::IndexData {
        schema_name: index_info.schema_name.clone(),
        index_name: index_info.index_name.clone(),
        index_type: user_index_type.to_string(),
        reindex_status: crate::types::ReindexStatus::Success,
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(percentage_reduction as f32),
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
        &format!(
            "Validating index integrity before dropping orphaned index: {}.{}",
            schema_name, index_name
        ),
    );

    let index_is_valid = validate_index_integrity(client, schema_name, index_name).await?;

    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!(
                "Index {}.{} is invalid, proceeding with drop operation",
                schema_name, index_name
            ),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Index {}.{} is valid, proceeding with drop operation",
                schema_name, index_name
            ),
        );
    }

    let drop_sql = format!(
        "DROP INDEX IF EXISTS \"{}\".\"{}\"",
        schema_name, index_name
    );

    logger.log(
        logging::LogLevel::Info,
        &format!("Dropping orphaned index: {}.{}", schema_name, index_name),
    );

    match client.execute(&drop_sql, &[]).await {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Success,
                &format!(
                    "Successfully dropped orphaned index: {}.{}",
                    schema_name, index_name
                ),
            );
            Ok(())
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!(
                    "Failed to drop orphaned index {}.{}: {}",
                    schema_name, index_name, e
                ),
            );
            Err(anyhow::anyhow!(
                "Failed to drop orphaned index {}.{}: {}",
                schema_name,
                index_name,
                e
            ))
        }
    }
}

/// Save excluded indexes to logbook before filtering them out
pub async fn save_excluded_indexes_to_logbook(
    client: &tokio_postgres::Client,
    indexes: &[IndexInfo],
    excluded_indexes: &std::collections::HashSet<String>,
    index_type: IndexFilterType,
    logger: &logging::Logger,
) -> Result<()> {
    for index in indexes {
        if excluded_indexes.contains(&index.index_name) {
            logger.log(
                logging::LogLevel::Info,
                &format!("Excluding index from reindexing: {}", index.index_name),
            );

            // Save excluded index to logbook
            let index_data = crate::save::IndexData {
                schema_name: index.schema_name.clone(),
                index_name: index.index_name.clone(),
                index_type: index_type.to_string(),
                reindex_status: crate::types::ReindexStatus::Excluded,
                before_size: None,
                after_size: None,
                size_change: None,
                reindex_duration: None,
            };

            if let Err(e) = crate::save::save_index_info(client, &index_data).await {
                logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "Failed to save excluded index info for {}.{}: {}",
                        index.schema_name, index.index_name, e
                    ),
                );
            }
        }
    }
    Ok(())
}

/// Filter out excluded indexes and orphaned _ccnew indexes from processing
pub fn filter_indexes(
    indexes: Vec<IndexInfo>,
    excluded_indexes: &std::collections::HashSet<String>,
    logger: &logging::Logger,
) -> Vec<IndexInfo> {
    indexes
        .into_iter()
        .filter(|index| {
            // Check if index is in exclude list
            if excluded_indexes.contains(&index.index_name) {
                return false;
            }

            // Check if index is a temporary concurrent reindex index
            if is_temporary_concurrent_reindex_index(&index.index_name) {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "Index appears to be a temporary concurrent reindex index (matches '_ccnew' pattern). Skipping reindexing: {}",
                        index.index_name
                    ),
                );
                return false;
            }

            true
        })
        .collect()
}
