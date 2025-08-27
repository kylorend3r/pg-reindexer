use crate::deadlock::{check_and_handle_deadlock_risk, remove_table_from_tracker};
use crate::logging;
use crate::types::{IndexInfo, SharedTableTracker};
use anyhow::{Context, Result};
use std::sync::Arc;
use rand::Rng;
use std::{thread, time};

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

pub async fn reindex_index_with_client(
    client: Arc<tokio_postgres::Client>,
    schema_name: String,
    index_name: String,
    index_type: String,
    index_num: usize,
    total_indexes: usize,
    skip_inactive_replication_slots: bool,
    skip_sync_replication_connection: bool,
    skip_active_vacuums: bool,
    shared_tracker: Arc<tokio::sync::Mutex<SharedTableTracker>>,
    logger: Arc<logging::Logger>,
    bloat_threshold: Option<u8>,
    concurrently: bool,
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
        &format!(
            "Starting pre-reindex checks for {}.{}",
            schema_name, index_name
        ),
    );

    // Get before size
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Getting before size for {}.{}",
            schema_name, index_name
        ),
    );
    let before_size = get_index_size(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Before size for {}.{}: {} bytes",
            schema_name, index_name, before_size
        ),
    );

    let reindex_sql = if concurrently {
        format!(
            "REINDEX INDEX CONCURRENTLY \"{}\".\"{}\"",
            schema_name, index_name
        )
    } else {
        format!(
            "REINDEX INDEX \"{}\".\"{}\"",
            schema_name, index_name
        )
    };

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Using {} reindexing for {}.{}",
            if concurrently { "online (CONCURRENTLY)" } else { "offline" },
            schema_name, index_name
        ),
    );

    // check if the index is invalid before reindexing
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Validating index integrity for {}.{}",
            schema_name, index_name
        ),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Index {}.{} validity: {}",
            schema_name, index_name, index_is_valid
        ),
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
            reindex_status: crate::types::ReindexStatus::InvalidIndex,
            before_size: None,
            after_size: None,
            size_change: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    // Check bloat ratio if threshold is specified
    if let Some(threshold) = bloat_threshold {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Checking bloat ratio for {}.{} (threshold: {}%)",
                schema_name, index_name, threshold
            ),
        );
        
        let bloat_ratio = get_index_bloat_ratio(&client, &index_name).await?;
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Bloat ratio for {}.{}: {}%",
                schema_name, index_name, bloat_ratio
            ),
        );

        if bloat_ratio < threshold as f64 {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Index bloat ratio ({}%) is below threshold ({}%), skipping reindexing {}.{}",
                    bloat_ratio, threshold, schema_name, index_name
                ),
            );
            logger.log_index_skipped(
                &schema_name,
                &index_name,
                &format!("Bloat ratio ({}%) below threshold ({}%)", bloat_ratio, threshold),
            );
            // Save skipped record to logbook
            let index_data = crate::save::IndexData {
                schema_name: schema_name.clone(),
                index_name: index_name.clone(),
                index_type: index_type.clone(),
                reindex_status: crate::types::ReindexStatus::BelowBloatThreshold,
                before_size: None,
                after_size: None,
                size_change: None,
            };
            crate::save::save_index_info(&client, &index_data).await?;

            return Ok(());
        } else {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Bloat ratio ({}%) is above threshold ({}%), proceeding with checks",
                    bloat_ratio, threshold
                ),
            );
        }
    }

    // Perform fresh reindexing checks for this thread
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Performing validation checks for {}.{} before reindexing",
            schema_name, index_name
        ),
    );

    let reindexing_results = crate::checks::perform_reindexing_checks(&client).await?;
    
    // Determine specific skip reason for better debugging
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
                schema_name, index_name, skip_reason
            ),
        );
        logger.log_index_skipped(
            &schema_name,
            &index_name,
            &format!("Skipped due to: {}", skip_reason),
        );

        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: crate::types::ReindexStatus::Skipped,
            before_size: None,
            after_size: None,
            size_change: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "All pre-reindex checks passed for {}.{}",
            schema_name, index_name
        ),
    );


    check_and_handle_deadlock_risk(&client, &schema_name, &index_name, &shared_tracker, &logger)
        .await?;

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting reindex operation for {}.{}",
            schema_name, index_name
        ),
    );

    // Add random delay between 0 and 10 seconds
    logger.log(
        logging::LogLevel::Info,
        &format!("Adding random delay between 0 and 5 seconds for {}.{}", schema_name, index_name),
    );
    let artificial_delay: u32 = rand::rng().random_range(1..=5);
    thread::sleep(time::Duration::from_secs(artificial_delay as u64));

    let start_time = std::time::Instant::now();
    let result = client.execute(&reindex_sql, &[]).await;
    let duration = start_time.elapsed();

    match &result {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Reindex SQL executed successfully for {}.{} in {:?}",
                    schema_name, index_name, duration
                ),
            );
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!(
                    "Reindex SQL failed for {}.{} after {:?}: {}",
                    schema_name, index_name, duration, e
                ),
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


    thread::sleep(time::Duration::from_secs(artificial_delay as u64));
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Checking if the index is valid before saving it to logbook for {}.{}. This is the final check because the index is already reindexed.",
            schema_name, index_name
        ),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;

    if !index_is_valid {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Index validation failed for {}.{}",
                schema_name, index_name
            ),
        );
        logger.log_index_validation_failed(&schema_name, &index_name);

        // Save failed validation record
        let index_data = crate::save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: crate::types::ReindexStatus::ValidationFailed,
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
        &format!(
            "Saving success record for {}.{}",
            schema_name, index_name
        ),
    );
    let index_data = crate::save::IndexData {
        schema_name: schema_name.clone(),
        index_name: index_name.clone(),
        index_type: index_type.clone(),
        reindex_status: crate::types::ReindexStatus::Success,
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(size_change),
    };
    crate::save::save_index_info(&client, &index_data).await?;

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Successfully completed reindex for {}.{}",
            schema_name, index_name
        ),
    );
    logger.log_index_success(&schema_name, &index_name);

    // Remove table from shared tracker
    remove_table_from_tracker(&client, &schema_name, &index_name, &shared_tracker, &logger).await?;

    Ok(())
}
