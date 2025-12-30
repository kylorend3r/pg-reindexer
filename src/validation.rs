/// Validation module for argument, thread/worker, and schema/table validation

use crate::config::{
    effective_maintenance_workers, MAX_BLOAT_THRESHOLD_PERCENTAGE, MAX_MAINTENANCE_WORK_MEM_GB,
    MAX_THREAD_COUNT, MIN_BLOAT_THRESHOLD_PERCENTAGE,
};
use crate::logging::Logger;
use crate::queries;
use crate::schema;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio_postgres::Client;

/// Validate command-line arguments
/// 
/// Validates:
/// - Bloat threshold (if provided)
/// - Maintenance work mem limit
/// - Index size limits
pub fn validate_arguments(
    reindex_only_bloated: Option<u8>,
    maintenance_work_mem_gb: u64,
    min_size_gb: u64,
    max_size_gb: u64,
) -> Result<()> {
    // Validate bloat threshold if provided
    if let Some(threshold) = reindex_only_bloated
        && threshold > MAX_BLOAT_THRESHOLD_PERCENTAGE
    {
        return Err(anyhow::anyhow!(
            "Bloat threshold ({}) must be between {} and {}",
            threshold,
            MIN_BLOAT_THRESHOLD_PERCENTAGE,
            MAX_BLOAT_THRESHOLD_PERCENTAGE
        ));
    }

    // Validate maintenance work mem limit
    if maintenance_work_mem_gb > MAX_MAINTENANCE_WORK_MEM_GB {
        return Err(anyhow::anyhow!(
            "Maintenance work mem ({}) exceeds maximum limit of {} GB. Please reduce the value.",
            maintenance_work_mem_gb,
            MAX_MAINTENANCE_WORK_MEM_GB
        ));
    }

    // Validate index size limits
    if min_size_gb > max_size_gb {
        return Err(anyhow::anyhow!(
            "Minimum index size ({} GB) cannot be greater than maximum index size ({} GB). Please adjust the values.",
            min_size_gb,
            max_size_gb
        ));
    }

    Ok(())
}

/// Validate thread count and parallel worker settings
/// 
/// Validates:
/// - Thread count against maximum limit
/// - Total workers (threads × effective_maintenance_workers) against PostgreSQL's max_parallel_workers
pub async fn validate_threads_and_workers(
    client: &Client,
    logger: &Arc<Logger>,
    threads: usize,
    max_parallel_maintenance_workers: u64,
) -> Result<()> {
    logger.log(
        crate::logging::LogLevel::Info,
        "Validating thread count and parallel worker settings...",
    );

    // Check maximum thread limit
    if threads > MAX_THREAD_COUNT {
        return Err(anyhow::anyhow!(
            "Thread count ({}) exceeds maximum limit of {}. Please reduce the number of threads.",
            threads,
            MAX_THREAD_COUNT
        ));
    }

    // Get current max_parallel_workers setting
    let rows = client
        .query(queries::GET_MAX_PARALLEL_WORKERS, &[])
        .await
        .context("Failed to get max_parallel_workers setting")?;

    if let Some(row) = rows.first() {
        let max_parallel_workers_str: String = row.get(0);
        let max_parallel_workers: u64 = max_parallel_workers_str
            .parse()
            .context("Failed to parse max_parallel_workers value")?;

        // Calculate total workers that would be used
        // When max_parallel_maintenance_workers is 0, PostgreSQL uses default (typically 2)
        let effective_maintenance_workers = effective_maintenance_workers(max_parallel_maintenance_workers);

        let total_workers = threads as u64 * effective_maintenance_workers;

        if total_workers > max_parallel_workers {
            return Err(anyhow::anyhow!(
                "Configuration would exceed PostgreSQL's max_parallel_workers limit. \
                Threads ({}) × effective_maintenance_workers ({}) = {} workers, \
                but max_parallel_workers is {}. \
                Note: When max_parallel_maintenance_workers is 0, PostgreSQL uses default of 2 workers. \
                Please reduce either --threads or --max-parallel-maintenance-workers.",
                threads,
                effective_maintenance_workers,
                total_workers,
                max_parallel_workers
            ));
        }

        logger.log(
            crate::logging::LogLevel::Success,
            &format!(
                "Validation passed: {} threads × {} workers = {} total workers (max: {})",
                threads, effective_maintenance_workers, total_workers, max_parallel_workers
            ),
        );
    } else {
        return Err(anyhow::anyhow!(
            "Failed to get max_parallel_workers setting"
        ));
    }

    Ok(())
}

/// Validate schemas and table existence
/// 
/// Validates:
/// - All schemas exist in the database
/// - Table exists in all schemas (if table name is provided)
pub async fn validate_schemas_and_table(
    client: &Client,
    logger: &Arc<Logger>,
    schema_names: &[String],
    table_name: Option<&str>,
) -> Result<()> {
    logger.log(
        crate::logging::LogLevel::Info,
        &format!("Validating {} schema(s) exist", schema_names.len()),
    );

    // Validate all schemas exist
    for schema_name in schema_names {
        match schema::schema_exists(client, schema_name).await {
            Ok(exists) => {
                if !exists {
                    return Err(anyhow::anyhow!(
                        "Schema '{}' does not exist in the database. Please verify the schema name and try again.",
                        schema_name
                    ));
                }
                logger.log(
                    crate::logging::LogLevel::Success,
                    &format!("Schema '{}' validation passed", schema_name),
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to validate schema '{}': {}",
                    schema_name,
                    e
                ));
            }
        }
    }

    // Validate that the table exists in all schemas if table name is provided
    if let Some(table) = table_name {
        logger.log(
            crate::logging::LogLevel::Info,
            &format!(
                "Validating table '{}' exists in {} schema(s)",
                table, schema_names.len()
            ),
        );

        for schema_name in schema_names {
            match schema::table_exists(client, schema_name, table).await {
                Ok(exists) => {
                    if !exists {
                        return Err(anyhow::anyhow!(
                            "Table '{}' does not exist in schema '{}'. Please verify the table name and try again.",
                            table,
                            schema_name
                        ));
                    }
                    logger.log(
                        crate::logging::LogLevel::Success,
                        &format!("Table '{}' validation passed in schema '{}'", table, schema_name),
                    );
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to validate table '{}' in schema '{}': {}",
                        table,
                        schema_name,
                        e
                    ));
                }
            }
        }
    }

    Ok(())
}

