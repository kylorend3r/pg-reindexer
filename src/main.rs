use anyhow::{Context, Result};
use clap::Parser;
use std::{env, fs, path::Path, sync::Arc};
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;
mod logging;
mod queries;
mod save;
mod schema;

#[derive(Parser, Debug)]
#[command(author, version, about = "PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table", long_about = None)]
struct Args {
    /// PostgreSQL host (can also be set via PG_HOST environment variable)
    #[arg(short = 'H', long)]
    host: Option<String>,

    /// PostgreSQL port (can also be set via PG_PORT environment variable)
    #[arg(short, long)]
    port: Option<u16>,

    /// Database name (can also be set via PG_DATABASE environment variable)
    #[arg(short, long)]
    database: Option<String>,

    /// Username (can also be set via PG_USER environment variable)
    #[arg(short = 'U', long)]
    username: Option<String>,

    /// Password (can also be set via PG_PASSWORD environment variable)
    #[arg(short = 'P', long)]
    password: Option<String>,

    /// Schema name to reindex (required)
    #[arg(short = 's', long)]
    schema: String,

    /// Table name to reindex (optional - if not provided, reindexes all indexes in schema)
    #[arg(short = 't', long)]
    table: Option<String>,

    /// Dry run - show what would be reindexed without actually doing it
    #[arg(short = 'f', long, default_value = "false")]
    dry_run: bool,

    /// Number of concurrent threads for reindexing (default: 2)
    #[arg(short = 'n', long, default_value = "2")]
    threads: usize,

    /// Verbose output
    #[arg(short = 'v', long, default_value = "false")]
    verbose: bool,

    /// Skip inactive replication slots check
    #[arg(short = 'i', long, default_value = "false")]
    skip_inactive_replication_slots: bool,

    /// Skip sync replication connection check
    #[arg(short = 'r', long, default_value = "false")]
    skip_sync_replication_connection: bool,

    /// Maximum index size in GB (default: 1024 GB = 1TB)
    #[arg(
        short = 'm',
        long,
        default_value = "1024",
        help = "Maximum index size in GB. Indexes larger than this will be excluded from reindexing"
    )]
    max_size_gb: u64,

    /// Maximum maintenance work mem in GB (default: 1 GB )
    #[arg(
        short = 'w',
        long,
        default_value = "1",
        help = "Maximum maintenance work mem in GB"
    )]
    maintenance_work_mem_gb: u64,

    /// Maximum parallel maintenance workers (default: 2)
    #[arg(
        short = 'x',
        long,
        default_value = "2",
        help = "Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety"
    )]
    max_parallel_maintenance_workers: u64,

    /// Maintenance IO concurrency (default: 10, max: 512)
    #[arg(
        short = 'c',
        long,
        default_value = "10",
        help = "Maintenance IO concurrency. Controls the number of concurrent I/O operations during maintenance operations"
    )]
    maintenance_io_concurrency: u64,

    /// Log file path (default: reindexer.log in current directory)
    #[arg(short = 'l', long, default_value = "reindexer.log")]
    log_file: String,
}

#[derive(Debug)]
struct IndexInfo {
    schema_name: String,
    index_name: String,
    index_type: String,
}

#[derive(Debug, Clone)]
struct ReindexingCheckResults {
    active_vacuum: bool,
    active_pgreindexer: bool,
    inactive_replication_slots: bool,
    sync_replication_connection: bool,
}

#[derive(Debug)]
struct SharedTableTracker {
    tables_being_reindexed: std::collections::HashMap<String, String>, // table_name -> index_name
}

// check if there is an already running pgreindexer process
async fn get_running_pgreindexer(client: &tokio_postgres::Client) -> Result<bool> {
    let rows = client
        .query(queries::GET_RUNNING_PGREINDEXER, &[])
        .await
        .context("Failed to query running pgreindexer")?;
    Ok(rows.len() > 0)
}

// check the active vacuums
async fn get_active_vacuum(client: &tokio_postgres::Client) -> Result<bool> {
    let rows = client
        .query(queries::GET_ACTIVE_VACUUM, &[])
        .await
        .context("Failed to query active vacuums")?;
    Ok(rows.len() > 0)
}

// check the inactive replication slots
async fn get_inactive_replication_slots(client: &tokio_postgres::Client) -> Result<bool> {
    let rows = client
        .query(queries::GET_INACTIVE_REPLICATION_SLOT_COUNT, &[])
        .await
        .context("Failed to query inactive replication slots")?;
    let inactive_replication_slot_count: i64 = rows.first().unwrap().get(0);
    Ok(inactive_replication_slot_count > 0)
}

// check the sync replication connection
async fn get_sync_replication_connection(client: &tokio_postgres::Client) -> Result<bool> {
    let rows = client
        .query(queries::GET_SYNC_REPLICATION_CONNECTION_COUNT, &[])
        .await
        .context("Failed to query sync replication connection")?;
    let sync_replication_connection_count: i64 = rows.first().unwrap().get(0);
    Ok(sync_replication_connection_count > 0)
}

async fn check_and_handle_deadlock_risk(
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
            "[DEBUG] Checking deadlock risk for index {}.{} on table {}",
            schema_name, index_name, full_table_name
        ),
    );

    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 12; // 1 hour max (12 * 5 minutes)

    loop {
        // Check shared tracker for currently reindexed tables
        let current_tables = {
            let tracker = shared_tracker.lock().await;
            tracker
                .tables_being_reindexed
                .keys()
                .cloned()
                .collect::<Vec<String>>()
        };

        logger.log(
            logging::LogLevel::Info,
            &format!(
                "[DEBUG] Current tables being reindexed: {:?}",
                current_tables
            ),
        );

        // Check if our table is already being reindexed
        if current_tables.contains(&full_table_name) {
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
                &format!("[DEBUG] Potential deadlock detected! Table {} is already being reindexed. Waiting 5 minutes... (retry {}/{})", 
                    full_table_name, retry_count, MAX_RETRIES),
            );

            // Wait 5 minutes
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;

            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "[DEBUG] Retrying reindex for {}.{} after 5 minute wait",
                    schema_name, index_name
                ),
            );

            // Continue the loop to check the tracker again after waiting
            continue;
        }

        // No deadlock risk, add our table to the tracker and break out of the loop
        {
            let mut tracker = shared_tracker.lock().await;
            tracker
                .tables_being_reindexed
                .insert(full_table_name.clone(), index_name.to_string());
            logger.log(
                logging::LogLevel::Info,
                &format!("[DEBUG] Added table {} to reindex tracker", full_table_name),
            );
        }
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

async fn remove_table_from_tracker(
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

// Perform all reindexing checks once and return results
async fn perform_reindexing_checks(
    client: &tokio_postgres::Client,
) -> Result<ReindexingCheckResults> {
    let active_vacuum = get_active_vacuum(client).await?;
    let active_pgreindexer = get_running_pgreindexer(client).await?;
    let inactive_replication_slots = get_inactive_replication_slots(client).await?;
    let sync_replication_connection = get_sync_replication_connection(client).await?;

    Ok(ReindexingCheckResults {
        active_vacuum,
        active_pgreindexer,
        inactive_replication_slots,
        sync_replication_connection,
    })
}

async fn get_indexes_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    max_size_gb: u64,
) -> Result<Vec<IndexInfo>> {
    let query = if let Some(_table) = table_name {
        queries::GET_INDEXES_IN_SCHEMA_WITH_TABLE
    } else {
        queries::GET_INDEXES_IN_SCHEMA
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

async fn get_index_size(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<i64> {
    let rows = client
        .query(queries::GET_INDEX_SIZE, &[&schema_name, &index_name])
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

async fn get_table_name_for_index(
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

async fn validate_index_integrity(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<bool> {
    let rows = client
        .query(
            queries::VALIDATE_INDEX_INTEGRITY,
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

async fn reindex_index_with_client(
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
        &format!(
            "[DEBUG] Starting reindex process for {}.{}",
            schema_name, index_name
        ),
    );

    // Get before size
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Getting before size for {}.{}",
            schema_name, index_name
        ),
    );
    let before_size = get_index_size(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Before size for {}.{}: {} bytes",
            schema_name, index_name, before_size
        ),
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
        &format!(
            "[DEBUG] Validating index integrity for {}.{}",
            schema_name, index_name
        ),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Index {}.{} validity: {}",
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
        let index_data = save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "invalid_index".to_string(),
            before_size: None,
            after_size: None,
            size_change: None,
        };
        save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    if reindexing_results.active_vacuum
        || reindexing_results.active_pgreindexer
        || (reindexing_results.inactive_replication_slots && !skip_inactive_replication_slots)
        || (reindexing_results.sync_replication_connection && !skip_sync_replication_connection)
    {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "[DEBUG] Skipping {}.{} due to reindexing conditions",
                schema_name, index_name
            ),
        );
        logger.log_index_skipped(
            &schema_name,
            &index_name,
            "Active vacuum, pgreindexer or inactive replication slots detected",
        );

        // Save skipped record to logbook
        let index_data = save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "skipped".to_string(),
            before_size: None,
            after_size: None,
            size_change: None,
        };
        save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Executing reindex for {}.{}",
            schema_name, index_name
        ),
    );

    // Check for potential deadlock before executing reindex
    check_and_handle_deadlock_risk(&client, &schema_name, &index_name, &shared_tracker, &logger)
        .await?;

    let start_time = std::time::Instant::now();
    let result = client.execute(&reindex_sql, &[]).await;
    let duration = start_time.elapsed();

    match &result {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "[DEBUG] Reindex SQL executed successfully for {}.{} in {:?}",
                    schema_name, index_name, duration
                ),
            );
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!(
                    "[DEBUG] Reindex SQL failed for {}.{} after {:?}: {}",
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
        &format!(
            "[DEBUG] Validating index integrity before saving for {}.{}",
            schema_name, index_name
        ),
    );
    let index_is_valid = validate_index_integrity(&client, &schema_name, &index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Final validation result for {}.{}: {}",
            schema_name, index_name, index_is_valid
        ),
    );

    if !index_is_valid {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "[DEBUG] Index validation failed for {}.{}",
                schema_name, index_name
            ),
        );
        logger.log_index_validation_failed(&schema_name, &index_name);

        // Save failed validation record
        let index_data = save::IndexData {
            schema_name: schema_name.clone(),
            index_name: index_name.clone(),
            index_type: index_type.clone(),
            reindex_status: "validation_failed".to_string(),
            before_size: Some(before_size),
            after_size: Some(after_size),
            size_change: Some(size_change),
        };
        save::save_index_info(&client, &index_data).await?;

        return Ok(());
    }

    // save the index info
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Saving success record for {}.{}",
            schema_name, index_name
        ),
    );
    let index_data = save::IndexData {
        schema_name: schema_name.clone(),
        index_name: index_name.clone(),
        index_type: index_type.clone(),
        reindex_status: "success".to_string(),
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(size_change),
    };
    save::save_index_info(&client, &index_data).await?;

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "[DEBUG] Successfully completed reindex for {}.{}",
            schema_name, index_name
        ),
    );
    logger.log_index_success(&schema_name, &index_name);

    // Remove table from shared tracker
    remove_table_from_tracker(&client, &schema_name, &index_name, &shared_tracker, &logger).await?;

    Ok(())
}

async fn set_session_parameters(
    client: &tokio_postgres::Client,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
) -> Result<()> {
    // This function can be improved to set session parameters from the cli arguments.
    // For now set the session parameters to 0.
    client
        .execute(queries::SET_STATEMENT_TIMEOUT, &[])
        .await
        .context("Set the statement timeout.")?;
    client
        .execute(queries::SET_IDLE_SESSION_TIMEOUT, &[])
        .await
        .context("Set the idle_session timeout.")?;
    client
        .execute(queries::SET_APPLICATION_NAME, &[])
        .await
        .context("Set the application name.")?;

    // The following operation defines the maintenance work mem in GB provided by the user.
    client
        .execute(
            format!(
                "SET maintenance_work_mem TO '{}GB';",
                maintenance_work_mem_gb
            )
            .as_str(),
            &[],
        )
        .await
        .context("Set the maintenance work mem.")?;

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

        // Safety check: ensure max_parallel_maintenance_workers is less than max_parallel_workers/2
        let safe_limit = max_parallel_workers / 2;
        if max_parallel_maintenance_workers >= max_parallel_workers {
            return Err(anyhow::anyhow!(
                "max_parallel_maintenance_workers ({}) must be less than max_parallel_workers ({})",
                max_parallel_maintenance_workers,
                max_parallel_workers
            ));
        }

        if max_parallel_maintenance_workers >= safe_limit {
            return Err(anyhow::anyhow!(
                "max_parallel_maintenance_workers ({}) must be less than max_parallel_workers/2 ({}) for safety",
                max_parallel_maintenance_workers,
                safe_limit
            ));
        }

        // Set max_parallel_maintenance_workers
        client
            .execute(
                format!(
                    "SET max_parallel_maintenance_workers TO '{}';",
                    max_parallel_maintenance_workers
                )
                .as_str(),
                &[],
            )
            .await
            .context("Set the max_parallel_maintenance_workers.")?;
    } else {
        return Err(anyhow::anyhow!(
            "Failed to get max_parallel_workers setting"
        ));
    }

    // Validate maintenance_io_concurrency (max 512)
    if maintenance_io_concurrency > 512 {
        return Err(anyhow::anyhow!(
            "maintenance_io_concurrency ({}) must be 512 or less",
            maintenance_io_concurrency
        ));
    }

    // Set maintenance_io_concurrency
    client
        .execute(
            format!(
                "SET maintenance_io_concurrency TO '{}';",
                maintenance_io_concurrency
            )
            .as_str(),
            &[],
        )
        .await
        .context("Set the maintenance_io_concurrency.")?;

    Ok(())
}

// Create a new database connection with session parameters set
async fn create_connection_with_session_parameters(
    connection_string: &str,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
) -> Result<tokio_postgres::Client> {
    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    // Spawn the connection to run in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Set session parameters for this connection
    set_session_parameters(
        &client,
        maintenance_work_mem_gb,
        max_parallel_maintenance_workers,
        maintenance_io_concurrency,
    )
    .await?;

    Ok(client)
}

fn get_password_from_pgpass(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
) -> Result<Option<String>> {
    // Get the path to .pgpass file
    let pgpass_path = env::var("PGPASSFILE").unwrap_or_else(|_| {
        let home = env::var("HOME").unwrap_or_else(|_| env::var("USERPROFILE").unwrap_or_default());
        format!("{}/.pgpass", home)
    });

    let pgpass_path = Path::new(&pgpass_path);

    if !pgpass_path.exists() {
        return Ok(None);
    }

    // Read the .pgpass file
    let content = fs::read_to_string(pgpass_path).context("Failed to read .pgpass file")?;

    // Parse each line
    for line in content.lines() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split by colon (format: hostname:port:database:username:password)
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() != 5 {
            continue; // Skip malformed lines
        }

        let file_host = parts[0];
        let file_port = parts[1];
        let file_database = parts[2];
        let file_username = parts[3];
        let file_password = parts[4];

        // Check if this line matches our connection parameters
        // Use wildcard matching (empty or '*' means match any)
        let host_matches = file_host.is_empty() || file_host == "*" || file_host == host;
        let port_matches =
            file_port.is_empty() || file_port == "*" || file_port == &port.to_string();
        let database_matches =
            file_database.is_empty() || file_database == "*" || file_database == database;
        let username_matches =
            file_username.is_empty() || file_username == "*" || file_username == username;

        if host_matches && port_matches && database_matches && username_matches {
            return Ok(Some(file_password.to_string()));
        }
    }

    Ok(None)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logger
    let logger = logging::Logger::new(args.log_file.clone());

    // Get connection parameters from command line arguments or environment variables
    let host = args
        .host
        .or_else(|| env::var("PG_HOST").ok())
        .unwrap_or_else(|| "localhost".to_string());

    let port = args
        .port
        .or_else(|| env::var("PG_PORT").ok().and_then(|p| p.parse().ok()))
        .unwrap_or(5432);

    let database = args
        .database
        .or_else(|| env::var("PG_DATABASE").ok())
        .unwrap_or_else(|| "postgres".to_string());

    let username = args
        .username
        .or_else(|| env::var("PG_USER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    let password = args
        .password
        .or_else(|| env::var("PG_PASSWORD").ok())
        .or_else(|| get_password_from_pgpass(&host, port, &database, &username).unwrap_or(None));

    // Build connection string
    let mut connection_string = format!(
        "host={} port={} dbname={} user={}",
        host, port, database, username
    );

    if let Some(pwd) = password {
        connection_string.push_str(&format!(" password={}", pwd));
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("Connecting to PostgreSQL at {}:{}", host, port),
    );

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .context("ERROR: Failed to connect to PostgreSQL")?;

    // Spawn the connection to run in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    logger.log(
        logging::LogLevel::Success,
        "Successfully connected to PostgreSQL",
    );

    logger.log(
        logging::LogLevel::Info,
        &format!("Discovering indexes in schema '{}'", args.schema),
    );

    if let Some(table) = &args.table {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Filtering for table '{}' (max size: {} GB)",
                table, args.max_size_gb
            ),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!("Max index size: {} GB", args.max_size_gb),
        );
    }

    // Get all indexes in the specified schema (and optionally table)
    let indexes = get_indexes_in_schema(
        &client,
        &args.schema,
        args.table.as_deref(),
        args.max_size_gb,
    )
    .await?;

    if indexes.is_empty() {
        if let Some(table) = &args.table {
            logger.log(
                logging::LogLevel::Warning,
                &format!(
                    "No indexes found in schema '{}' for table '{}'",
                    args.schema, table
                ),
            );
        } else {
            logger.log(
                logging::LogLevel::Warning,
                &format!("No indexes found in schema '{}'", args.schema),
            );
        }
        return Ok(());
    }

    if let Some(table) = &args.table {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Found {} indexes in schema '{}' for table '{}'",
                indexes.len(),
                args.schema,
                table
            ),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Found {} indexes in schema '{}'",
                indexes.len(),
                args.schema
            ),
        );
    }

    if args.dry_run {
        logger.log_dry_run(&indexes);
        return Ok(());
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("Found {} indexes to process", indexes.len()),
    );

    logger.log(
        logging::LogLevel::Info,
        "Setting up session parameters and schema",
    );

    // Set session parameters for the main connection
    set_session_parameters(
        &client,
        args.maintenance_work_mem_gb,
        args.max_parallel_maintenance_workers,
        args.maintenance_io_concurrency,
    )
    .await?;
    logger.log_session_parameters(
        args.maintenance_work_mem_gb,
        args.max_parallel_maintenance_workers,
        args.maintenance_io_concurrency,
    );

    logger.log(
        logging::LogLevel::Info,
        "Checking if the schema exists to store the reindex information.",
    );
    match schema::create_index_info_table(&client).await {
        Ok(_) => {
            logger.log(logging::LogLevel::Success, "Schema check passed.");
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Warning,
                &format!("Failed to create reindex_logbook table: {}", e),
            );
        }
    }

    logger.log(
        logging::LogLevel::Success,
        "Session parameters and schema setup completed",
    );

    // Perform reindexing checks once and share results
    logger.log(logging::LogLevel::Info, "Performing reindexing checks...");
    let reindexing_results = perform_reindexing_checks(&client).await?;
    let reindexing_results = Arc::new(reindexing_results);

    // Adjust thread count if table name is provided
    let effective_threads = if args.table.is_some() {
        logger.log(
            logging::LogLevel::Info,
            "Table name provided, setting thread count to 1 to avoid conflicts",
        );
        1
    } else {
        args.threads
    };

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting concurrent reindex process with {} threads",
            effective_threads
        ),
    );
    let start_time = std::time::Instant::now();

    // Create a semaphore to limit concurrent operations
    let semaphore = Arc::new(Semaphore::new(effective_threads));
    let connection_string = Arc::new(connection_string);

    // Create shared table tracker
    let shared_tracker = Arc::new(tokio::sync::Mutex::new(SharedTableTracker {
        tables_being_reindexed: std::collections::HashMap::new(),
    }));

    // Create tasks for all indexes
    let mut tasks = Vec::new();
    let total_indexes = indexes.len();
    let logger = Arc::new(logger);

    for (i, index) in indexes.iter().enumerate() {
        let connection_string = connection_string.clone();
        let semaphore = semaphore.clone();
        let schema_name = index.schema_name.clone();
        let index_name = index.index_name.clone();
        let index_type = index.index_type.clone();
        let verbose = args.verbose;
        let skip_inactive_replication_slots = args.skip_inactive_replication_slots;
        let skip_sync_replication_connection = args.skip_sync_replication_connection;
        let reindexing_results = reindexing_results.clone();
        let shared_tracker = shared_tracker.clone();
        let logger = logger.clone();
        let maintenance_work_mem_gb = args.maintenance_work_mem_gb;
        let max_parallel_maintenance_workers = args.max_parallel_maintenance_workers;
        let maintenance_io_concurrency = args.maintenance_io_concurrency;

        let task = tokio::spawn(async move {
            // Acquire permit from semaphore
            let _permit = semaphore.acquire().await.unwrap();

            // Create a new connection for this thread
            let client = create_connection_with_session_parameters(
                &connection_string,
                maintenance_work_mem_gb,
                max_parallel_maintenance_workers,
                maintenance_io_concurrency,
            )
            .await?;

            reindex_index_with_client(
                Arc::new(client),
                schema_name,
                index_name,
                index_type,
                i,
                total_indexes,
                verbose,
                skip_inactive_replication_slots,
                skip_sync_replication_connection,
                reindexing_results,
                shared_tracker,
                logger,
            )
            .await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete and collect results
    let mut error_count = 0;

    for task in tasks {
        match task.await {
            Ok(Ok(_)) => {
                // Task completed (could be success, skipped, or validation_failed)
            }
            Ok(Err(e)) => {
                error_count += 1;
                eprintln!("  ✗ Task failed: {}", e);
            }
            Err(e) => {
                error_count += 1;
                eprintln!("  ✗ Task panicked: {}", e);
            }
        }
    }

    let duration = start_time.elapsed();

    // Create a new logger for the final message
    let final_logger = logging::Logger::new(args.log_file.clone());
    final_logger.log_completion_message(total_indexes, error_count, duration, effective_threads);
    final_logger.log(logging::LogLevel::Success, "Reindex process completed");

    Ok(())
}
