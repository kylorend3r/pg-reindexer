
use crate::connection::{create_connection_with_session_parameters, set_session_parameters};
use crate::index_operations::{get_indexes_in_schema, reindex_index_with_client};
use crate::types::{IndexInfo, SharedTableTracker};
use anyhow::{Context, Result};
use clap::Parser;
use std::{env, fs, path::Path, sync::Arc};
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;

// Application constants
const MAX_MAINTENANCE_WORK_MEM_GB: u64 = 32;

mod checks;
mod connection;
mod deadlock;
mod index_operations;
mod logging;
mod queries;
mod save;
mod schema;
mod types;

#[derive(Parser, Debug)]
#[command(author, version, about = "PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table", long_about = None)]
struct Args {
    /// PostgreSQL host (can also be set via PG_HOST environment variable)
    #[arg(short = 'H', long, help = "Host to connect to PostgreSQL")]
    host: Option<String>,

    /// PostgreSQL port (can also be set via PG_PORT environment variable)
    #[arg(short, long, help = "Port to connect to PostgreSQL")]
    port: Option<u16>,

    /// Database name (can also be set via PG_DATABASE environment variable)
    #[arg(short, long, help = "Database name to connect to")]
    database: Option<String>,

    /// Username (can also be set via PG_USER environment variable)
    #[arg(short = 'U', long, help = "Username for the PostgreSQL user")]
    username: Option<String>,

    /// Password (can also be set via PG_PASSWORD environment variable)
    #[arg(short = 'P', long, help = "Password for the PostgreSQL user")]
    password: Option<String>,

    /// Schema name to reindex (required)
    #[arg(short = 's', long, help = "Schema name to reindex")]
    schema: String,

    /// Table name to reindex (optional - if not provided, reindexes all indexes in schema)
    #[arg(short = 't', long, help = "Table name to reindex")]
    table: Option<String>,

    /// Dry run - show what would be reindexed without actually doing it
    #[arg(
        short = 'f', 
        long, 
        default_value = "false", 
        help = "Dry run - show what would be reindexed without actually doing it"
    )]
    dry_run: bool,

    /// Number of concurrent threads for reindexing (default: 2, max: 32)
    #[arg(
        short = 'n', 
        long, 
        default_value = "2", 
        help = "Number of concurrent threads for reindexing. If set to 1, it will reindex indexes one by one to avoid conflicts."
    )]
    threads: usize,

    /// Verbose output
    #[arg(
        short = 'v', 
        long, 
        default_value = "false", 
        help = "Verbose output. If set to true, it will print more detailed information about the reindexing process."
    )]
    verbose: bool,

    /// Skip inactive replication slots check
    #[arg(
        short = 'i', 
        long, 
        default_value = "false",
        help = "Skip checking inactive replication slots(pg_replication_slots). If there is an inactive replication slot it may cause WAL files to be kept in the WAL directory or slot can miss some WAL files due to limitation if exists."
    )]
    skip_inactive_replication_slots: bool,

    /// Skip sync replication connection check
    #[arg(
        short = 'r', 
        long, 
        default_value = "false", 
        help = "Skip checking sync replication connection/slot status. If there is a sync replication instance, it will be skipped.")]
    skip_sync_replication_connection: bool,

    /// Maximum index size in GB (default: 1024 GB = 1TB)
    #[arg(
        short = 'm',
        long,
        default_value = "1024",
        help = "Maximum index size in GB. Indexes larger than this will be excluded from reindexing"
    )]
    max_size_gb: u64,

    /// Maximum maintenance work mem in GB (default: 1 GB, max: 32 GB)
    #[arg(
        short = 'w',
        long,
        default_value = "1",
        help = "Maximum maintenance work mem in GB (max: 32 GB)"
    )]
    maintenance_work_mem_gb: u64,

    /// Maximum parallel maintenance workers (default: 2, 0 = use PostgreSQL default)
    #[arg(
        short = 'x',
        long,
        default_value = "2",
        help = "Maximum parallel maintenance workers. Must be less than max_parallel_workers/2 for safety. Use 0 for PostgreSQL default (typically 2)"
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
    #[arg(
        short = 'l', 
        long, 
        default_value = "reindexer.log", 
        help = "Log file path. If not specified it will use the current directory with the name reindexer.log"
    )]
    log_file: String,

    /// Reindex only indexes with bloat ratio above this percentage (0-100)
    #[arg(
        long,
        value_name = "PERCENTAGE",
        help = "Reindex only indexes with bloat ratio above this percentage (0-100). If not specified, all indexes will be reindexed."
    )]
    reindex_only_bloated: Option<u8>,

    /// Use CONCURRENTLY for online reindexing (default: true)
    #[arg(
        long,
        default_value = "false",
        help = "Use REINDEX INDEX CONCURRENTLY for online reindexing. Set to false to use offline reindexing (REINDEX INDEX)."
    )]
    concurrently: bool,
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

    // Validate bloat threshold if provided
    if let Some(threshold) = args.reindex_only_bloated {
        if threshold > 100 {
            return Err(anyhow::anyhow!(
                "Bloat threshold ({}) must be between 0 and 100",
                threshold
            ));
        }
    }

    // Validate maintenance work mem limit
    if args.maintenance_work_mem_gb > MAX_MAINTENANCE_WORK_MEM_GB {
        return Err(anyhow::anyhow!(
            "Maintenance work mem ({}) exceeds maximum limit of {} GB. Please reduce the value.",
            args.maintenance_work_mem_gb, MAX_MAINTENANCE_WORK_MEM_GB
        ));
    }

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

    // Validate thread count and parallel worker settings
    logger.log(
        logging::LogLevel::Info,
        "Validating thread count and parallel worker settings...",
    );

    // Check maximum thread limit
    if args.threads > 32 {
        return Err(anyhow::anyhow!(
            "Thread count ({}) exceeds maximum limit of 32. Please reduce the number of threads.",
            args.threads
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
        let effective_maintenance_workers = if args.max_parallel_maintenance_workers == 0 {
            2 // Default PostgreSQL behavior
        } else {
            args.max_parallel_maintenance_workers
        };

        let total_workers = args.threads as u64 * effective_maintenance_workers;

        if total_workers > max_parallel_workers {
            return Err(anyhow::anyhow!(
                "Configuration would exceed PostgreSQL's max_parallel_workers limit. \
                Threads ({}) × effective_maintenance_workers ({}) = {} workers, \
                but max_parallel_workers is {}. \
                Note: When max_parallel_maintenance_workers is 0, PostgreSQL uses default of 2 workers. \
                Please reduce either --threads or --max-parallel-maintenance-workers.",
                args.threads,
                effective_maintenance_workers,
                total_workers,
                max_parallel_workers
            ));
        }

        logger.log(
            logging::LogLevel::Success,
            &format!(
                "Validation passed: {} threads × {} workers = {} total workers (max: {})",
                args.threads, effective_maintenance_workers, total_workers, max_parallel_workers
            ),
        );
    } else {
        return Err(anyhow::anyhow!(
            "Failed to get max_parallel_workers setting"
        ));
    }

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

    if let Some(threshold) = args.reindex_only_bloated {
        logger.log(
            logging::LogLevel::Info,
            &format!("Bloat threshold enabled: only reindexing indexes with bloat ratio >= {}%", threshold),
        );
    }

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

    // Note: Per-thread checks will be performed when each thread starts
    logger.log(logging::LogLevel::Info, "Per-thread checks will be performed when each thread starts");

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
    let shared_tracker = Arc::new(tokio::sync::Mutex::new(SharedTableTracker::new()));

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
        let shared_tracker = shared_tracker.clone();
        let logger = logger.clone();
        let maintenance_work_mem_gb = args.maintenance_work_mem_gb;
        let max_parallel_maintenance_workers = args.max_parallel_maintenance_workers;
        let maintenance_io_concurrency = args.maintenance_io_concurrency;
        let bloat_threshold = args.reindex_only_bloated;
        let concurrently = args.concurrently;

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
                shared_tracker,
                logger,
                bloat_threshold,
                concurrently,
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
