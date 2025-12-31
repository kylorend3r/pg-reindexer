use crate::connection::{create_connection_ssl, set_session_parameters, ConnectionConfig};
use crate::types::IndexFilterType;
use anyhow::{Context, Result};
use clap::Parser;
use std::{collections::HashSet, sync::Arc};

mod checks;
mod config;
mod connection;
mod credentials;
mod index_operations;
mod logging;
mod memory_table;
mod orchestrator;
mod queries;
mod save;
mod schema;
mod state;
mod types;
mod validation;

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

    /// Schema name(s) to reindex (required). Can be a single schema or comma-separated list (max 512 schemas)
    #[arg(short = 's', long, help = "Schema name(s) to reindex. Can be a single schema or comma-separated list (max 512 schemas)")]
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
        help = "Skip checking sync replication connection/slot status. If there is a sync replication instance, it will be skipped."
    )]
    skip_sync_replication_connection: bool,

    /// Skip active vacuum check
    #[arg(
        long,
        default_value = "false",
        help = "Skip checking active vacuum processes. If there is an active vacuum process, it will be skipped."
    )]
    skip_active_vacuums: bool,

    /// Maximum index size in GB (default: 1024 GB = 1TB)
    #[arg(
        short = 'm',
        long,
        default_value = "1024",
        help = "Maximum index size in GB. Indexes larger than this will be excluded from reindexing"
    )]
    max_size_gb: u64,

    /// Minimum index size in GB (default: 0 GB)
    #[arg(
        long,
        default_value = "0",
        help = "Minimum index size in GB. Indexes smaller than this will be excluded from reindexing"
    )]
    min_size_gb: u64,

    /// Index type to reindex (default: btree)
    #[arg(
        long,
        default_value = "btree",
        value_parser = clap::value_parser!(IndexFilterType),
        help = "Index type to reindex: 'btree' for regular b-tree indexes, 'constraint' for primary keys and unique constraints, 'all' for all index types"
    )]
    index_type: IndexFilterType,

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

    /// Lock timeout in seconds (default: 0 = no timeout)
    #[arg(
        long,
        default_value = "0",
        help = "Lock timeout in seconds. Set to 0 for no timeout (default). This controls how long to wait for locks before timing out."
    )]
    lock_timeout_seconds: u64,

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

    /// Clean orphaned _ccnew indexes before starting reindexing
    #[arg(
        long,
        default_value = "false",
        help = "Drop orphaned _ccnew indexes (temporary concurrent reindex indexes) before starting the reindexing process. These indexes are created by PostgreSQL during REINDEX INDEX CONCURRENTLY operations and may be left behind if the operation was interrupted."
    )]
    clean_orphaned_indexes: bool,

    /// Enable SSL connection to PostgreSQL
    #[arg(
        long,
        default_value = "false",
        help = "Enable SSL connection to PostgreSQL. When enabled, the connection will use SSL/TLS encryption."
    )]
    ssl: bool,

    /// Allow self-signed SSL certificates
    #[arg(
        long,
        default_value = "false",
        help = "Allow self-signed or invalid SSL certificates."
    )]
    ssl_self_signed: bool,

    /// Path to CA certificate file for SSL connection
    #[arg(
        long,
        help = "Path to CA certificate file (.pem) for SSL connection. If not provided, uses system default certificate store."
    )]
    ssl_ca_cert: Option<String>,

    /// Path to client certificate file for SSL connection
    #[arg(
        long,
        help = "Path to client certificate file (.pem) for SSL connection. Requires --ssl-client-key."
    )]
    ssl_client_cert: Option<String>,

    /// Path to client private key file for SSL connection
    #[arg(
        long,
        help = "Path to client private key file (.pem) for SSL connection. Requires --ssl-client-cert."
    )]
    ssl_client_key: Option<String>,

    /// Comma-separated list of index names to exclude from reindexing
    #[arg(
        long,
        help = "Comma-separated list of index names to exclude from reindexing. These indexes will be skipped even if they match other selection criteria."
    )]
    exclude_indexes: Option<String>,

    /// Resume reindexing from previous state
    #[arg(
        long,
        default_value = "false",
        help = "Resume reindexing from previous state. If enabled, the tool will load pending/failed indexes from the reindex_state table and continue processing."
    )]
    resume: bool,

    /// Silence mode - only log to file, print only startup and completion messages to terminal
    #[arg(
        long,
        default_value = "false",
        help = "Silence mode: suppresses all terminal output except startup and completion messages. All logs are still written to the log file."
    )]
    silence_mode: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Parse schemas
    let parsed_schemas: Vec<String> = args.schema
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    if parsed_schemas.is_empty() {
        return Err(anyhow::anyhow!("At least one schema must be provided"));
    }
    
    if parsed_schemas.len() > 512 {
        return Err(anyhow::anyhow!(
            "Maximum of 512 schemas allowed, but {} schemas were provided",
            parsed_schemas.len()
        ));
    }

    // Validate arguments
    validation::validate_arguments(
        args.reindex_only_bloated,
        args.maintenance_work_mem_gb,
        args.min_size_gb,
        args.max_size_gb,
    )?;

    // Index type validation is now handled by the enum's FromStr implementation

    // Initialize logger with silence mode if enabled
    let logger = logging::Logger::new_with_silence(args.log_file.clone(), args.silence_mode);
    let logger_arc = Arc::new(logger);

    // Deduplicate schemas and warn if duplicates were found
    let mut schemas: Vec<String> = Vec::new();
    let mut seen_schemas: HashSet<String> = HashSet::new();
    let mut duplicate_count = 0;
    
    for schema in parsed_schemas {
        if seen_schemas.contains(&schema) {
            duplicate_count += 1;
        } else {
            seen_schemas.insert(schema.clone());
            schemas.push(schema);
        }
    }
    
    if duplicate_count > 0 {
        logger_arc.log(
            logging::LogLevel::Warning,
            &format!(
                "Found {} duplicate schema name(s) in the provided list. Duplicates have been removed.",
                duplicate_count
            ),
        );
    }
    
    // Print startup message (always visible, even in silence mode)
    if args.silence_mode {
        println!("Starting PostgreSQL reindexer (silence mode enabled - logs are being written to {})", args.log_file);
    }

    // Build connection configuration from arguments
    let connection_config = ConnectionConfig::from_args(
        args.host.clone(),
        args.port,
        args.database.clone(),
        args.username.clone(),
        args.password.clone(),
        args.ssl,
        args.ssl_self_signed,
        args.ssl_ca_cert.clone(),
        args.ssl_client_cert.clone(),
        args.ssl_client_key.clone(),
    )?;

    let connection_string = connection_config.build_connection_string();

    if args.ssl {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Connecting to PostgreSQL at {}:{} with SSL", connection_config.host, connection_config.port),
        );
    } else {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Connecting to PostgreSQL at {}:{}", connection_config.host, connection_config.port),
        );
    }

    // Connect to PostgreSQL with SSL support
    let client = create_connection_ssl(
        &connection_string,
        connection_config.ssl,
        connection_config.ssl_self_signed,
        connection_config.ssl_ca_cert.clone(),
        connection_config.ssl_client_cert.clone(),
        connection_config.ssl_client_key.clone(),
        &logger_arc,
    )
    .await?;

    logger_arc.log(
        logging::LogLevel::Success,
        "Successfully connected to PostgreSQL",
    );

    // Validate thread count and parallel worker settings
    validation::validate_threads_and_workers(
        &client,
        &logger_arc,
        args.threads,
        args.max_parallel_maintenance_workers,
    )
    .await?;

    // Get the current temp_file_limit setting and check if it's limited or not.
    // Warn the user if it's limited.
    let temp_file_limit = client
        .query(queries::GET_TEMP_FILE_LIMIT, &[])
        .await
        .context("Failed to get temp_file_limit setting")?;
    let temp_file_limit_str: String = temp_file_limit.first().unwrap().get(0);
    let temp_file_limit: i128 = temp_file_limit_str
        .parse()
        .context("Failed to parse temp_file_limit value")?;
    if temp_file_limit != -1 {
        logger_arc.log(logging::LogLevel::Warning, "Temp file limit is limited at database level. This may cause reindexing to fail at some point.Ensure you have set a proper temp_file_limit in your postgresql.conf file.");
    } else {
        logger_arc.log(
            logging::LogLevel::Success,
            "Temp file limit is not limited at database level.",
        );
    }

    // Clean orphaned _ccnew indexes if requested
    if args.clean_orphaned_indexes {
        logger_arc.log(
            logging::LogLevel::Info,
            "Will clean orphaned _ccnew indexes during index discovery...",
        );
    }

    // Validate schemas and table existence
    validation::validate_schemas_and_table(
        &client,
        &logger_arc,
        &schemas,
        args.table.as_deref(),
    )
    .await?;

    if schemas.len() == 1 {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Discovering indexes in schema '{}'", schemas[0]),
        );
    } else {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Discovering indexes in {} schemas: {}", schemas.len(), schemas.join(", ")),
        );
    }

    if let Some(table) = &args.table {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!(
                "Filtering for table '{}' (min size: {} GB, max size: {} GB, index type: {})",
                table, args.min_size_gb, args.max_size_gb, args.index_type
            ),
        );
    } else {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!(
                "Index size range: {} GB - {} GB, index type: {}",
                args.min_size_gb, args.max_size_gb, args.index_type
            ),
        );
    }

    // Log the index size limits for clarity
    logger_arc.log_index_size_limits(args.min_size_gb, args.max_size_gb);

    // Initialize resume manager
    let resume_manager = state::ResumeManager::new(&client, logger_arc.clone());

    // Generate session ID and handle resume logic
    let session_id = resume_manager
        .initialize_session(
            args.resume,
            &schemas,
            || async {
                crate::index_operations::get_indexes_in_schemas(
                    &client,
                    &schemas,
                    args.table.as_deref(),
                    args.min_size_gb,
                    args.max_size_gb,
                    args.index_type,
                )
                .await
            },
        )
        .await?;

    // Get indexes - when resuming, load from state table; otherwise discover from database
    let indexes = resume_manager
        .load_or_discover_indexes(
            args.resume,
            &schemas,
            args.table.as_deref(),
            || async {
                crate::index_operations::get_indexes_in_schemas(
                    &client,
                    &schemas,
                    args.table.as_deref(),
                    args.min_size_gb,
                    args.max_size_gb,
                    args.index_type,
                )
                .await
            },
        )
        .await?;

    // Parse exclude-indexes if provided
    let excluded_indexes: HashSet<String> = if let Some(exclude_list) = &args.exclude_indexes {
        exclude_list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        HashSet::new()
    };

    if !excluded_indexes.is_empty() {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!(
                "Excluding {} indexes from reindexing: {}",
                excluded_indexes.len(),
                args.exclude_indexes.as_ref().unwrap()
            ),
        );
    }

    if indexes.is_empty() {
        if args.resume {
            // Resume mode: No pending indexes found
            if let Some(table) = &args.table {
                if schemas.len() == 1 {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Resume mode: No pending indexes found in state table for schema '{}' and table '{}'. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                            schemas[0], table
                        ),
                    );
                } else {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Resume mode: No pending indexes found in state table for {} schemas and table '{}'. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                            schemas.len(), table
                        ),
                    );
                }
            } else {
                if schemas.len() == 1 {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Resume mode: No pending indexes found in state table for schema '{}'. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                            schemas[0]
                        ),
                    );
                } else {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Resume mode: No pending indexes found in state table for {} schemas. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                            schemas.len()
                        ),
                    );
                }
            }
        } else {
            // Normal mode: No indexes found
            if let Some(table) = &args.table {
                if schemas.len() == 1 {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "No indexes found in schema '{}' for table '{}'",
                            schemas[0], table
                        ),
                    );
                } else {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "No indexes found in {} schemas for table '{}'",
                            schemas.len(), table
                        ),
                    );
                }
            } else {
                if schemas.len() == 1 {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!("No indexes found in schema '{}'", schemas[0]),
                    );
                } else {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!("No indexes found in {} schemas", schemas.len()),
                    );
                }
            }
        }
        return Ok(());
    }

    if let Some(table) = &args.table {
        if schemas.len() == 1 {
            logger_arc.log(
                logging::LogLevel::Info,
                &format!(
                    "Found {} indexes in schema '{}' for table '{}'",
                    indexes.len(),
                    schemas[0],
                    table
                ),
            );
        } else {
            logger_arc.log(
                logging::LogLevel::Info,
                &format!(
                    "Found {} indexes across {} schemas for table '{}'",
                    indexes.len(),
                    schemas.len(),
                    table
                ),
            );
        }
    } else {
        if schemas.len() == 1 {
            logger_arc.log(
                logging::LogLevel::Info,
                &format!(
                    "Found {} indexes in schema '{}'",
                    indexes.len(),
                    schemas[0]
                ),
            );
        } else {
            logger_arc.log(
                logging::LogLevel::Info,
                &format!(
                    "Found {} indexes across {} schemas",
                    indexes.len(),
                    schemas.len()
                ),
            );
        }
    }

    if args.dry_run {
        logger_arc.log_dry_run(&indexes);
        return Ok(());
    }

    logger_arc.log(
        logging::LogLevel::Info,
        &format!("Found {} indexes to process", indexes.len()),
    );

    if let Some(threshold) = args.reindex_only_bloated {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!(
                "Bloat threshold enabled: only reindexing indexes with bloat ratio >= {}%",
                threshold
            ),
        );
    }

    logger_arc.log(
        logging::LogLevel::Info,
        "Setting up session parameters and schema",
    );

    // Set session parameters for the main connection
    set_session_parameters(
        &client,
        args.maintenance_work_mem_gb,
        args.max_parallel_maintenance_workers,
        args.maintenance_io_concurrency,
        args.lock_timeout_seconds,
    )
    .await?;
    logger_arc.log_session_parameters(
        args.maintenance_work_mem_gb,
        args.max_parallel_maintenance_workers,
        args.maintenance_io_concurrency,
        args.lock_timeout_seconds,
    );

    logger_arc.log(
        logging::LogLevel::Info,
        "Checking if the schema exists to store the reindex information.",
    );
    match schema::create_index_info_table(&client).await {
        Ok(_) => {
            logger_arc.log(logging::LogLevel::Success, "Schema check passed.");
        }
        Err(e) => {
            logger_arc.log(
                logging::LogLevel::Warning,
                &format!("Failed to create reindex_logbook table: {}", e),
            );
        }
    }

    // Create reindex_state table
    logger_arc.log(
        logging::LogLevel::Info,
        "Creating reindex_state table for state tracking.",
    );
    match schema::create_reindex_state_table(&client).await {
        Ok(_) => {
            logger_arc.log(logging::LogLevel::Success, "Reindex state table created/verified.");
        }
        Err(e) => {
            logger_arc.log(
                logging::LogLevel::Warning,
                &format!("Failed to create reindex_state table: {}", e),
            );
        }
    }

    logger_arc.log(
        logging::LogLevel::Success,
        "Session parameters and schema setup completed",
    );

    // Note: Per-thread checks will be performed when each thread starts
    logger_arc.log(
        logging::LogLevel::Info,
        "Per-thread checks will be performed when each thread starts",
    );

    // Adjust thread count if table name is provided
    let effective_threads = if args.table.is_some() {
        logger_arc.log(
            logging::LogLevel::Info,
            "Table name provided, setting thread count to 1 to avoid conflicts",
        );
        1
    } else {
        args.threads
    };

    logger_arc.log(
        logging::LogLevel::Info,
        &format!(
            "Starting concurrent reindex process with {} threads",
            effective_threads
        ),
    );
    let start_time = std::time::Instant::now();

    let connection_string = Arc::new(connection_string);

    // Create shared memory table for index management
    let memory_table = Arc::new(memory_table::SharedIndexMemoryTable::new());

    // Create orchestrator
    let orchestrator = orchestrator::ReindexOrchestrator::new(
        client,
        logger_arc.clone(),
        connection_string.clone(),
        connection_config,
    );

    // Clean orphaned _ccnew indexes if requested
    orchestrator
        .clean_orphaned_indexes(&indexes, args.clean_orphaned_indexes)
        .await?;

    // Process excluded indexes and filter indexes
    let filtered_indexes = orchestrator
        .process_and_filter_indexes(indexes, &excluded_indexes, args.index_type)
        .await?;

    // Initialize state table if needed
    orchestrator
        .initialize_state_if_needed(args.resume, &filtered_indexes, session_id.as_deref())
        .await?;

    // Re-initialize memory table with filtered indexes
    memory_table
        .initialize_with_indexes(filtered_indexes.clone())
        .await;

    // Create worker configuration
    let worker_config = orchestrator::WorkerConfig {
        maintenance_work_mem_gb: args.maintenance_work_mem_gb,
        max_parallel_maintenance_workers: args.max_parallel_maintenance_workers,
        maintenance_io_concurrency: args.maintenance_io_concurrency,
        lock_timeout_seconds: args.lock_timeout_seconds,
        skip_inactive_replication_slots: args.skip_inactive_replication_slots,
        skip_sync_replication_connection: args.skip_sync_replication_connection,
        skip_active_vacuums: args.skip_active_vacuums,
        bloat_threshold: args.reindex_only_bloated,
        concurrently: args.concurrently,
        use_ssl: args.ssl,
        accept_invalid_certs: args.ssl_self_signed,
        ssl_ca_cert: args.ssl_ca_cert.clone(),
        ssl_client_cert: args.ssl_client_cert.clone(),
        ssl_client_key: args.ssl_client_key.clone(),
        user_index_type: args.index_type,
        session_id: session_id.clone(),
    };

    // Create and spawn worker tasks
    let tasks = orchestrator.create_worker_tasks(effective_threads, memory_table.clone(), worker_config);

    // Wait for all worker tasks to complete and collect results
    let _error_count = orchestrator.collect_worker_results(tasks).await?;

    // Get final statistics and log completion message
    orchestrator
        .finalize_and_log_completion(
            memory_table,
            start_time,
            effective_threads,
            args.log_file.clone(),
            args.silence_mode,
        )
        .await;

    Ok(())
}
