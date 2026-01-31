use pg_reindexer::connection::{create_connection_ssl, set_session_parameters, ConnectionConfig};
use pg_reindexer::types::{IndexFilterType, LogFormat};
use pg_reindexer::{logging, memory_table, orchestrator, queries, schema, state, validation};
use anyhow::{Context, Result};
use clap::Parser;
use std::{collections::HashSet, sync::Arc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::signal;

#[derive(Parser, Debug)]
#[command(author, version, about = "PostgreSQL Index Reindexer - Reindexes all indexes in a specific schema or table", long_about = None)]
struct Args {
    /// PostgreSQL host (can also be set via PG_HOST environment variable)
    #[arg(short = 'H', long, help = "Host to connect to PostgreSQL")]
    host: Option<String>,

    /// PostgreSQL port (can also be set via PG_PORT environment variable)
    #[arg(short, long, help = "Port to connect to PostgreSQL")]
    port: Option<u16>,

    /// Database name(s) to connect to. Can be a single database or comma-separated list of databases (can also be set via PG_DATABASE environment variable)
    #[arg(short, long, help = "Database name(s) to connect to. Can be a single database or comma-separated list of databases")]
    database: Option<String>,

    /// Username (can also be set via PG_USER environment variable)
    #[arg(short = 'U', long, help = "Username for the PostgreSQL user")]
    username: Option<String>,

    /// Password (can also be set via PG_PASSWORD environment variable)
    #[arg(short = 'P', long, help = "Password for the PostgreSQL user")]
    password: Option<String>,

    /// Schema name(s) to reindex. Can be a single schema or comma-separated list (max 512 schemas). Not required if --discover-all-schemas is used.
    #[arg(short = 's', long, help = "Schema name(s) to reindex. Can be a single schema or comma-separated list (max 512 schemas). Not required if --discover-all-schemas is used.")]
    schema: Option<String>,

    /// Discover all schemas in the database and collect indexes for all discovered schemas
    #[arg(
        long,
        default_value = "false",
        help = "Discover all user schemas in the database and collect indexes for all discovered schemas. System schemas (pg_catalog, information_schema, etc.) are excluded."
    )]
    discover_all_schemas: bool,

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

    /// Log format: 'text' for human-readable output, 'json' for structured JSON output
    #[arg(
        long,
        default_value = "text",
        value_parser = clap::value_parser!(LogFormat),
        help = "Log format: 'text' for human-readable output (default), 'json' for structured JSON output suitable for log aggregation systems"
    )]
    log_format: LogFormat,

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

    /// Order indexes by size: 'asc' for smallest first, 'desc' for largest first. If not specified, indexes are ordered ascending by default.
    #[arg(
        long,
        value_name = "ORDER",
        help = "Order indexes by size: 'asc' for smallest first, 'desc' for largest first. If not specified, indexes are ordered ascending by default."
    )]
    order_by_size: Option<String>,

    /// Ask for final confirmation with index count summary before proceeding
    #[arg(
        long,
        default_value = "false",
        help = "Ask for final confirmation with a summary of indexes (count) before proceeding with reindexing"
    )]
    ask_confirmation: bool,

    /// Include indexes from partitioned table partitions
    #[arg(
        long,
        default_value = "false",
        help = "Include indexes from partitioned table partitions. When enabled, indexes from all partitions of partitioned tables will be collected and reindexed."
    )]
    include_partitions: bool,

    /// Path to configuration file (TOML format). Configuration file values are overridden by CLI arguments.
    #[arg(
        short = 'C',
        long,
        value_name = "FILE",
        help = "Path to TOML configuration file. Configuration file values are overridden by CLI arguments."
    )]
    config: Option<String>,
}

/// Configuration structure for file-based configuration
/// All fields are optional to allow partial configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    schema: Option<String>,
    discover_all_schemas: Option<bool>,
    table: Option<String>,
    dry_run: Option<bool>,
    threads: Option<usize>,
    skip_inactive_replication_slots: Option<bool>,
    skip_sync_replication_connection: Option<bool>,
    skip_active_vacuums: Option<bool>,
    max_size_gb: Option<u64>,
    min_size_gb: Option<u64>,
    index_type: Option<String>,
    maintenance_work_mem_gb: Option<u64>,
    max_parallel_maintenance_workers: Option<u64>,
    maintenance_io_concurrency: Option<u64>,
    lock_timeout_seconds: Option<u64>,
    log_file: Option<String>,
    log_format: Option<String>,
    reindex_only_bloated: Option<u8>,
    concurrently: Option<bool>,
    clean_orphaned_indexes: Option<bool>,
    ssl: Option<bool>,
    ssl_self_signed: Option<bool>,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    exclude_indexes: Option<String>,
    resume: Option<bool>,
    silence_mode: Option<bool>,
    order_by_size: Option<String>,
    ask_confirmation: Option<bool>,
    include_partitions: Option<bool>,
}

/// Load configuration from a TOML file
fn load_config_file(path: &str) -> Result<Config> {
    let file_path = Path::new(path);
    
    // Check if file exists
    if !file_path.exists() {
        return Err(anyhow::anyhow!("Configuration file not found: {}", path));
    }

    // Read and parse TOML content
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read configuration file: {}", path))?;

    let config = toml::from_str::<Config>(&content)
        .with_context(|| format!("Failed to parse TOML configuration file: {}", path))?;

    Ok(config)
}

/// Merge configuration file values with CLI arguments
/// CLI arguments take precedence over config file values
fn merge_config(config_file: Config, mut args: Args) -> Args {
    // Connection settings
    if args.host.is_none() {
        args.host = config_file.host;
    }
    if args.port.is_none() {
        args.port = config_file.port;
    }
    if args.database.is_none() {
        args.database = config_file.database;
    }
    if args.username.is_none() {
        args.username = config_file.username;
    }
    if args.password.is_none() {
        args.password = config_file.password;
    }

    // Schema settings
    if args.schema.is_none() {
        args.schema = config_file.schema;
    }
    if let Some(discover) = config_file.discover_all_schemas {
        // Use config value only if CLI didn't explicitly override (when both are provided, CLI takes precedence)
        // Since we can't detect CLI explicit override perfectly, we check if config has a different value
        if args.discover_all_schemas != discover {
            args.discover_all_schemas = discover;
        }
    }
    if args.table.is_none() {
        args.table = config_file.table;
    }

    // Operation settings
    if let Some(dry_run) = config_file.dry_run {
        if args.dry_run != dry_run {
            args.dry_run = dry_run;
        }
    }
    if args.threads == 2 {
        // Only use config if CLI didn't override (default is 2)
        if let Some(threads) = config_file.threads {
            args.threads = threads;
        }
    }

    // Skip flags
    if let Some(skip_slots) = config_file.skip_inactive_replication_slots {
        if args.skip_inactive_replication_slots != skip_slots {
            args.skip_inactive_replication_slots = skip_slots;
        }
    }
    if let Some(skip_sync) = config_file.skip_sync_replication_connection {
        if args.skip_sync_replication_connection != skip_sync {
            args.skip_sync_replication_connection = skip_sync;
        }
    }
    if let Some(skip_vacuums) = config_file.skip_active_vacuums {
        if args.skip_active_vacuums != skip_vacuums {
            args.skip_active_vacuums = skip_vacuums;
        }
    }

    // Size settings
    if args.max_size_gb == 1024 {
        // Only use config if CLI didn't override (default is 1024)
        if let Some(max_size) = config_file.max_size_gb {
            args.max_size_gb = max_size;
        }
    }
    if args.min_size_gb == 0 {
        // Only use config if CLI didn't override (default is 0)
        if let Some(min_size) = config_file.min_size_gb {
            args.min_size_gb = min_size;
        }
    }

    // Index type
    if args.index_type == IndexFilterType::Btree {
        // Only use config if CLI didn't override (default is Btree)
        if let Some(ref index_type_str) = config_file.index_type {
            if let Ok(index_type) = index_type_str.parse::<IndexFilterType>() {
                args.index_type = index_type;
            }
        }
    }

    // Maintenance settings
    if args.maintenance_work_mem_gb == 1 {
        // Only use config if CLI didn't override (default is 1)
        if let Some(mem_gb) = config_file.maintenance_work_mem_gb {
            args.maintenance_work_mem_gb = mem_gb;
        }
    }
    if args.max_parallel_maintenance_workers == 2 {
        // Only use config if CLI didn't override (default is 2)
        if let Some(workers) = config_file.max_parallel_maintenance_workers {
            args.max_parallel_maintenance_workers = workers;
        }
    }
    if args.maintenance_io_concurrency == 10 {
        // Only use config if CLI didn't override (default is 10)
        if let Some(io_concurrency) = config_file.maintenance_io_concurrency {
            args.maintenance_io_concurrency = io_concurrency;
        }
    }
    if args.lock_timeout_seconds == 0 {
        // Only use config if CLI didn't override (default is 0)
        if let Some(timeout) = config_file.lock_timeout_seconds {
            args.lock_timeout_seconds = timeout;
        }
    }

    // Log file
    if args.log_file == "reindexer.log" {
        // Only use config if CLI didn't override (default is "reindexer.log")
        if let Some(log_file) = config_file.log_file {
            args.log_file = log_file;
        }
    }

    // Log format
    if args.log_format == LogFormat::Text {
        // Only use config if CLI didn't override (default is Text)
        if let Some(ref log_format_str) = config_file.log_format {
            if let Ok(log_format) = log_format_str.parse::<LogFormat>() {
                args.log_format = log_format;
            }
        }
    }

    // Optional settings
    if args.reindex_only_bloated.is_none() {
        args.reindex_only_bloated = config_file.reindex_only_bloated;
    }
    if let Some(concurrent) = config_file.concurrently {
        if args.concurrently != concurrent {
            args.concurrently = concurrent;
        }
    }
    if let Some(clean) = config_file.clean_orphaned_indexes {
        if args.clean_orphaned_indexes != clean {
            args.clean_orphaned_indexes = clean;
        }
    }

    // SSL settings
    if let Some(ssl_enabled) = config_file.ssl {
        if args.ssl != ssl_enabled {
            args.ssl = ssl_enabled;
        }
    }
    if let Some(ssl_self) = config_file.ssl_self_signed {
        if args.ssl_self_signed != ssl_self {
            args.ssl_self_signed = ssl_self;
        }
    }
    if args.ssl_ca_cert.is_none() {
        args.ssl_ca_cert = config_file.ssl_ca_cert;
    }
    if args.ssl_client_cert.is_none() {
        args.ssl_client_cert = config_file.ssl_client_cert;
    }
    if args.ssl_client_key.is_none() {
        args.ssl_client_key = config_file.ssl_client_key;
    }

    // Other settings
    if args.exclude_indexes.is_none() {
        args.exclude_indexes = config_file.exclude_indexes;
    }
    if let Some(resume) = config_file.resume {
        if args.resume != resume {
            args.resume = resume;
        }
    }
    if let Some(silence) = config_file.silence_mode {
        if args.silence_mode != silence {
            args.silence_mode = silence;
        }
    }
    if args.order_by_size.is_none() {
        args.order_by_size = config_file.order_by_size;
    }
    if let Some(ask_conf) = config_file.ask_confirmation {
        if args.ask_confirmation != ask_conf {
            args.ask_confirmation = ask_conf;
        }
    }
    if let Some(include_parts) = config_file.include_partitions {
        if args.include_partitions != include_parts {
            args.include_partitions = include_parts;
        }
    }

    args
}

/// Ask user for confirmation before proceeding with reindexing
/// Returns Ok(true) if user confirms, Ok(false) if user declines
fn ask_user_confirmation(
    index_count: usize,
    schema_count: usize,
    table_name: Option<&str>,
) -> Result<bool> {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  REINDEXING CONFIRMATION");
    println!("═══════════════════════════════════════════════════════════");
    
    if let Some(table) = table_name {
        if schema_count == 1 {
            println!("  Schema: 1");
        } else {
            println!("  Schemas: {}", schema_count);
        }
        println!("  Table: {}", table);
    } else {
        if schema_count == 1 {
            println!("  Schema: 1");
        } else {
            println!("  Schemas: {}", schema_count);
        }
    }
    println!("  Indexes to reindex: {}", index_count);
    println!("═══════════════════════════════════════════════════════════");
    print!("  Proceed with reindexing? [y/N]: ");
    
    // Flush stdout to ensure the prompt is displayed immediately
    use std::io::Write;
    std::io::stdout().flush().context("Failed to flush stdout")?;
    
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .context("Failed to read user input")?;
    
    let trimmed = input.trim().to_lowercase();
    Ok(trimmed == "y" || trimmed == "yes")
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    // Load and merge configuration file if provided
    if let Some(config_path) = &args.config {
        let config = load_config_file(config_path)
            .context("Failed to load configuration file")?;
        args = merge_config(config, args);
    }

    // Validate that either schema is provided or discover_all_schemas is enabled
    if args.schema.is_none() && !args.discover_all_schemas {
        return Err(anyhow::anyhow!(
            "Either --schema must be provided or --discover-all-schemas must be enabled"
        ));
    }

    // If both are provided, schema takes precedence (but warn the user)
    if args.schema.is_some() && args.discover_all_schemas {
        eprintln!("Warning: Both --schema and --discover-all-schemas are provided. --schema will be used and --discover-all-schemas will be ignored.");
    }

    // Parse databases - support comma-separated list
    let databases: Vec<String> = if let Some(db_str) = &args.database {
        db_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        // Fall back to environment variable or default
        let default_db = ConnectionConfig::from_args(
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
        )?
        .database;
        vec![default_db]
    };

    if databases.is_empty() {
        return Err(anyhow::anyhow!(
            "At least one database must be provided via --database argument or PG_DATABASE environment variable"
        ));
    }

    // Parse schemas - will be discovered later if discover_all_schemas is enabled
    let parsed_schemas: Vec<String> = if let Some(schema_str) = &args.schema {
        schema_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };
    
    if !parsed_schemas.is_empty() && parsed_schemas.len() > 512 {
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

    // Initialize logger with silence mode and log format
    let logger = logging::Logger::new(args.log_file.clone(), args.silence_mode, args.log_format);
    let logger_arc = Arc::new(logger);

    // Print startup message (always visible, even in silence mode)
    if args.silence_mode {
        println!("Starting PostgreSQL reindexer (silence mode enabled - logs are being written to {})", args.log_file);
    }

    // Process each database
    let total_databases = databases.len();
    for (db_index, database_name) in databases.iter().enumerate() {
        if total_databases > 1 {
            logger_arc.log(
                logging::LogLevel::Info,
                &format!(
                    "Processing database {}/{}: {}",
                    db_index + 1,
                    total_databases,
                    database_name
                ),
            );
        }

        // Process this database
        if let Err(e) = process_database(
            database_name.clone(),
            &args,
            &parsed_schemas,
            logger_arc.clone(),
        )
        .await
        {
            logger_arc.log(
                logging::LogLevel::Error,
                &format!("Failed to process database '{}': {}", database_name, e),
            );
            // Continue with next database instead of failing completely
            if total_databases > 1 {
                logger_arc.log(
                    logging::LogLevel::Warning,
                    &format!("Continuing with next database..."),
                );
                continue;
            } else {
                return Err(e);
            }
        }

        if total_databases > 1 {
            logger_arc.log(
                logging::LogLevel::Success,
                &format!("Completed processing database: {}", database_name),
            );
        }
    }

    if total_databases > 1 {
        logger_arc.log(
            logging::LogLevel::Success,
            &format!("Completed processing all {} database(s)", total_databases),
        );
    }

    Ok(())
}

/// Process reindexing for a single database
async fn process_database(
    database_name: String,
    args: &Args,
    parsed_schemas: &[String],
    logger_arc: Arc<logging::Logger>,
) -> Result<()> {
    // Validate and normalize order_by_size argument
    let order_by_size: Option<String> = if let Some(ref order) = args.order_by_size {
        let normalized = order.to_lowercase();
        match normalized.as_str() {
            "asc" | "desc" => Some(normalized),
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid value for --order-by-size: '{}'. Must be 'asc' or 'desc'",
                    order
                ));
            }
        }
    } else {
        None
    };
    let order_by_size_str = order_by_size.as_deref();

    // Build connection configuration for this specific database
    let connection_config = ConnectionConfig::from_args(
        args.host.clone(),
        args.port,
        Some(database_name.clone()),
        args.username.clone(),
        args.password.clone(),
        args.ssl,
        args.ssl_self_signed,
        args.ssl_ca_cert.clone(),
        args.ssl_client_cert.clone(),
        args.ssl_client_key.clone(),
    )?;

    let connection_string = connection_config.build_connection_string();

    // Discover schemas if discover_all_schemas is enabled
    let schemas: Vec<String> = if args.discover_all_schemas && args.schema.is_none() {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Discovering all user schemas in database '{}'...", database_name),
        );

        // Connect to PostgreSQL for schema discovery
        let discovery_client = create_connection_ssl(
            &connection_string,
            connection_config.ssl,
            connection_config.ssl_self_signed,
            connection_config.ssl_ca_cert.clone(),
            connection_config.ssl_client_cert.clone(),
            connection_config.ssl_client_key.clone(),
            &logger_arc,
        )
        .await?;

        let discovered_schemas = schema::discover_all_user_schemas(&discovery_client)
            .await
            .context("Failed to discover schemas")?;

        if discovered_schemas.is_empty() {
            return Err(anyhow::anyhow!(
                "No user schemas found in database '{}'. System schemas are excluded.",
                database_name
            ));
        }

        logger_arc.log(
            logging::LogLevel::Success,
            &format!(
                "Discovered {} schema(s) in database '{}': {}",
                discovered_schemas.len(),
                database_name,
                discovered_schemas.join(", ")
            ),
        );

        discovered_schemas
    } else {
        // Use provided schemas
        parsed_schemas.to_vec()
    };

    // Deduplicate schemas and warn if duplicates were found
    let mut seen_schemas: HashSet<String> = HashSet::new();
    let mut duplicate_count = 0;
    let mut deduplicated_schemas: Vec<String> = Vec::new();
    
    for schema in schemas {
        if seen_schemas.contains(&schema) {
            duplicate_count += 1;
        } else {
            seen_schemas.insert(schema.clone());
            deduplicated_schemas.push(schema);
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

    let schemas = deduplicated_schemas;

    // Connect to PostgreSQL with SSL support for main work
    // (discovery_client from schema discovery is dropped here)
    if args.ssl {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Connecting to database '{}' at {}:{} with SSL", database_name, connection_config.host, connection_config.port),
        );
    } else {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Connecting to database '{}' at {}:{}", database_name, connection_config.host, connection_config.port),
        );
    }

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
        &format!("Successfully connected to database '{}'", database_name),
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

    // Log ordering preference if specified
    if let Some(order) = order_by_size_str {
        logger_arc.log(
            logging::LogLevel::Info,
            &format!("Indexes will be ordered by size: {} ({} first)", order, if order == "asc" { "smallest" } else { "largest" }),
        );
    }

    // Initialize resume manager
    let resume_manager = state::ResumeManager::new(&client, logger_arc.clone());

    // Generate session ID and handle resume logic
    let session_id = if args.include_partitions {
        resume_manager
            .initialize_session(
                args.resume,
                &schemas,
                || async {
                    pg_reindexer::index_operations::get_indexes_in_schemas_with_partitions(
                        &client,
                        &schemas,
                        args.table.as_deref(),
                        args.min_size_gb,
                        args.max_size_gb,
                        args.index_type,
                        order_by_size_str,
                        true,
                        &logger_arc,
                    )
                    .await
                },
            )
            .await?
    } else {
        resume_manager
            .initialize_session(
                args.resume,
                &schemas,
                || async {
                    pg_reindexer::index_operations::get_indexes_in_schemas(
                        &client,
                        &schemas,
                        args.table.as_deref(),
                        args.min_size_gb,
                        args.max_size_gb,
                        args.index_type,
                        order_by_size_str,
                    )
                    .await
                },
            )
            .await?
    };

    // Get indexes - when resuming, load from state table; otherwise discover from database
    let indexes = if args.include_partitions {
        resume_manager
            .load_or_discover_indexes(
                args.resume,
                &schemas,
                args.table.as_deref(),
                || async {
                    pg_reindexer::index_operations::get_indexes_in_schemas_with_partitions(
                        &client,
                        &schemas,
                        args.table.as_deref(),
                        args.min_size_gb,
                        args.max_size_gb,
                        args.index_type,
                        order_by_size_str,
                        true,
                        &logger_arc,
                    )
                    .await
                },
            )
            .await?
    } else {
        resume_manager
            .load_or_discover_indexes(
                args.resume,
                &schemas,
                args.table.as_deref(),
                || async {
                    pg_reindexer::index_operations::get_indexes_in_schemas(
                        &client,
                        &schemas,
                        args.table.as_deref(),
                        args.min_size_gb,
                        args.max_size_gb,
                        args.index_type,
                        order_by_size_str,
                    )
                    .await
                },
            )
            .await?
    };

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
    let connection_string_for_cleanup = connection_string.clone();
    let connection_config_for_cleanup = connection_config.clone();

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

    // Ask for confirmation if requested (skip for dry_run and silence_mode)
    if args.ask_confirmation && !args.dry_run && !args.silence_mode {
        let confirmed = ask_user_confirmation(
            filtered_indexes.len(),
            schemas.len(),
            args.table.as_deref(),
        )?;
        
        if !confirmed {
            logger_arc.log(
                logging::LogLevel::Info,
                "Reindexing cancelled by user confirmation",
            );
            if !args.silence_mode {
                println!("Reindexing cancelled by user.");
            }
            return Ok(());
        }
        
        logger_arc.log(
            logging::LogLevel::Info,
            "User confirmed proceeding with reindexing",
        );
    }

    // Initialize state table if needed
    orchestrator
        .initialize_state_if_needed(args.resume, &filtered_indexes, session_id.as_deref())
        .await?;

    // Re-initialize memory table with filtered indexes
    memory_table
        .initialize_with_indexes(filtered_indexes.clone())
        .await;

    // Create cancellation token for graceful shutdown
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    // Spawn signal handler task for SIGINT (Ctrl+C), SIGTERM, and SIGHUP
    let cancel_tx_signal = cancel_tx.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to listen for SIGTERM signal: {}", e);
                    // Fall back to just SIGINT
                    if let Err(e) = signal::ctrl_c().await {
                        eprintln!("Failed to listen for Ctrl+C signal: {}", e);
                    } else {
                        let _ = cancel_tx_signal.send(true);
                    }
                    return;
                }
            };

            // Try to set up SIGHUP handler (terminal close), but continue if it fails
            let mut sighup_opt = signal(SignalKind::hangup()).ok();

            if let Some(ref mut sighup) = sighup_opt {
                tokio::select! {
                    result = signal::ctrl_c() => {
                        if let Err(e) = result {
                            eprintln!("Failed to listen for Ctrl+C signal: {}", e);
                            return;
                        }
                        // SIGINT received, set cancellation flag
                        let _ = cancel_tx_signal.send(true);
                    }
                    result = sigterm.recv() => {
                        match result {
                            Some(_) => {
                                // SIGTERM received, set cancellation flag
                                let _ = cancel_tx_signal.send(true);
                            }
                            None => {
                                eprintln!("SIGTERM signal stream closed");
                            }
                        }
                    }
                    result = sighup.recv() => {
                        match result {
                            Some(_) => {
                                // SIGHUP received (terminal closed), set cancellation flag
                                let _ = cancel_tx_signal.send(true);
                            }
                            None => {
                                // SIGHUP stream closed, ignore
                            }
                        }
                    }
                }
            } else {
                // SIGHUP not available, use only SIGINT and SIGTERM
                tokio::select! {
                    result = signal::ctrl_c() => {
                        if let Err(e) = result {
                            eprintln!("Failed to listen for Ctrl+C signal: {}", e);
                            return;
                        }
                        // SIGINT received, set cancellation flag
                        let _ = cancel_tx_signal.send(true);
                    }
                    result = sigterm.recv() => {
                        match result {
                            Some(_) => {
                                // SIGTERM received, set cancellation flag
                                let _ = cancel_tx_signal.send(true);
                            }
                            None => {
                                eprintln!("SIGTERM signal stream closed");
                            }
                        }
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            // On non-Unix systems, only handle Ctrl+C
            if let Err(e) = signal::ctrl_c().await {
                eprintln!("Failed to listen for Ctrl+C signal: {}", e);
                return;
            }
            // Signal received, set cancellation flag
            let _ = cancel_tx_signal.send(true);
        }
    });

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
    let tasks = orchestrator.create_worker_tasks(
        effective_threads,
        memory_table.clone(),
        worker_config,
        cancel_rx.clone(),
    );

    // Wait for all worker tasks to complete and collect results
    let (error_count, was_cancelled) = orchestrator
        .collect_worker_results(tasks, &cancel_rx)
        .await?;

    // Handle cancellation cleanup
    if was_cancelled || *cancel_rx.borrow() {
        logger_arc.log(
            logging::LogLevel::Warning,
            "Cancellation detected. Cleaning up state...",
        );

        // Reset in_progress indexes back to pending to allow resume
        if let Some(ref sid) = session_id {
            // Create a new connection for cleanup operations
            let cleanup_client = create_connection_ssl(
                &connection_string_for_cleanup,
                connection_config_for_cleanup.ssl,
                connection_config_for_cleanup.ssl_self_signed,
                connection_config_for_cleanup.ssl_ca_cert.clone(),
                connection_config_for_cleanup.ssl_client_cert.clone(),
                connection_config_for_cleanup.ssl_client_key.clone(),
                &logger_arc,
            )
            .await;

            match cleanup_client {
                Ok(client) => {
                    match state::reset_in_progress_to_pending(&client, Some(sid)).await {
                        Ok(count) => {
                            if count > 0 {
                                logger_arc.log(
                                    logging::LogLevel::Info,
                                    &format!(
                                        "Reset {} in-progress index(es) back to pending state. You can resume with --resume flag.",
                                        count
                                    ),
                                );
                            }
                        }
                        Err(e) => {
                            logger_arc.log(
                                logging::LogLevel::Warning,
                                &format!("Failed to reset in-progress indexes: {}", e),
                            );
                        }
                    }
                }
                Err(e) => {
                    logger_arc.log(
                        logging::LogLevel::Warning,
                        &format!("Failed to create connection for cleanup: {}", e),
                    );
                }
            }
        }

        if !args.silence_mode {
            println!("\n⚠️  Reindexing cancelled. Progress has been saved. Use --resume flag to continue.");
        }
        logger_arc.log(
            logging::LogLevel::Warning,
            "Reindexing cancelled by user. Use --resume flag to continue from where it left off.",
        );
    }

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

    // Return error if there were worker errors and not cancelled
    if error_count > 0 && !was_cancelled && !*cancel_rx.borrow() {
        return Err(anyhow::anyhow!(
            "Reindexing completed with {} worker error(s)",
            error_count
        ));
    }

    Ok(())
}
