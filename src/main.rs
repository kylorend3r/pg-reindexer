use crate::connection::set_session_parameters;
use crate::index_operations::get_indexes_in_schema;
use anyhow::{Context, Result};
use clap::Parser;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::{collections::HashSet, env, fs, path::Path, sync::Arc};
use tokio_postgres::{Config, NoTls, config::SslMode};

// Application constants
const MAX_MAINTENANCE_WORK_MEM_GB: u64 = 32;

mod checks;
mod connection;
mod index_operations;
mod logging;
mod memory_table;
mod queries;
mod save;
mod schema;
mod state;
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
        help = "Index type to reindex: 'btree' for regular b-tree indexes, 'constraint' for primary keys and unique constraints"
    )]
    index_type: String,

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
            file_port.is_empty() || file_port == "*" || file_port == port.to_string();
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

/// Check if an index name matches PostgreSQL's temporary concurrent reindex pattern
/// This matches names like "_ccnew", "_ccnew1", "_ccnew2", etc.
fn is_temporary_concurrent_reindex_index(index_name: &str) -> bool {
    if let Some(ccnew_pos) = index_name.find("_ccnew") {
        // Check if the part after "_ccnew" is either empty or consists only of digits
        let after_ccnew = &index_name[ccnew_pos + 6..];
        after_ccnew.is_empty() || after_ccnew.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Validate bloat threshold if provided
    if let Some(threshold) = args.reindex_only_bloated
        && threshold > 100
    {
        return Err(anyhow::anyhow!(
            "Bloat threshold ({}) must be between 0 and 100",
            threshold
        ));
    }

    // Validate maintenance work mem limit
    if args.maintenance_work_mem_gb > MAX_MAINTENANCE_WORK_MEM_GB {
        return Err(anyhow::anyhow!(
            "Maintenance work mem ({}) exceeds maximum limit of {} GB. Please reduce the value.",
            args.maintenance_work_mem_gb,
            MAX_MAINTENANCE_WORK_MEM_GB
        ));
    }

    // Validate index size limits
    if args.min_size_gb > args.max_size_gb {
        return Err(anyhow::anyhow!(
            "Minimum index size ({} GB) cannot be greater than maximum index size ({} GB). Please adjust the values.",
            args.min_size_gb,
            args.max_size_gb
        ));
    }

    // Validate index type
    if !["btree", "constraint"].contains(&args.index_type.as_str()) {
        return Err(anyhow::anyhow!(
            "Invalid index type '{}'. Must be one of: 'btree', 'constraint'",
            args.index_type
        ));
    }

    // Initialize logger with silence mode if enabled
    let logger = logging::Logger::new_with_silence(args.log_file.clone(), args.silence_mode);
    
    // Print startup message (always visible, even in silence mode)
    if args.silence_mode {
        println!("Starting PostgreSQL reindexer (silence mode enabled - logs are being written to {})", args.log_file);
    }

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
        .or_else(|| {
            let env_password = env::var("PG_PASSWORD").ok();
            // If PG_PASSWORD is set but empty, treat it as None to allow pgpass fallback
            if env_password.as_ref().is_some_and(|p| p.is_empty()) {
                None
            } else {
                env_password
            }
        })
        .or_else(|| get_password_from_pgpass(&host, port, &database, &username).unwrap_or(None));

    // Build connection string
    let mut connection_string = format!(
        "host={} port={} dbname={} user={}",
        host, port, database, username
    );

    if let Some(pwd) = password {
        connection_string.push_str(&format!(" password={}", pwd));
    }

    if args.ssl {
        logger.log(
            logging::LogLevel::Info,
            &format!("Connecting to PostgreSQL at {}:{} with SSL", host, port),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!("Connecting to PostgreSQL at {}:{}", host, port),
        );
    }

    // Connect to PostgreSQL with SSL support
    let client = if args.ssl {
        logger.log(
            logging::LogLevel::Info,
            "Creating connection to PostgreSQL",
        );
        // Parse connection string into Config
        let mut config: Config = connection_string
            .parse()
            .context("Failed to parse connection string")?;

        // Set SSL mode
        config.ssl_mode(SslMode::Require);

        let (client, connection) = {
            let mut tls_builder = TlsConnector::builder();

            // Handle custom CA certificate
            if let Some(ca_cert_path) = &args.ssl_ca_cert {
                logger.log(
                    logging::LogLevel::Info,
                    &format!("Loading CA certificate from: {}", ca_cert_path),
                );
                let ca_cert_data =
                    fs::read(ca_cert_path).context("Failed to read CA certificate file")?;
                let ca_cert = Certificate::from_pem(&ca_cert_data)
                    .context("Failed to parse CA certificate")?;
                tls_builder.add_root_certificate(ca_cert);
            }

            // Handle client certificate and key
            if let (Some(client_cert_path), Some(client_key_path)) =
                (&args.ssl_client_cert, &args.ssl_client_key)
            {
                logger.log(
                    logging::LogLevel::Info,
                    &format!("Loading client certificate from: {}", client_cert_path),
                );
                let client_cert_data =
                    fs::read(client_cert_path).context("Failed to read client certificate file")?;

                logger.log(
                    logging::LogLevel::Info,
                    &format!("Loading client key from: {}", client_key_path),
                );
                let client_key_data =
                    fs::read(client_key_path).context("Failed to read client key file")?;

                // Combine certificate and key into a single PEM for Identity
                let mut identity_data = client_cert_data.clone();
                identity_data.extend_from_slice(&client_key_data);

                let identity = Identity::from_pkcs12(&identity_data, "")
                    .or_else(|_| {
                        // Try PKCS8 format if PKCS12 fails
                        Identity::from_pkcs8(&client_cert_data, &client_key_data)
                    })
                    .context("Failed to parse client certificate and key")?;

                tls_builder.identity(identity);
            } else if args.ssl_client_cert.is_some() || args.ssl_client_key.is_some() {
                return Err(anyhow::anyhow!(
                    "Both --ssl-client-cert and --ssl-client-key must be provided together"
                ));
            }

            // Handle invalid certificate acceptance
            if args.ssl_self_signed {
                logger.log(
                    logging::LogLevel::Info,
                    "Connection configured to allow self-signed certificates",
                );
                tls_builder.danger_accept_invalid_certs(true);
            }

            let tls_connector = tls_builder
                .build()
                .context("Failed to create TLS connector")?;

            let tls = MakeTlsConnector::new(tls_connector);
            config
                .connect(tls)
                .await
                .context("ERROR: Failed to connect to PostgreSQL with SSL")?
        };

        // Spawn the connection to run in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        client
    } else {
        logger.log(
            logging::LogLevel::Info,
            "Creating connection to PostgreSQL",
        );
        // Connect without SSL
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
        client
    };

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
        logger.log(logging::LogLevel::Warning, "Temp file limit is limited at database level. This may cause reindexing to fail at some point.Ensure you have set a proper temp_file_limit in your postgresql.conf file.");
    } else {
        logger.log(
            logging::LogLevel::Success,
            "Temp file limit is not limited at database level.",
        );
    }

    // Clean orphaned _ccnew indexes if requested
    if args.clean_orphaned_indexes {
        logger.log(
            logging::LogLevel::Info,
            "Will clean orphaned _ccnew indexes during index discovery...",
        );
    }

    // Validate that the schema exists before proceeding
    logger.log(
        logging::LogLevel::Info,
        &format!("Validating schema '{}' exists", args.schema),
    );

    match schema::schema_exists(&client, &args.schema).await {
        Ok(exists) => {
            if !exists {
                return Err(anyhow::anyhow!(
                    "Schema '{}' does not exist in the database. Please verify the schema name and try again.",
                    args.schema
                ));
            }
            logger.log(
                logging::LogLevel::Success,
                &format!("Schema '{}' validation passed", args.schema),
            );
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Failed to validate schema '{}': {}",
                args.schema,
                e
            ));
        }
    }

    // Validate that the table exists if table name is provided
    if let Some(table) = &args.table {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Validating table '{}' exists in schema '{}'",
                table, args.schema
            ),
        );

        match schema::table_exists(&client, &args.schema, table).await {
            Ok(exists) => {
                if !exists {
                    return Err(anyhow::anyhow!(
                        "Table '{}' does not exist in schema '{}'. Please verify the table name and try again.",
                        table,
                        args.schema
                    ));
                }
                logger.log(
                    logging::LogLevel::Success,
                    &format!("Table '{}' validation passed", table),
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to validate table '{}' in schema '{}': {}",
                    table,
                    args.schema,
                    e
                ));
            }
        }
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("Discovering indexes in schema '{}'", args.schema),
    );

    if let Some(table) = &args.table {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Filtering for table '{}' (min size: {} GB, max size: {} GB, index type: {})",
                table, args.min_size_gb, args.max_size_gb, args.index_type
            ),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Index size range: {} GB - {} GB, index type: {}",
                args.min_size_gb, args.max_size_gb, args.index_type
            ),
        );
    }

    // Log the index size limits for clarity
    logger.log_index_size_limits(args.min_size_gb, args.max_size_gb);

    // Generate session ID and handle resume logic
    let session_id = if args.resume {
        // Check if there are pending indexes in the state table
        match state::has_pending_indexes(&client, None).await {
            Ok(has_pending) => {
                if has_pending {
                    logger.log(
                        logging::LogLevel::Info,
                        "Resume mode: Found pending indexes in state table. Discovering all indexes and merging with state.",
                    );
                    // Generate session ID for resume
                    let session_id = state::generate_session_id();
                    Some(session_id)
                } else {
                    logger.log(
                        logging::LogLevel::Info,
                        "Resume mode: No pending indexes found. Starting fresh session.",
                    );
                    // Start fresh
    let indexes = get_indexes_in_schema(
        &client,
        &args.schema,
        args.table.as_deref(),
                        args.min_size_gb,
        args.max_size_gb,
                        &args.index_type,
    )
    .await?;
                    
                    let session_id = state::generate_session_id();
                    state::initialize_state_table(&client, &indexes, &session_id).await?;
                    Some(session_id)
                }
            }
            Err(e) => {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!("Failed to check for pending indexes: {}. Starting fresh session.", e),
                );
                // Start fresh
                let indexes = get_indexes_in_schema(
                    &client,
                    &args.schema,
                    args.table.as_deref(),
                    args.min_size_gb,
                    args.max_size_gb,
                    &args.index_type,
                )
                .await?;
                
                let session_id = state::generate_session_id();
                state::initialize_state_table(&client, &indexes, &session_id).await?;
                Some(session_id)
            }
        }
    } else {
        // Normal mode - start from zero by clearing existing state for this schema
        logger.log(
            logging::LogLevel::Info,
            &format!("Starting fresh: clearing any existing state for schema '{}'", args.schema),
        );
        
        if let Err(e) = state::clear_schema_state(&client, &args.schema).await {
            logger.log(
                logging::LogLevel::Warning,
                &format!("Failed to clear existing state for schema '{}': {}", args.schema, e),
            );
        } else {
            logger.log(
                logging::LogLevel::Success,
                &format!("Cleared existing state for schema '{}'", args.schema),
            );
        }
        
        Some(state::generate_session_id())
    };

    // Get indexes - when resuming, load from state table; otherwise discover from database
    let indexes = if args.resume {
        // Resume mode: Load pending indexes from state table
        logger.log(
            logging::LogLevel::Info,
            "Resume mode: Loading pending indexes from state table...",
        );
        
        state::load_pending_indexes(&client, &args.schema, args.table.as_deref()).await?
    } else {
        // Normal mode: Discover indexes from database
        get_indexes_in_schema(
            &client,
            &args.schema,
            args.table.as_deref(),
            args.min_size_gb,
            args.max_size_gb,
            &args.index_type,
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
        logger.log(
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
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "Resume mode: No pending indexes found in state table for schema '{}' and table '{}'. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                        args.schema, table
                    ),
                );
            } else {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "Resume mode: No pending indexes found in state table for schema '{}'. All indexes may have been completed. Please start the tool without --resume flag to begin a new session.",
                        args.schema
                    ),
                );
            }
        } else {
            // Normal mode: No indexes found
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
            &format!(
                "Bloat threshold enabled: only reindexing indexes with bloat ratio >= {}%",
                threshold
            ),
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
        args.lock_timeout_seconds,
    )
    .await?;
    logger.log_session_parameters(
        args.maintenance_work_mem_gb,
        args.max_parallel_maintenance_workers,
        args.maintenance_io_concurrency,
        args.lock_timeout_seconds,
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

    // Create reindex_state table
    logger.log(
        logging::LogLevel::Info,
        "Creating reindex_state table for state tracking.",
    );
    match schema::create_reindex_state_table(&client).await {
        Ok(_) => {
            logger.log(logging::LogLevel::Success, "Reindex state table created/verified.");
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Warning,
                &format!("Failed to create reindex_state table: {}", e),
            );
        }
    }

    logger.log(
        logging::LogLevel::Success,
        "Session parameters and schema setup completed",
    );

    // Note: Per-thread checks will be performed when each thread starts
    logger.log(
        logging::LogLevel::Info,
        "Per-thread checks will be performed when each thread starts",
    );

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

    let connection_string = Arc::new(connection_string);

    // Create shared memory table for index management
    let memory_table = Arc::new(memory_table::SharedIndexMemoryTable::new());

    // Create worker tasks
    let mut tasks = Vec::new();
    let logger = Arc::new(logger);

    // Clean orphaned _ccnew indexes if requested
    if args.clean_orphaned_indexes {
        logger.log(
            logging::LogLevel::Info,
            "Cleaning orphaned _ccnew indexes...",
        );

        for index in &indexes {
            if is_temporary_concurrent_reindex_index(&index.index_name)
                && let Err(e) = index_operations::clean_orphaned_ccnew_index(
                    &client,
                    &index.schema_name,
                    &index.index_name,
                    &logger,
                )
                .await
            {
                logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "Failed to drop orphaned index {}.{}: {}",
                        index.schema_name, index.index_name, e
                    ),
                );
            }
        }
    }

    // Save excluded indexes to logbook before filtering them out
    for index in &indexes {
        if excluded_indexes.contains(&index.index_name) {
            logger.log(
                logging::LogLevel::Info,
                &format!("Excluding index from reindexing: {}", index.index_name),
            );

            // Save excluded index to logbook
            let index_data = crate::save::IndexData {
                schema_name: index.schema_name.clone(),
                index_name: index.index_name.clone(),
                index_type: args.index_type.clone(),
                reindex_status: crate::types::ReindexStatus::Excluded,
                before_size: None,
                after_size: None,
                size_change: None,
                reindex_duration: None,
            };

            if let Err(e) = crate::save::save_index_info(&client, &index_data).await {
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

    // Filter out excluded indexes and orphaned _ccnew indexes from processing
    // Note: When resuming, indexes are already filtered (only pending ones loaded from state table)
    let filtered_indexes: Vec<_> = indexes.into_iter()
        .filter(|index| {
            // Check if index is in exclude list
            if excluded_indexes.contains(&index.index_name) {
                return false;
            }

            // Check if index is a temporary concurrent reindex index
            if is_temporary_concurrent_reindex_index(&index.index_name) {
                logger.log(logging::LogLevel::Warning, &format!("Index appears to be a temporary concurrent reindex index (matches '_ccnew' pattern). Skipping reindexing: {}", index.index_name));
                return false;
            }

            true
        })
        .collect();

    // Initialize state table if not resuming (if resuming, it was already initialized)
    if !args.resume {
        if let Some(ref sid) = session_id {
            if let Err(e) = state::initialize_state_table(&client, &filtered_indexes, sid).await {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!("Failed to initialize state table: {}", e),
                );
            }
        }
    }

    // Re-initialize memory table with filtered indexes
    memory_table
        .initialize_with_indexes(filtered_indexes.clone())
        .await;

    // Create worker tasks instead of individual index tasks
    for worker_id in 0..effective_threads {
        let connection_string = connection_string.clone();
        let memory_table = memory_table.clone();
        let logger = logger.clone();
        let maintenance_work_mem_gb = args.maintenance_work_mem_gb;
        let max_parallel_maintenance_workers = args.max_parallel_maintenance_workers;
        let maintenance_io_concurrency = args.maintenance_io_concurrency;
        let lock_timeout_seconds = args.lock_timeout_seconds;
        let skip_inactive_replication_slots = args.skip_inactive_replication_slots;
        let skip_sync_replication_connection = args.skip_sync_replication_connection;
        let skip_active_vacuums = args.skip_active_vacuums;
        let bloat_threshold = args.reindex_only_bloated;
        let concurrently = args.concurrently;
        let use_ssl = args.ssl;
        let accept_invalid_certs = args.ssl_self_signed;
        let ssl_ca_cert = args.ssl_ca_cert.clone();
        let ssl_client_cert = args.ssl_client_cert.clone();
        let ssl_client_key = args.ssl_client_key.clone();
        let user_index_type = args.index_type.clone();
        let session_id_clone = session_id.clone();

        let task = tokio::spawn(async move {
            index_operations::worker_with_memory_table(
                worker_id,
                connection_string.to_string(),
                memory_table,
                logger,
                maintenance_work_mem_gb,
                max_parallel_maintenance_workers,
                maintenance_io_concurrency,
                lock_timeout_seconds,
                skip_inactive_replication_slots,
                skip_sync_replication_connection,
                skip_active_vacuums,
                bloat_threshold,
                concurrently,
                use_ssl,
                accept_invalid_certs,
                ssl_ca_cert,
                ssl_client_cert,
                ssl_client_key,
                user_index_type,
                session_id_clone,
            )
            .await
        });

        tasks.push(task);
    }

    // Wait for all worker tasks to complete and collect results
    let mut _error_count = 0;

    for (worker_id, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(_)) => {
                // Worker completed successfully
                logger.log(
                    logging::LogLevel::Info,
                    &format!("Worker {} completed successfully", worker_id),
                );
            }
            Ok(Err(e)) => {
                _error_count += 1;
                eprintln!("  ✗ Worker {} failed: {}", worker_id, e);
                logger.log(
                    logging::LogLevel::Error,
                    &format!("Worker {} failed: {}", worker_id, e),
                );
            }
            Err(e) => {
                _error_count += 1;
                eprintln!("  ✗ Worker {} panicked: {}", worker_id, e);
                logger.log(
                    logging::LogLevel::Error,
                    &format!("Worker {} panicked: {}", worker_id, e),
                );
            }
        }
    }

    // Get final statistics from memory table
    let (pending, in_progress, completed, failed, skipped) = memory_table.get_statistics().await;

    let duration = start_time.elapsed();

    // Create a new logger for the final message (with silence mode)
    let final_logger = logging::Logger::new_with_silence(args.log_file.clone(), args.silence_mode);
    let total_processed = completed + failed + skipped + pending + in_progress;
    final_logger.log_completion_message(total_processed, failed, duration, effective_threads);
    
    if !args.silence_mode {
        final_logger.log(logging::LogLevel::Success, "Reindex process completed");
    }

    Ok(())
}
