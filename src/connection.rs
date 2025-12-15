use crate::config::{
    MILLISECONDS_PER_SECOND, DEFAULT_DEADLOCK_TIMEOUT, MAX_MAINTENANCE_IO_CONCURRENCY,
    PARALLEL_WORKERS_SAFETY_DIVISOR,
};
use anyhow::{Context, Result};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use tokio_postgres::{Config, NoTls, config::SslMode};

pub async fn set_session_parameters(
    client: &tokio_postgres::Client,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
) -> Result<()> {
    // This function can be improved to set session parameters from the cli arguments.
    // For now set the session parameters to 0.
    client
        .execute(crate::queries::SET_STATEMENT_TIMEOUT, &[])
        .await
        .context("Set the statement timeout.")?;
    client
        .execute(crate::queries::SET_IDLE_SESSION_TIMEOUT, &[])
        .await
        .context("Set the idle_session timeout.")?;
    client
        .execute(crate::queries::SET_APPLICATION_NAME, &[])
        .await
        .context("Set the application name.")?;
    client
        .execute(crate::queries::SET_LOG_STATEMENTS, &[])
        .await
        .context("Set log_statement to 'all'.")?;

    // Set lock_timeout
    // PostgreSQL SET command doesn't support parameterized queries with type casting
    // Since lock_timeout_seconds is a u64, it's safe to format directly
    let lock_timeout_sql = if lock_timeout_seconds == 0 {
        "SET lock_timeout TO '0'".to_string()
    } else {
        let lock_timeout_ms = lock_timeout_seconds * MILLISECONDS_PER_SECOND;
        format!("SET lock_timeout TO '{}ms'", lock_timeout_ms)
    };
    client
        .execute(&lock_timeout_sql, &[])
        .await
        .context("Set the lock_timeout.")?;

    // Set deadlock_timeout
    // PostgreSQL SET command doesn't support parameterized queries
    // Since DEFAULT_DEADLOCK_TIMEOUT is a constant string, it's safe to format directly
    let deadlock_timeout_sql = format!("SET deadlock_timeout TO '{}'", DEFAULT_DEADLOCK_TIMEOUT);
    client
        .execute(&deadlock_timeout_sql, &[])
        .await
        .context("Set the deadlock_timeout.")?;

    // Set maintenance_work_mem
    // PostgreSQL SET command doesn't support parameterized queries
    // Since maintenance_work_mem_gb is a u64, it's safe to format directly
    let maintenance_work_mem_sql = format!("SET maintenance_work_mem TO '{}GB'", maintenance_work_mem_gb);
    client
        .execute(&maintenance_work_mem_sql, &[])
        .await
        .context("Set the maintenance work mem.")?;

    // Get current max_parallel_workers setting
    let rows = client
        .query(crate::queries::GET_MAX_PARALLEL_WORKERS, &[])
        .await
        .context("Failed to get max_parallel_workers setting")?;

    if let Some(row) = rows.first() {
        let max_parallel_workers_str: String = row.get(0);
        let max_parallel_workers: u64 = max_parallel_workers_str
            .parse()
            .context("Failed to parse max_parallel_workers value")?;

        // Safety check: ensure max_parallel_maintenance_workers is less than max_parallel_workers/2
        let safe_limit = max_parallel_workers / PARALLEL_WORKERS_SAFETY_DIVISOR;
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
        // PostgreSQL SET command doesn't support parameterized queries
        // Since max_parallel_maintenance_workers is a u64, it's safe to format directly
        let max_parallel_maintenance_workers_sql = format!(
            "SET max_parallel_maintenance_workers TO {}",
            max_parallel_maintenance_workers
        );
        client
            .execute(&max_parallel_maintenance_workers_sql, &[])
            .await
            .context("Set the max_parallel_maintenance_workers.")?;
    } else {
        return Err(anyhow::anyhow!(
            "Failed to get max_parallel_workers setting"
        ));
    }

    // Validate maintenance_io_concurrency
    if maintenance_io_concurrency > MAX_MAINTENANCE_IO_CONCURRENCY {
        return Err(anyhow::anyhow!(
            "maintenance_io_concurrency ({}) must be {} or less",
            maintenance_io_concurrency,
            MAX_MAINTENANCE_IO_CONCURRENCY
        ));
    }

    // Set maintenance_io_concurrency
    // PostgreSQL SET command doesn't support parameterized queries
    // Since maintenance_io_concurrency is a u64, it's safe to format directly
    let maintenance_io_concurrency_sql = format!(
        "SET maintenance_io_concurrency TO {}",
        maintenance_io_concurrency
    );
    client
        .execute(&maintenance_io_concurrency_sql, &[])
        .await
        .context("Set the maintenance_io_concurrency.")?;

    Ok(())
}

// Create a new database connection with SSL support (without setting session parameters)
pub async fn create_connection_ssl(
    connection_string: &str,
    use_ssl: bool,
    accept_invalid_certs: bool,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    logger: &crate::logging::Logger,
) -> Result<tokio_postgres::Client> {
    if use_ssl {
        logger.log(
            crate::logging::LogLevel::Info,
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
            if let Some(ca_cert_path) = &ssl_ca_cert {
                logger.log(
                    crate::logging::LogLevel::Info,
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
                (&ssl_client_cert, &ssl_client_key)
            {
                logger.log(
                    crate::logging::LogLevel::Info,
                    &format!("Loading client certificate from: {}", client_cert_path),
                );
                let client_cert_data =
                    fs::read(client_cert_path).context("Failed to read client certificate file")?;

                logger.log(
                    crate::logging::LogLevel::Info,
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
            } else if ssl_client_cert.is_some() || ssl_client_key.is_some() {
                return Err(anyhow::anyhow!(
                    "Both --ssl-client-cert and --ssl-client-key must be provided together"
                ));
            }

            // Handle invalid certificate acceptance
            if accept_invalid_certs {
                logger.log(
                    crate::logging::LogLevel::Info,
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

        Ok(client)
    } else {
        logger.log(
            crate::logging::LogLevel::Info,
            "Creating connection to PostgreSQL",
        );
        // Connect without SSL
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .context("ERROR: Failed to connect to PostgreSQL")?;

        // Spawn the connection to run in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(client)
    }
}

// Create a new database connection with session parameters set and SSL support
pub async fn create_connection_with_session_parameters_ssl(
    connection_string: &str,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
    use_ssl: bool,
    accept_invalid_certs: bool,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    logger: &crate::logging::Logger,
) -> Result<tokio_postgres::Client> {
    // Create connection using the shared SSL connection logic
    let client = create_connection_ssl(
        connection_string,
        use_ssl,
        accept_invalid_certs,
        ssl_ca_cert,
        ssl_client_cert,
        ssl_client_key,
        logger,
    )
    .await?;

    // Set session parameters for this connection
    set_session_parameters(
        &client,
        maintenance_work_mem_gb,
        max_parallel_maintenance_workers,
        maintenance_io_concurrency,
        lock_timeout_seconds,
    )
    .await?;

    Ok(client)
}
