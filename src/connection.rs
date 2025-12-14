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

    // Set lock_timeout (convert from seconds to milliseconds)
    let lock_timeout_ms = lock_timeout_seconds * MILLISECONDS_PER_SECOND;
    client
        .execute(
            &format!("SET lock_timeout TO '{}ms';", lock_timeout_ms),
            &[],
        )
        .await
        .context("Set the lock_timeout.")?;

    // Set deadlock_timeout
    client
        .execute(&format!("SET deadlock_timeout TO '{}';", DEFAULT_DEADLOCK_TIMEOUT), &[])
        .await
        .context("Set the deadlock_timeout.")?;

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

    // Validate maintenance_io_concurrency
    if maintenance_io_concurrency > MAX_MAINTENANCE_IO_CONCURRENCY {
        return Err(anyhow::anyhow!(
            "maintenance_io_concurrency ({}) must be {} or less",
            maintenance_io_concurrency,
            MAX_MAINTENANCE_IO_CONCURRENCY
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
    if use_ssl {
        logger.log(
            crate::logging::LogLevel::Info,
            "Creating connection for worker",
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
                let client_cert_data =
                    fs::read(client_cert_path).context("Failed to read client certificate file")?;
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
                    "Both ssl_client_cert and ssl_client_key must be provided together"
                ));
            }

            // Handle invalid certificate acceptance
            if accept_invalid_certs {
                tls_builder.danger_accept_invalid_certs(true);
            }

            let tls_connector = tls_builder
                .build()
                .context("Failed to create TLS connector")?;

            let tls = MakeTlsConnector::new(tls_connector);
            config
                .connect(tls)
                .await
                .context("Failed to connect to PostgreSQL with SSL")?
        };

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
            lock_timeout_seconds,
        )
        .await?;

        Ok(client)
    } else {
        logger.log(
            crate::logging::LogLevel::Info,
            "Creating connection for worker",
        );
        // Connect without SSL
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
            lock_timeout_seconds,
        )
        .await?;

        Ok(client)
    }
}
