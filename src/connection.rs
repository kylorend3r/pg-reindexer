use anyhow::{Context, Result};
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use rustls::{ClientConfig, RootCertStore};

/// SSL configuration options
#[derive(Debug, Clone)]
pub struct SslConfig {
    pub enabled: bool,
}

impl Default for SslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

/// Create a TLS connector for PostgreSQL connections (no certificate verification)
fn create_tls_connector() -> Result<MakeRustlsConnect> {
    // Create a TLS config that accepts any certificate (no verification)
    let config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();

    Ok(MakeRustlsConnect::new(config))
}

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
    let lock_timeout_ms = lock_timeout_seconds * 1000;
    client
        .execute(
            &format!("SET lock_timeout TO '{}ms';", lock_timeout_ms),
            &[],
        )
        .await
        .context("Set the lock_timeout.")?;

    // Set deadlock_timeout to 1 second
    client
        .execute("SET deadlock_timeout TO '1s';", &[])
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
pub async fn create_connection_with_session_parameters(
    connection_string: &str,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
    ssl_config: &SslConfig,
) -> Result<tokio_postgres::Client> {
    // Connect to PostgreSQL with or without SSL
    let client = if ssl_config.enabled {
        let tls_connector = create_tls_connector()
            .context("Failed to create TLS connector")?;
        let (client, connection) = tokio_postgres::connect(connection_string, tls_connector)
            .await
            .context("Failed to connect to PostgreSQL with SSL")?;
        
        // Spawn the connection to run in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        
        client
    } else {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;
        
        // Spawn the connection to run in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        
        client
    };

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
