use anyhow::{Context, Result};
use tokio_postgres::NoTls;

pub async fn set_session_parameters(
    client: &tokio_postgres::Client,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
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
