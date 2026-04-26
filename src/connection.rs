use crate::config::{
    MILLISECONDS_PER_SECOND, DEFAULT_DEADLOCK_TIMEOUT, MAX_MAINTENANCE_IO_CONCURRENCY,
    PARALLEL_WORKERS_SAFETY_DIVISOR, DEFAULT_POSTGRES_HOST, DEFAULT_POSTGRES_PORT,
    DEFAULT_POSTGRES_DATABASE, DEFAULT_POSTGRES_USERNAME,
};
use crate::credentials::get_password_from_pgpass;
use crate::types::{LogStatement, SslMode};
use anyhow::{Context, Result};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::{env, fs};
use tokio_postgres::{Config, NoTls, config::SslMode as TokioSslMode};
use zeroize::Zeroizing;

/// Password wrapper that zeroes memory on drop.
/// Debug output shows [REDACTED] to prevent accidental logging.
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(s: String) -> Self {
        Self(Zeroizing::new(s))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Clone for SecretString {
    fn clone(&self) -> Self {
        Self::new(self.0.as_str().to_owned())
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Connection configuration structure
/// 
/// Holds all connection parameters needed to connect to PostgreSQL
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: Option<SecretString>,
    pub sslmode: SslMode,
    pub ssl_ca_cert: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,
}

impl ConnectionConfig {
    /// Build connection configuration from command-line arguments
    /// 
    /// Resolves connection parameters in the following order:
    /// 1. Command-line arguments
    /// 2. Environment variables (PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD)
    /// 3. Default values
    /// 
    /// For password, also checks .pgpass file if not provided via args or env.
    pub fn from_args(
        host: Option<String>,
        port: Option<u16>,
        database: Option<String>,
        username: Option<String>,
        password: Option<String>,
        sslmode: SslMode,
        ssl_ca_cert: Option<String>,
        ssl_client_cert: Option<String>,
        ssl_client_key: Option<String>,
    ) -> Result<Self> {
        let host = host
            .or_else(|| env::var("PG_HOST").ok())
            .unwrap_or_else(|| DEFAULT_POSTGRES_HOST.to_string());

        let port = port
            .or_else(|| env::var("PG_PORT").ok().and_then(|p| p.parse().ok()))
            .unwrap_or(DEFAULT_POSTGRES_PORT);

        let database = database
            .or_else(|| env::var("PG_DATABASE").ok())
            .unwrap_or_else(|| DEFAULT_POSTGRES_DATABASE.to_string());

        let username = username
            .or_else(|| env::var("PG_USER").ok())
            .unwrap_or_else(|| DEFAULT_POSTGRES_USERNAME.to_string());

        let password = password
            .or_else(|| {
                let env_password = env::var("PG_PASSWORD").ok();
                // If PG_PASSWORD is set but empty, treat it as None to allow pgpass fallback
                if env_password.as_ref().is_some_and(|p| p.is_empty()) {
                    None
                } else {
                    env_password
                }
            })
            .or_else(|| {
                get_password_from_pgpass(&host, port, &database, &username).unwrap_or(None)
            })
            .map(SecretString::new);

        Ok(Self {
            host,
            port,
            database,
            username,
            password,
            sslmode,
            ssl_ca_cert,
            ssl_client_cert,
            ssl_client_key,
        })
    }

    /// Build PostgreSQL connection string from configuration
    pub fn build_connection_string(&self) -> String {
        let mut connection_string = format!(
            "host={} port={} dbname={} user={}",
            escape_libpq_value(&self.host),
            self.port,
            escape_libpq_value(&self.database),
            escape_libpq_value(&self.username),
        );

        if let Some(ref pwd) = self.password {
            connection_string.push_str(&format!(" password={}", escape_libpq_value(pwd.expose())));
        }

        connection_string
    }
}

fn escape_libpq_value(s: &str) -> String {
    if s.chars().any(|c| c.is_whitespace() || c == '\'' || c == '\\') {
        let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{}'", escaped)
    } else {
        s.to_owned()
    }
}

pub async fn set_session_parameters(
    client: &tokio_postgres::Client,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
    log_statement: LogStatement,
) -> Result<()> {
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
    let log_statement_sql = format!("SET log_statement TO '{}'", log_statement.as_pg_value());
    client
        .execute(&log_statement_sql, &[])
        .await
        .context("Set log_statement.")?;

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
    sslmode: &SslMode,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    logger: &crate::logging::Logger,
) -> Result<tokio_postgres::Client> {
    logger.log(
        crate::logging::LogLevel::Info,
        "Creating connection to PostgreSQL",
    );

    if *sslmode == SslMode::Disable {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .context("ERROR: Failed to connect to PostgreSQL")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        return Ok(client);
    }

    // SSL path
    let mut config: Config = connection_string
        .parse()
        .context("Failed to parse connection string")?;
    config.ssl_mode(TokioSslMode::Require);

    let mut tls_builder = TlsConnector::builder();

    match sslmode {
        SslMode::Require => {
            // Encrypt only — skip certificate and hostname verification.
            // Equivalent to libpq sslmode=require.
            tls_builder.danger_accept_invalid_certs(true);
            tls_builder.danger_accept_invalid_hostnames(true);
        }
        SslMode::VerifyCa => {
            // Verify the certificate chain but not the hostname.
            tls_builder.danger_accept_invalid_hostnames(true);
        }
        SslMode::VerifyFull => {
            // native-tls default: verify both certificate chain and hostname.
        }
        SslMode::Disable => unreachable!(),
    }

    // Optional custom CA certificate
    if let Some(ca_cert_path) = &ssl_ca_cert {
        logger.log(
            crate::logging::LogLevel::Info,
            &format!("Loading CA certificate from: {}", ca_cert_path),
        );
        let ca_cert_data = fs::read(ca_cert_path).context("Failed to read CA certificate file")?;
        let ca_cert =
            Certificate::from_pem(&ca_cert_data).context("Failed to parse CA certificate")?;
        tls_builder.add_root_certificate(ca_cert);
    }

    // Optional client certificate + key (mutual TLS)
    match (&ssl_client_cert, &ssl_client_key) {
        (Some(cert_path), Some(key_path)) => {
            logger.log(
                crate::logging::LogLevel::Info,
                &format!("Loading client certificate from: {}", cert_path),
            );
            let cert_data =
                fs::read(cert_path).context("Failed to read client certificate file")?;

            logger.log(
                crate::logging::LogLevel::Info,
                &format!("Loading client key from: {}", key_path),
            );
            let key_data = fs::read(key_path).context("Failed to read client key file")?;

            let mut identity_data = cert_data.clone();
            identity_data.extend_from_slice(&key_data);

            let identity = Identity::from_pkcs12(&identity_data, "")
                .or_else(|_| Identity::from_pkcs8(&cert_data, &key_data))
                .context("Failed to parse client certificate and key")?;

            tls_builder.identity(identity);
        }
        (None, None) => {}
        _ => {
            return Err(anyhow::anyhow!(
                "Both --ssl-client-cert and --ssl-client-key must be provided together"
            ));
        }
    }

    let tls = MakeTlsConnector::new(
        tls_builder
            .build()
            .context("Failed to create TLS connector")?,
    );

    let (client, connection) = config
        .connect(tls)
        .await
        .context("ERROR: Failed to connect to PostgreSQL with SSL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

// Create a new database connection with session parameters set and SSL support
pub async fn create_connection_with_session_parameters_ssl(
    connection_string: &str,
    maintenance_work_mem_gb: u64,
    max_parallel_maintenance_workers: u64,
    maintenance_io_concurrency: u64,
    lock_timeout_seconds: u64,
    log_statement: LogStatement,
    sslmode: &SslMode,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    logger: &crate::logging::Logger,
) -> Result<tokio_postgres::Client> {
    let client = create_connection_ssl(
        connection_string,
        sslmode,
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
        log_statement,
    )
    .await?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::{create_connection_ssl, escape_libpq_value};
    use crate::logging::Logger;
    use crate::types::SslMode;

    #[test]
    fn plain_alphanumeric_is_unchanged() {
        assert_eq!(escape_libpq_value("mypassword123"), "mypassword123");
    }

    #[test]
    fn password_with_space_is_quoted() {
        assert_eq!(escape_libpq_value("my password"), "'my password'");
    }

    #[test]
    fn password_with_single_quote_is_escaped() {
        assert_eq!(escape_libpq_value("it's"), "'it\\'s'");
    }

    #[test]
    fn password_with_backslash_is_escaped() {
        assert_eq!(escape_libpq_value("pass\\word"), "'pass\\\\word'");
    }

    #[test]
    fn password_with_space_and_quote_is_fully_escaped() {
        assert_eq!(escape_libpq_value("it's alive"), "'it\\'s alive'");
    }

    #[test]
    fn empty_string_is_unchanged() {
        assert_eq!(escape_libpq_value(""), "");
    }

    fn silent_logger() -> Logger {
        Logger::new_with_silence(String::new(), true)
    }

    // The cert/key mismatch check fires before any network I/O, so these
    // tests do not require a running PostgreSQL instance.

    #[tokio::test]
    async fn ssl_client_cert_without_key_returns_error() {
        let logger = silent_logger();
        let err = create_connection_ssl(
            "host=localhost port=5432 dbname=postgres user=postgres",
            &SslMode::Require,
            None,
            Some("client.pem".to_string()),
            None, // key intentionally omitted
            &logger,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Both --ssl-client-cert and --ssl-client-key must be provided together"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn ssl_client_key_without_cert_returns_error() {
        let logger = silent_logger();
        let err = create_connection_ssl(
            "host=localhost port=5432 dbname=postgres user=postgres",
            &SslMode::Require,
            None,
            None, // cert intentionally omitted
            Some("client.key".to_string()),
            &logger,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Both --ssl-client-cert and --ssl-client-key must be provided together"),
            "unexpected error: {}",
            err
        );
    }
}
