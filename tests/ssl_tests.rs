//! SSL/TLS tests for pg-reindexer
//!
//! ## CLI tests (always run, no DB required)
//! Verify that all SSL flags parse correctly and appear in help output.
//!
//! ## DB integration tests (marked #[ignore], require a PostgreSQL with SSL)
//! Set the standard connection env vars plus:
//!   PG_SSL_CA_CERT      — path to the server CA certificate (.pem)
//!   PG_SSL_CLIENT_CERT  — path to a client certificate (.pem)
//!   PG_SSL_CLIENT_KEY   — path to the client private key (.pem)
//!
//! Run with:
//!   cargo test --test ssl_tests -- --include-ignored

use assert_cmd::Command;
use pg_reindexer::types::SslMode;
use predicates::prelude::*;
use std::io::Write;
use tempfile::Builder;

fn cmd() -> Command {
    #[allow(deprecated)]
    Command::cargo_bin("pg-reindexer").unwrap()
}

// ============================================================
// Help text
// ============================================================

#[test]
fn test_help_shows_sslmode_flag() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--sslmode"));
}

#[test]
fn test_help_shows_sslmode_values() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("require"))
        .stdout(predicate::str::contains("verify-ca"))
        .stdout(predicate::str::contains("verify-full"));
}

#[test]
fn test_help_shows_ssl_ca_cert_flag() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--ssl-ca-cert"));
}

#[test]
fn test_help_shows_ssl_client_cert_flag() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--ssl-client-cert"));
}

#[test]
fn test_help_shows_ssl_client_key_flag() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--ssl-client-key"));
}

// ============================================================
// CLI argument parsing (no DB connection)
// ============================================================

#[test]
fn test_sslmode_disable_parses() {
    cmd()
        .args(["--schema", "public", "--sslmode", "disable"])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_sslmode_require_parses() {
    cmd()
        .args(["--schema", "public", "--sslmode", "require"])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_sslmode_verify_ca_parses() {
    cmd()
        .args(["--schema", "public", "--sslmode", "verify-ca"])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_sslmode_verify_full_parses() {
    cmd()
        .args(["--schema", "public", "--sslmode", "verify-full"])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_sslmode_invalid_rejected() {
    cmd()
        .args(["--schema", "public", "--sslmode", "yes-please"])
        .env_clear()
        .assert()
        .failure()
        .stderr(predicate::str::contains("sslmode").or(predicate::str::contains("invalid")));
}

#[test]
fn test_ssl_ca_cert_parses() {
    let f = Builder::new().suffix(".pem").tempfile().unwrap();
    let path = f.path().to_str().unwrap();

    cmd()
        .args(["--schema", "public", "--sslmode", "verify-ca", "--ssl-ca-cert", path])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_ssl_client_cert_and_key_parses() {
    let cert = Builder::new().suffix(".pem").tempfile().unwrap();
    let key = Builder::new().suffix(".pem").tempfile().unwrap();

    cmd()
        .args([
            "--schema", "public",
            "--sslmode", "verify-full",
            "--ssl-client-cert", cert.path().to_str().unwrap(),
            "--ssl-client-key", key.path().to_str().unwrap(),
        ])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_ssl_all_flags_parse() {
    let ca = Builder::new().suffix(".pem").tempfile().unwrap();
    let cert = Builder::new().suffix(".pem").tempfile().unwrap();
    let key = Builder::new().suffix(".pem").tempfile().unwrap();

    cmd()
        .args([
            "--schema", "public",
            "--sslmode", "verify-full",
            "--ssl-ca-cert", ca.path().to_str().unwrap(),
            "--ssl-client-cert", cert.path().to_str().unwrap(),
            "--ssl-client-key", key.path().to_str().unwrap(),
        ])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// Config file: SSL settings
// ============================================================

#[test]
fn test_config_file_sslmode_require() {
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        f.as_file_mut(),
        r#"schema = "public"
sslmode = "require""#
    )
    .unwrap();

    cmd()
        .args(["--config", f.path().to_str().unwrap()])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_config_file_sslmode_verify_ca() {
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        f.as_file_mut(),
        r#"schema = "public"
sslmode = "verify-ca""#
    )
    .unwrap();

    cmd()
        .args(["--config", f.path().to_str().unwrap()])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_config_file_ssl_ca_cert() {
    let ca = Builder::new().suffix(".pem").tempfile().unwrap();
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        f.as_file_mut(),
        r#"schema = "public"
ssl = true
ssl-ca-cert = "{}"#,
        ca.path().to_str().unwrap()
    )
    .unwrap();
    writeln!(f.as_file_mut(), r#""#).unwrap();

    cmd()
        .args(["--config", f.path().to_str().unwrap()])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_config_file_all_ssl_settings() {
    let ca = Builder::new().suffix(".pem").tempfile().unwrap();
    let cert = Builder::new().suffix(".pem").tempfile().unwrap();
    let key = Builder::new().suffix(".pem").tempfile().unwrap();
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        f.as_file_mut(),
        r#"schema = "public"
sslmode = "verify-full"
ssl-ca-cert = "{ca}"
ssl-client-cert = "{cc}"
ssl-client-key = "{ck}""#,
        ca = ca.path().to_str().unwrap(),
        cc = cert.path().to_str().unwrap(),
        ck = key.path().to_str().unwrap(),
    )
    .unwrap();

    cmd()
        .args(["--config", f.path().to_str().unwrap()])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_cli_sslmode_overrides_config() {
    let mut f = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        f.as_file_mut(),
        r#"schema = "public"
sslmode = "disable""#
    )
    .unwrap();

    cmd()
        .args(["--config", f.path().to_str().unwrap(), "--sslmode", "require"])
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// DB integration tests — require an SSL-enabled PostgreSQL
//
// No cert files needed. Set the standard env vars:
//   PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
//
// Run with:
//   cargo test --test ssl_tests -- --include-ignored
// ============================================================

fn ssl_db_connection_string() -> String {
    let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".to_string());
    let db = std::env::var("PG_DATABASE").unwrap_or_else(|_| "postgres".to_string());
    let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string());
    let password =
        std::env::var("PG_PASSWORD").expect("PG_PASSWORD must be set for SSL DB tests");
    format!(
        "host={host} port={port} dbname={db} user={user} password={password}",
        host = host,
        port = port,
        db = db,
        user = user,
        password = password,
    )
}

fn silent_logger() -> pg_reindexer::logging::Logger {
    pg_reindexer::logging::Logger::new_with_silence(String::new(), true)
}

/// sslmode=require — encrypt only, no certificate verification.
/// Works with both self-signed and CA-signed server certificates.
/// This is the right mode for your default PostgreSQL setup (server.crt / server.key).
#[tokio::test]
#[ignore]
async fn test_sslmode_require_connects() {
    let conn = ssl_db_connection_string();
    let logger = silent_logger();

    let client = pg_reindexer::connection::create_connection_ssl(
        &conn,
        &SslMode::Require,
        None,
        None,
        None,
        &logger,
    )
    .await
    .expect("sslmode=require should succeed");

    let row = client
        .query_one("SELECT 1::int", &[])
        .await
        .expect("query over SSL connection should succeed");
    assert_eq!(row.get::<_, i32>(0), 1);
}

/// sslmode=verify-ca — verify the certificate chain against the system CA store.
/// Use when the server has a certificate issued by a trusted CA.
#[tokio::test]
#[ignore]
async fn test_sslmode_verify_ca_connects() {
    let conn = ssl_db_connection_string();
    let logger = silent_logger();

    let client = pg_reindexer::connection::create_connection_ssl(
        &conn,
        &SslMode::VerifyCa,
        None,
        None,
        None,
        &logger,
    )
    .await
    .expect("sslmode=verify-ca should succeed when the server certificate is trusted");

    let row = client
        .query_one("SELECT 1::int", &[])
        .await
        .expect("query over SSL connection should succeed");
    assert_eq!(row.get::<_, i32>(0), 1);
}

/// sslmode=verify-full — verify certificate chain and hostname.
#[tokio::test]
#[ignore]
async fn test_sslmode_verify_full_connects() {
    let conn = ssl_db_connection_string();
    let logger = silent_logger();

    let client = pg_reindexer::connection::create_connection_ssl(
        &conn,
        &SslMode::VerifyFull,
        None,
        None,
        None,
        &logger,
    )
    .await
    .expect("sslmode=verify-full should succeed when the server certificate matches the hostname");

    let row = client
        .query_one("SELECT 1::int", &[])
        .await
        .expect("query over SSL connection should succeed");
    assert_eq!(row.get::<_, i32>(0), 1);
}

/// Confirm pg_stat_ssl reports ssl = true for the active session.
#[tokio::test]
#[ignore]
async fn test_sslmode_require_pg_stat_ssl_is_true() {
    let conn = ssl_db_connection_string();
    let logger = silent_logger();

    let client = pg_reindexer::connection::create_connection_ssl(
        &conn,
        &SslMode::Require,
        None,
        None,
        None,
        &logger,
    )
    .await
    .expect("sslmode=require should succeed");

    let row = client
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("pg_stat_ssl query should succeed");
    let ssl_active: bool = row.get(0);
    assert!(ssl_active, "pg_stat_ssl must report ssl = true for this session");
}

/// Confirm that sslmode=disable is rejected when the server enforces SSL via pg_hba.conf hostssl.
#[tokio::test]
#[ignore]
async fn test_sslmode_disable_is_rejected_when_server_requires_ssl() {
    let conn = ssl_db_connection_string();
    let logger = silent_logger();

    let result = pg_reindexer::connection::create_connection_ssl(
        &conn,
        &SslMode::Disable,
        None,
        None,
        None,
        &logger,
    )
    .await;

    assert!(
        result.is_err(),
        "sslmode=disable should be rejected by a server that enforces SSL"
    );
}
