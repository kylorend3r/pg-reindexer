//! CLI tests for the `plan` subcommand
//! All tests validate argument parsing only — no database connection required.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::{Builder, NamedTempFile};
use std::io::Write;

#[allow(deprecated)]
fn get_cmd() -> Command {
    Command::cargo_bin("pg-reindexer").unwrap()
}

// ============================================================
// Help and discovery
// ============================================================

#[test]
fn test_plan_help_flag() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("sort-by"))
        .stdout(predicate::str::contains("format"))
        .stdout(predicate::str::contains("output"));
}

#[test]
fn test_main_help_shows_plan_subcommand() {
    let mut cmd = get_cmd();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("plan"));
}

// ============================================================
// Schema requirement
// ============================================================

#[test]
fn test_plan_requires_schema_or_discover() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--database")
        .arg("mydb")
        .env_clear()
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("schema")
                .or(predicate::str::contains("discover-all-schemas")),
        );
}

#[test]
fn test_plan_with_schema_flag() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_with_discover_all_schemas() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--discover-all-schemas")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_with_multiple_schemas() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public,app,analytics")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// --sort-by flag
// ============================================================

#[test]
fn test_plan_sort_by_bloat() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("bloat")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_size() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("size")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_scan_frequency() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("scan-frequency")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_age() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("age")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_all_valid_criteria() {
    for criterion in &["bloat", "size", "scan-frequency", "age"] {
        let mut cmd = get_cmd();
        cmd.arg("plan")
            .arg("--schema")
            .arg("public")
            .arg("--sort-by")
            .arg(criterion)
            .env_clear()
            .assert()
            .code(predicate::ne(2));
    }
}

#[test]
fn test_plan_sort_by_multiple_criteria() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("bloat,size")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_all_criteria_combined() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("bloat,size,scan-frequency,age")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_by_invalid_criterion() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("invalid_criterion")
        .env_clear()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid").or(predicate::str::contains("invalid")));
}

#[test]
fn test_plan_sort_by_default_is_bloat() {
    // Without --sort-by, should parse fine (default is bloat)
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// --format flag
// ============================================================

#[test]
fn test_plan_format_json() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--format")
        .arg("json")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_format_csv() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--format")
        .arg("csv")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_format_invalid() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--format")
        .arg("xml")
        .assert()
        .failure();
}

// ============================================================
// --output flag
// ============================================================

#[test]
fn test_plan_with_output_file() {
    let out_file = NamedTempFile::new().unwrap();
    let out_path = out_file.path().to_str().unwrap();

    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--output")
        .arg(out_path)
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_output_with_csv_format() {
    let out_file = NamedTempFile::new().unwrap();
    let out_path = out_file.path().to_str().unwrap();

    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--format")
        .arg("csv")
        .arg("--output")
        .arg(out_path)
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// Connection flags
// ============================================================

#[test]
fn test_plan_connection_flags() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--host")
        .arg("localhost")
        .arg("--port")
        .arg("5432")
        .arg("--username")
        .arg("postgres")
        .arg("--schema")
        .arg("public")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_ssl_flags() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--ssl")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_ssl_self_signed() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--ssl")
        .arg("--ssl-self-signed")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

// ============================================================
// Config file support
// ============================================================

#[test]
fn test_plan_with_config_file() {
    let mut config_file = Builder::new().suffix(".toml").tempfile().unwrap();
    writeln!(
        config_file.as_file_mut(),
        r#"
host = "localhost"
port = 5432
database = "mydb"
username = "postgres"
schema = "public"
"#
    )
    .unwrap();

    let config_path = config_file.path().to_str().unwrap();
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--config")
        .arg(config_path)
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_config_file_not_found() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--config")
        .arg("/nonexistent/config.toml")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("not found")
                .or(predicate::str::contains("Configuration file")),
        );
}

// ============================================================
// Combined options
// ============================================================

#[test]
fn test_plan_full_options() {
    let out_file = NamedTempFile::new().unwrap();
    let out_path = out_file.path().to_str().unwrap();

    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("bloat,size")
        .arg("--format")
        .arg("csv")
        .arg("--output")
        .arg(out_path)
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}

#[test]
fn test_plan_sort_and_format_json() {
    let mut cmd = get_cmd();
    cmd.arg("plan")
        .arg("--schema")
        .arg("public")
        .arg("--sort-by")
        .arg("age")
        .arg("--format")
        .arg("json")
        .env_clear()
        .assert()
        .code(predicate::ne(2));
}
