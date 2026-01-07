//! CLI-level integration tests using assert_cmd
//! Tests argument validation, help/version, and error scenarios

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::NamedTempFile;

/// Helper to get the binary command
#[allow(deprecated)] // cargo_bin is deprecated but still works for our use case
fn get_cmd() -> Command {
    Command::cargo_bin("pg-reindexer").unwrap()
}

#[test]
fn test_help_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("PostgreSQL Index Reindexer"))
        .stdout(predicate::str::contains("--schema"))
        .stdout(predicate::str::contains("--threads"));
}

#[test]
fn test_version_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("pg-reindexer"))
        .stdout(predicate::str::contains("3.0.2"));
}

#[test]
fn test_missing_required_schema() {
    let mut cmd = get_cmd();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("schema")
            .or(predicate::str::contains("required")));
}

#[test]
#[ignore] // Excluded from default test runs
fn test_thread_count_too_high() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--threads")
        .arg("40")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Thread count")
            .and(predicate::str::contains("exceeds maximum limit"))
            .or(predicate::str::contains("max_parallel_workers")));
}

#[test]
fn test_bloat_threshold_too_high() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--reindex-only-bloated")
        .arg("150")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Bloat threshold")
            .and(predicate::str::contains("must be between")));
}

#[test]
fn test_maintenance_work_mem_too_high() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--maintenance-work-mem-gb")
        .arg("50")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Maintenance work mem")
            .and(predicate::str::contains("exceeds maximum limit")));
}

#[test]
fn test_min_size_greater_than_max_size() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--min-size-gb")
        .arg("100")
        .arg("--max-size-gb")
        .arg("50")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Minimum index size")
            .and(predicate::str::contains("cannot be greater than")));
}

#[test]
fn test_invalid_index_type() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--index-type")
        .arg("invalid_type")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid index filter type")
            .or(predicate::str::contains("invalid")));
}

#[test]
fn test_valid_index_types() {
    for index_type in &["btree", "constraint", "all"] {
        let mut cmd = get_cmd();
        // This will fail at DB connection, but should pass argument parsing
        cmd.arg("--schema")
            .arg("public")
            .arg("--index-type")
            .arg(index_type)
            .env_clear()
            .assert()
            .code(predicate::ne(101)) // Not a parsing error (101 is often used for parse errors)
            .code(predicate::ne(2)); // Not a clap error (2 is often used for invalid args)
    }
}

#[test]
fn test_dry_run_flag_parsing() {
    let mut cmd = get_cmd();
    // Should parse --dry-run flag correctly
    // Will fail at DB connection, but should not fail at argument parsing
    cmd.arg("--schema")
        .arg("public")
        .arg("--dry-run")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_log_file_flag() {
    let log_file = NamedTempFile::new().unwrap();
    let log_path = log_file.path().to_str().unwrap();
    
    let mut cmd = get_cmd();
    // Should parse --log-file flag correctly
    cmd.arg("--schema")
        .arg("public")
        .arg("--log-file")
        .arg(log_path)
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_all_skip_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--skip-inactive-replication-slots")
        .arg("--skip-sync-replication-connection")
        .arg("--skip-active-vacuums")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_table_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--table")
        .arg("test_table")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_size_filtering_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--min-size-gb")
        .arg("1")
        .arg("--max-size-gb")
        .arg("100")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_connection_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--host")
        .arg("localhost")
        .arg("--port")
        .arg("5432")
        .arg("--database")
        .arg("testdb")
        .arg("--username")
        .arg("testuser")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_ssl_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--ssl")
        .arg("--ssl-self-signed")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_threads_at_max_boundary() {
    let mut cmd = get_cmd();
    // 32 is the max, should parse correctly
    cmd.arg("--schema")
        .arg("public")
        .arg("--threads")
        .arg("32")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_maintenance_work_mem_at_max_boundary() {
    let mut cmd = get_cmd();
    // 32 GB is the max, should parse correctly
    cmd.arg("--schema")
        .arg("public")
        .arg("--maintenance-work-mem-gb")
        .arg("32")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_bloat_threshold_at_boundaries() {
    // Test valid boundaries (0 and 100)
    for threshold in &["0", "100"] {
        let mut cmd = get_cmd();
        cmd.arg("--schema")
            .arg("public")
            .arg("--reindex-only-bloated")
            .arg(threshold)
            .env_clear()
            .assert()
            .code(predicate::ne(101))
            .code(predicate::ne(2));
    }
}

#[test]
fn test_exclude_indexes_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--exclude-indexes")
        .arg("idx1,idx2,idx3")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_resume_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--resume")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_silence_mode_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--silence-mode")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_concurrently_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--concurrently")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_clean_orphaned_indexes_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--clean-orphaned-indexes")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_discover_all_schemas_flag() {
    let mut cmd = get_cmd();
    // Should parse --discover-all-schemas flag correctly
    // Will fail at DB connection, but should not fail at argument parsing
    cmd.arg("--discover-all-schemas")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_discover_all_schemas_with_other_flags() {
    let mut cmd = get_cmd();
    // Should parse --discover-all-schemas with other flags
    cmd.arg("--discover-all-schemas")
        .arg("--dry-run")
        .arg("--index-type")
        .arg("btree")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_discover_all_schemas_without_schema() {
    let mut cmd = get_cmd();
    // Should work without --schema when --discover-all-schemas is provided
    cmd.arg("--discover-all-schemas")
        .arg("--dry-run")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_help_contains_discover_all_schemas() {
    let mut cmd = get_cmd();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--discover-all-schemas"));
}

// ============================================================================
// Multi-database support tests
// ============================================================================

#[test]
fn test_help_contains_multi_database_description() {
    let mut cmd = get_cmd();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Database name(s)"))
        .stdout(predicate::str::contains("comma-separated"));
}

#[test]
fn test_single_database_parsing() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("mydb")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_multiple_databases_parsing() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2,db3")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_multiple_databases_with_whitespace() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1 , db2 , db3")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_multiple_databases_with_mixed_whitespace() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("  db1  ,  db2  ,  db3  ")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_other_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--dry-run")
        .arg("--threads")
        .arg("2")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_discover_all_schemas() {
    let mut cmd = get_cmd();
    cmd.arg("--discover-all-schemas")
        .arg("--database")
        .arg("db1,db2,db3")
        .arg("--dry-run")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_table_filter() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--table")
        .arg("test_table")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_exclude_indexes() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--exclude-indexes")
        .arg("idx1,idx2")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_ssl_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--ssl")
        .arg("--ssl-self-signed")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_all_connection_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2,db3")
        .arg("--host")
        .arg("localhost")
        .arg("--port")
        .arg("5432")
        .arg("--username")
        .arg("testuser")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_large_number_of_databases() {
    let mut cmd = get_cmd();
    let databases: Vec<String> = (1..=10).map(|i| format!("db{}", i)).collect();
    let db_list = databases.join(",");
    
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg(&db_list)
        .arg("--dry-run")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_special_characters() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("test_db,test-db,test_db_2")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_resume_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--resume")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_silence_mode() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2,db3")
        .arg("--silence-mode")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_concurrently_flag() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--concurrently")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_clean_orphaned_indexes() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--clean-orphaned-indexes")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_bloat_threshold() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--reindex-only-bloated")
        .arg("50")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_size_filters() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--min-size-gb")
        .arg("1")
        .arg("--max-size-gb")
        .arg("100")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_index_type() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--index-type")
        .arg("constraint")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_maintenance_settings() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--maintenance-work-mem-gb")
        .arg("2")
        .arg("--max-parallel-maintenance-workers")
        .arg("2")
        .arg("--maintenance-io-concurrency")
        .arg("10")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_lock_timeout() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--lock-timeout-seconds")
        .arg("30")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_custom_log_file() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--log-file")
        .arg("/tmp/test_reindexer.log")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_all_skip_flags() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2")
        .arg("--skip-inactive-replication-slots")
        .arg("--skip-sync-replication-connection")
        .arg("--skip-active-vacuums")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_environment_variable_fallback() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .env("PG_DATABASE", "env_db1,env_db2")
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_argument_overrides_env() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("arg_db1,arg_db2")
        .env("PG_DATABASE", "env_db1,env_db2")
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_empty_database_string_handling() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_only_commas() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg(",,,")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_commas_and_spaces_only() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg(" , , , ")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_trailing_comma() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,db2,")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_leading_comma() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg(",db1,db2")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

#[test]
fn test_database_with_consecutive_commas() {
    let mut cmd = get_cmd();
    cmd.arg("--schema")
        .arg("public")
        .arg("--database")
        .arg("db1,,db2")
        .env_clear()
        .assert()
        .code(predicate::ne(101))
        .code(predicate::ne(2));
}

