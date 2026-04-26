use pg_reindexer::logging::Logger;
use pg_reindexer::memory_table::SharedIndexMemoryTable;
use pg_reindexer::queries;
use pg_reindexer::types::{IndexInfo, LogFormat};
use std::collections::HashSet;
use tempfile::NamedTempFile;

fn test_index(schema: &str, table: &str, index: &str) -> IndexInfo {
    IndexInfo {
        schema_name: schema.to_string(),
        index_name: index.to_string(),
        index_type: "btree".to_string(),
        table_name: table.to_string(),
        size_bytes: Some(1024),
        parent_table_name: None,
    }
}

#[tokio::test]
async fn test_completed_table_names_only_include_completed_and_are_deduplicated() {
    let memory_table = SharedIndexMemoryTable::new();
    let log_file = NamedTempFile::new().unwrap();
    let logger = Logger::new(
        log_file.path().to_string_lossy().to_string(),
        true,
        LogFormat::Text,
    );

    memory_table
        .initialize_with_indexes(vec![
            test_index("public", "users", "users_idx_1"),
            test_index("public", "users", "users_idx_2"),
            test_index("public", "orders", "orders_idx_1"),
        ])
        .await;

    memory_table
        .release_index_and_mark_completed("public", "users_idx_1", "users", 0, &logger)
        .await;
    memory_table
        .release_index_and_mark_completed("public", "users_idx_2", "users", 0, &logger)
        .await;
    memory_table
        .release_index_and_mark_failed("public", "orders_idx_1", "orders", 0, &logger)
        .await;

    let completed = memory_table.get_completed_table_names().await;
    assert_eq!(
        completed,
        HashSet::from([("public".to_string(), "users".to_string())])
    );
}

#[test]
fn test_analyze_query_constants_cover_both_paths() {
    assert!(queries::ANALYZE_TABLE_SKIP_LOCKED.contains("SKIP_LOCKED"));
    assert!(!queries::ANALYZE_TABLE.contains("SKIP_LOCKED"));
    assert!(queries::ANALYZE_TABLE.contains("ANALYZE"));
    assert!(queries::GET_PG_VERSION_NUM.contains("server_version_num"));
}
