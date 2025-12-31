use crate::types::IndexFilterType;

// SQL queries used throughout the application
pub const GET_ACTIVE_VACUUM: &str = r#"
    SELECT * FROM pg_stat_activity 
    WHERE state = 'active' 
    AND lower(query) LIKE 'vacuum%' 
    AND pid != pg_backend_pid();
"#;

/// Builds the SQL query for fetching indexes based on filters
/// 
/// # Parameters
/// - `has_table_filter`: If true, includes `t.relname = $2` condition
/// - `index_type`: Determines the index type filter (All, Btree, or Constraint)
/// 
/// # Returns
/// The constructed SQL query string
pub fn build_indexes_query(has_table_filter: bool, index_type: IndexFilterType) -> String {
    // Base query structure (common to all variants)
    let base_query = r#"
        SELECT 
            n.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            ROUND(pg_relation_size(i.oid)::numeric/(1024*1024*1024), 2) || ' GB' as index_size,
            am.amname as index_type
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        WHERE n.nspname = $1"#;

    // Build WHERE clause conditions
    let mut conditions = vec!["    AND i.relkind = 'i'".to_string()];
    
    // Add table filter if needed
    if has_table_filter {
        conditions.push("    AND t.relname = $2".to_string());
    }
    
    // Add index type filter
    match index_type {
        IndexFilterType::Btree => {
            conditions.push("    AND x.indisprimary = false".to_string());
            conditions.push("    AND x.indisunique = false".to_string());
            conditions.push("    AND am.amname = 'btree'".to_string());
        }
        IndexFilterType::Constraint => {
            conditions.push("    AND (x.indisprimary = true OR x.indisunique = true)".to_string());
        }
        IndexFilterType::All => {
            conditions.push("    AND x.indisprimary = false".to_string());
            conditions.push("    AND x.indisunique = false".to_string());
        }
    }
    
    // Add size filters with correct parameter positions
    let size_param_start = if has_table_filter { 3 } else { 2 };
    conditions.push(format!(
        "    AND pg_relation_size(i.oid) >= (${}::bigint*1024*1024*1024)",
        size_param_start
    ));
    conditions.push(format!(
        "    AND pg_relation_size(i.oid) < (${}::bigint*1024*1024*1024)",
        size_param_start + 1
    ));
    
    // Build final query
    let where_clause = conditions.join("\n");
    format!("{}\n{}\n    ORDER BY pg_relation_size(i.oid) ASC;", base_query, where_clause)
}

pub const GET_INDEX_SIZE: &str = r#"
    SELECT pg_relation_size(i.oid) as index_size
    FROM pg_index x
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = $1 AND i.relname = $2
"#;

pub const VALIDATE_INDEX_INTEGRITY: &str = r#"
    SELECT
        c.relname AS index_name,
        t.relname AS table_name,
        i.indisvalid,
        i.indisready,
        i.indislive
    FROM
        pg_class c
    JOIN
        pg_index i ON i.indexrelid = c.oid
    JOIN
        pg_class t ON i.indrelid = t.oid
    JOIN
        pg_namespace n ON n.oid = t.relnamespace
    WHERE
        c.relkind = 'i'
        AND n.nspname = $1
        AND c.relname = $2
"#;

// Get the inactive replication slot count. The name of the slot is not important in this case since it can cause replication lags or problems if reindexing produces a lot of WALs.
pub const GET_INACTIVE_REPLICATION_SLOT_COUNT: &str = r#"
    SELECT COUNT(*) FROM pg_replication_slots WHERE active = false;
"#;

// Get if there is any sync replication connection.
pub const GET_SYNC_REPLICATION_CONNECTION_COUNT: &str = r#"
    SELECT COUNT(*) FROM pg_stat_replication WHERE sync_state = 'sync';
"#;

// Session parameter queries
pub const SET_STATEMENT_TIMEOUT: &str = "SET statement_timeout TO 0";
pub const SET_IDLE_SESSION_TIMEOUT: &str = "SET idle_session_timeout TO 0";
pub const SET_APPLICATION_NAME: &str = "SET application_name TO 'reindexer'";
pub const SET_LOG_STATEMENTS: &str = "SET log_statement TO 'all'";

// Get max_parallel_workers setting
pub const GET_MAX_PARALLEL_WORKERS: &str = "SHOW max_parallel_workers";

// Get bloat ratio for a specific index
pub const GET_INDEX_BLOAT_RATIO: &str = r#"
    WITH specific_index AS (
        SELECT 
            idx.schemaname,
            idx.tablename,
            idx.indexname,
            idx.indexdef,
            pg_relation_size(idx.indexname::regclass) as index_size_bytes,
            pg_relation_size(idx.tablename::regclass) as table_size_bytes,
            idx_cls.relpages as index_pages,
            tbl_cls.relpages as table_pages,
            idx_cls.reltuples as index_tuples,
            tbl_cls.reltuples as table_tuples,
            am.amname as index_type
        FROM pg_indexes idx
        JOIN pg_class idx_cls ON idx.indexname = idx_cls.relname
        JOIN pg_class tbl_cls ON idx.tablename = tbl_cls.relname
        JOIN pg_am am ON idx_cls.relam = am.oid
        WHERE idx.indexname = $1
    )
    SELECT 
        CASE 
            WHEN table_pages > 0 AND index_pages > 0 THEN
                ROUND(
                    (((index_pages::numeric / table_pages::numeric) * 100) - 
                     ((index_size_bytes::numeric / table_size_bytes::numeric) * 100)), 2
                )::text
            ELSE '0'
        END as bloat_percentage
    FROM specific_index;
"#;

// Get the current temp_file_limit setting
pub const GET_TEMP_FILE_LIMIT: &str =
    "SELECT setting FROM pg_settings WHERE name='temp_file_limit'";

// Check if a schema exists
pub const CHECK_SCHEMA_EXISTS: &str = r#"
    SELECT EXISTS(
        SELECT 1 
        FROM pg_namespace 
        WHERE nspname = $1
    );
"#;

// Check if a table exists in a specific schema
pub const CHECK_TABLE_EXISTS: &str = r#"
    SELECT EXISTS(
        SELECT 1 
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 
        AND n.nspname = $2
        AND c.relkind = 'r'
    );
"#;
