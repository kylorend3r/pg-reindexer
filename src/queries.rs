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
/// - `order_by_size`: Optional ordering by size: `Some("asc")` for ascending, `Some("desc")` for descending, `None` for default ascending
/// 
/// # Returns
/// The constructed SQL query string
pub fn build_indexes_query(has_table_filter: bool, index_type: IndexFilterType, order_by_size: Option<&str>) -> String {
    // Base query structure (common to all variants)
    let base_query = r#"
        SELECT 
            n.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            ROUND(pg_relation_size(i.oid)::numeric/(1024*1024*1024), 2) || ' GB' as index_size,
            am.amname as index_type,
            pg_relation_size(i.oid) as size_bytes
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
    
    // Build ORDER BY clause based on order_by_size parameter
    let order_clause = match order_by_size {
        Some("asc") => "    ORDER BY pg_relation_size(i.oid) ASC",
        Some("desc") => "    ORDER BY pg_relation_size(i.oid) DESC",
        _ => "    ORDER BY pg_relation_size(i.oid) ASC", // Default to ASC for backward compatibility
    };
    
    // Build final query
    let where_clause = conditions.join("\n");
    format!("{}\n{}\n{};", base_query, where_clause, order_clause)
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

// Get all user schemas (excluding system schemas and tool-managed schemas)
pub const GET_ALL_USER_SCHEMAS: &str = r#"
    SELECT nspname
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'reindexer')
    AND nspname NOT LIKE 'pg_temp_%'
    AND nspname NOT LIKE 'pg_toast_temp_%'
    ORDER BY nspname;
"#;

// Check if a table is partitioned (has child partitions)
pub const CHECK_TABLE_IS_PARTITIONED: &str = r#"
    SELECT EXISTS(
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1
        AND n.nspname = $2
        AND c.relkind = 'p'
    );
"#;

// Get all partitions of a partitioned table
pub const GET_TABLE_PARTITIONS: &str = r#"
    SELECT
        child_ns.nspname as partition_schema,
        child.relname as partition_name
    FROM pg_inherits i
    JOIN pg_class parent ON parent.oid = i.inhparent
    JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
    JOIN pg_class child ON child.oid = i.inhrelid
    JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
    WHERE parent.relname = $1
    AND parent_ns.nspname = $2
    AND parent.relkind = 'p'
    ORDER BY child.relname;
"#;

// Get indexes for partitioned tables (includes partition indexes)
// This query retrieves indexes from all partitions of a partitioned table
pub const GET_PARTITIONED_TABLE_INDEXES: &str = r#"
    WITH partition_tables AS (
        SELECT
            child_ns.nspname as partition_schema,
            child.relname as partition_name,
            child.oid as partition_oid
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        WHERE parent.relname = $1
        AND parent_ns.nspname = $2
        AND parent.relkind = 'p'
    )
    SELECT
        pt.partition_schema as schema_name,
        pt.partition_name as table_name,
        i.relname as index_name,
        ROUND(pg_relation_size(i.oid)::numeric/(1024*1024*1024), 2) || ' GB' as index_size,
        am.amname as index_type,
        pg_relation_size(i.oid) as size_bytes,
        $1 as parent_table_name
    FROM partition_tables pt
    JOIN pg_index x ON x.indrelid = pt.partition_oid
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_am am ON am.oid = i.relam
    WHERE i.relkind = 'i'
    AND x.indisprimary = false
    AND x.indisunique = false
    ORDER BY pt.partition_name, pg_relation_size(i.oid) ASC;
"#;

// Check if any tables in a schema are partitioned
pub const GET_PARTITIONED_TABLES_IN_SCHEMA: &str = r#"
    SELECT
        n.nspname as schema_name,
        c.relname as table_name,
        (SELECT COUNT(*) FROM pg_inherits WHERE inhparent = c.oid) as partition_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1
    AND c.relkind = 'p'
    ORDER BY c.relname;
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_table_is_partitioned_query_format() {
        // Verify the CHECK_TABLE_IS_PARTITIONED query contains expected elements
        assert!(
            CHECK_TABLE_IS_PARTITIONED.contains("pg_inherits"),
            "Query should use pg_inherits to detect partitioned tables"
        );
        assert!(
            CHECK_TABLE_IS_PARTITIONED.contains("relkind = 'p'"),
            "Query should filter for partitioned tables (relkind = 'p')"
        );
    }

    #[test]
    fn test_get_table_partitions_query_format() {
        // Verify the GET_TABLE_PARTITIONS query contains expected elements
        assert!(
            GET_TABLE_PARTITIONS.contains("pg_inherits"),
            "Query should use pg_inherits to find partitions"
        );
        assert!(
            GET_TABLE_PARTITIONS.contains("partition_schema"),
            "Query should return partition schema"
        );
        assert!(
            GET_TABLE_PARTITIONS.contains("partition_name"),
            "Query should return partition name"
        );
    }

    #[test]
    fn test_get_partitioned_table_indexes_query_format() {
        // Verify the GET_PARTITIONED_TABLE_INDEXES query contains expected elements
        assert!(
            GET_PARTITIONED_TABLE_INDEXES.contains("partition_tables"),
            "Query should use CTE for partition_tables"
        );
        assert!(
            GET_PARTITIONED_TABLE_INDEXES.contains("parent_table_name"),
            "Query should return parent_table_name"
        );
        assert!(
            GET_PARTITIONED_TABLE_INDEXES.contains("size_bytes"),
            "Query should return size_bytes"
        );
    }

    #[test]
    fn test_get_partitioned_tables_in_schema_query_format() {
        // Verify the GET_PARTITIONED_TABLES_IN_SCHEMA query contains expected elements
        assert!(
            GET_PARTITIONED_TABLES_IN_SCHEMA.contains("relkind = 'p'"),
            "Query should filter for partitioned tables"
        );
        assert!(
            GET_PARTITIONED_TABLES_IN_SCHEMA.contains("partition_count"),
            "Query should return partition count"
        );
    }

    #[test]
    fn test_build_indexes_query_ascending_order() {
        let query = build_indexes_query(false, IndexFilterType::Btree, Some("asc"));
        assert!(
            query.contains("ORDER BY pg_relation_size(i.oid) ASC"),
            "Query should contain ASC ordering clause"
        );
        assert!(
            query.contains("size_bytes"),
            "Query should include size_bytes column"
        );
    }

    #[test]
    fn test_build_indexes_query_descending_order() {
        let query = build_indexes_query(false, IndexFilterType::Btree, Some("desc"));
        assert!(
            query.contains("ORDER BY pg_relation_size(i.oid) DESC"),
            "Query should contain DESC ordering clause"
        );
        assert!(
            query.contains("size_bytes"),
            "Query should include size_bytes column"
        );
    }

    #[test]
    fn test_build_indexes_query_default_ordering() {
        let query = build_indexes_query(false, IndexFilterType::Btree, None);
        assert!(
            query.contains("ORDER BY pg_relation_size(i.oid) ASC"),
            "Query should default to ASC ordering"
        );
        assert!(
            query.contains("size_bytes"),
            "Query should include size_bytes column"
        );
    }

    #[test]
    fn test_build_indexes_query_with_table_filter() {
        let query = build_indexes_query(true, IndexFilterType::Btree, Some("asc"));
        assert!(
            query.contains("t.relname = $2"),
            "Query should include table filter when has_table_filter is true"
        );
        assert!(
            query.contains("ORDER BY pg_relation_size(i.oid) ASC"),
            "Query should contain ordering clause"
        );
    }

    #[test]
    fn test_build_indexes_query_includes_size_bytes() {
        // Test that all variants include size_bytes in SELECT
        let variants = vec![
            (false, IndexFilterType::Btree, Some("asc")),
            (false, IndexFilterType::Btree, Some("desc")),
            (false, IndexFilterType::Btree, None),
            (true, IndexFilterType::Constraint, Some("asc")),
            (false, IndexFilterType::All, Some("desc")),
        ];

        for (has_table_filter, index_type, order_by_size) in variants {
            let query = build_indexes_query(has_table_filter, index_type, order_by_size);
            assert!(
                query.contains("pg_relation_size(i.oid) as size_bytes"),
                "Query should include size_bytes column regardless of filters"
            );
        }
    }
}
