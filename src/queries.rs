// SQL queries used throughout the application

pub const GET_RUNNING_PGREINDEXER: &str = r#"
    SELECT * FROM pg_stat_activity 
    WHERE state = 'active' 
    AND upper(query) LIKE '%pgreindexer%' 
    AND pid != pg_backend_pid();
"#;

pub const GET_ACTIVE_VACUUM: &str = r#"
    SELECT * FROM pg_stat_activity 
    WHERE state = 'active' 
    AND lower(query) LIKE 'vacuum%' 
    AND pid != pg_backend_pid();
"#;

pub const GET_INDEXES_IN_SCHEMA_WITH_TABLE: &str = r#"
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
    WHERE n.nspname = $1
    AND t.relname = $2
    AND i.relkind = 'i'
    AND x.indisprimary = false
    AND x.indisunique = false
    AND pg_relation_size(i.oid) < ($3::bigint*1024*1024*1024)
    ORDER BY pg_relation_size(i.oid) ASC;
"#;

pub const GET_INDEXES_IN_SCHEMA: &str = r#"
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
    WHERE n.nspname = $1
    AND i.relkind = 'i'
    AND x.indisprimary = false
    AND x.indisunique = false
    AND pg_relation_size(i.oid) < ($2::bigint*1024*1024*1024)
    ORDER BY pg_relation_size(i.oid) ASC;
"#;

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
