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
    AND upper(query) LIKE '%VACUUM%' 
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

// Session parameter queries
pub const SET_STATEMENT_TIMEOUT: &str = "SET statement_timeout TO 0";
pub const SET_IDLE_SESSION_TIMEOUT: &str = "SET idle_session_timeout TO 0";
pub const SET_APPLICATION_NAME: &str = "SET application_name TO 'reindexer'";
