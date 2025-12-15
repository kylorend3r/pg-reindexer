/// Application-wide configuration constants and settings

// Connection defaults
pub const DEFAULT_POSTGRES_HOST: &str = "localhost";
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;
pub const DEFAULT_POSTGRES_DATABASE: &str = "postgres";
pub const DEFAULT_POSTGRES_USERNAME: &str = "postgres";

// Thread and worker limits
pub const MAX_THREAD_COUNT: usize = 32;
pub const DEFAULT_THREAD_COUNT: usize = 2;
pub const DEFAULT_POSTGRES_MAINTENANCE_WORKERS: u64 = 2;

// Maintenance work memory limits
pub const MAX_MAINTENANCE_WORK_MEM_GB: u64 = 32;
pub const DEFAULT_MAINTENANCE_WORK_MEM_GB: u64 = 1;

// Parallel maintenance workers
pub const DEFAULT_MAX_PARALLEL_MAINTENANCE_WORKERS: u64 = 2;
pub const PARALLEL_WORKERS_SAFETY_DIVISOR: u64 = 2; // max_parallel_maintenance_workers must be < max_parallel_workers / 2

// Maintenance IO concurrency
pub const MAX_MAINTENANCE_IO_CONCURRENCY: u64 = 512;
pub const DEFAULT_MAINTENANCE_IO_CONCURRENCY: u64 = 10;

// Timeouts and delays
pub const MILLISECONDS_PER_SECOND: u64 = 1000;
pub const DEFAULT_DEADLOCK_TIMEOUT: &str = "1s";
pub const DEFAULT_RETRY_DELAY_MS: u64 = 100;

// Bloat threshold limits
pub const MIN_BLOAT_THRESHOLD_PERCENTAGE: u8 = 0;
pub const MAX_BLOAT_THRESHOLD_PERCENTAGE: u8 = 100;

// Index size defaults
pub const DEFAULT_MAX_INDEX_SIZE_GB: u64 = 1024;
pub const DEFAULT_MIN_INDEX_SIZE_GB: u64 = 0;

// Index type defaults
pub const DEFAULT_INDEX_TYPE: crate::types::IndexFilterType = crate::types::IndexFilterType::Btree;

// Lock timeout defaults
pub const DEFAULT_LOCK_TIMEOUT_SECONDS: u64 = 0; // 0 = no timeout

/// Application configuration structure
/// This struct holds all application-wide settings that can be derived from command-line arguments
/// Note: Currently only partially used, but kept for potential future refactoring
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AppConfig {
    // Connection settings
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: Option<String>,

    // Thread and worker settings
    pub threads: usize,
    pub max_parallel_maintenance_workers: u64,

    // Maintenance settings
    pub maintenance_work_mem_gb: u64,
    pub maintenance_io_concurrency: u64,
    pub lock_timeout_seconds: u64,

    // Index filtering settings
    pub min_size_gb: u64,
    pub max_size_gb: u64,
    pub index_type: crate::types::IndexFilterType,
    pub bloat_threshold: Option<u8>,

    // SSL settings
    pub ssl: bool,
    pub ssl_self_signed: bool,
    pub ssl_ca_cert: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,

    // Behavior flags
    pub concurrently: bool,
    pub skip_inactive_replication_slots: bool,
    pub skip_sync_replication_connection: bool,
    pub skip_active_vacuums: bool,
    pub clean_orphaned_indexes: bool,
    pub resume: bool,
    pub silence_mode: bool,
}

impl AppConfig {
    /// Create a new AppConfig with default values
    pub fn new() -> Self {
        Self {
            host: DEFAULT_POSTGRES_HOST.to_string(),
            port: DEFAULT_POSTGRES_PORT,
            database: DEFAULT_POSTGRES_DATABASE.to_string(),
            username: DEFAULT_POSTGRES_USERNAME.to_string(),
            password: None,
            threads: DEFAULT_THREAD_COUNT,
            max_parallel_maintenance_workers: DEFAULT_MAX_PARALLEL_MAINTENANCE_WORKERS,
            maintenance_work_mem_gb: DEFAULT_MAINTENANCE_WORK_MEM_GB,
            maintenance_io_concurrency: DEFAULT_MAINTENANCE_IO_CONCURRENCY,
            lock_timeout_seconds: DEFAULT_LOCK_TIMEOUT_SECONDS,
            min_size_gb: DEFAULT_MIN_INDEX_SIZE_GB,
            max_size_gb: DEFAULT_MAX_INDEX_SIZE_GB,
            index_type: DEFAULT_INDEX_TYPE,
            bloat_threshold: None,
            ssl: false,
            ssl_self_signed: false,
            ssl_ca_cert: None,
            ssl_client_cert: None,
            ssl_client_key: None,
            concurrently: false,
            skip_inactive_replication_slots: false,
            skip_sync_replication_connection: false,
            skip_active_vacuums: false,
            clean_orphaned_indexes: false,
            resume: false,
            silence_mode: false,
        }
    }

}

/// Calculate the effective number of maintenance workers
/// Returns the default PostgreSQL value (2) if max_parallel_maintenance_workers is 0
pub fn effective_maintenance_workers(max_parallel_maintenance_workers: u64) -> u64 {
    if max_parallel_maintenance_workers == 0 {
        DEFAULT_POSTGRES_MAINTENANCE_WORKERS
    } else {
        max_parallel_maintenance_workers
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}
