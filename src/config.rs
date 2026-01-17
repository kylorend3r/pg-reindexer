/// Application-wide configuration constants and settings

// Connection defaults
pub const DEFAULT_POSTGRES_HOST: &str = "localhost";
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;
pub const DEFAULT_POSTGRES_DATABASE: &str = "postgres";
pub const DEFAULT_POSTGRES_USERNAME: &str = "postgres";

// Thread and worker limits
pub const MAX_THREAD_COUNT: usize = 32;
pub const DEFAULT_POSTGRES_MAINTENANCE_WORKERS: u64 = 2;

// Maintenance work memory limits
pub const MAX_MAINTENANCE_WORK_MEM_GB: u64 = 32;

// Parallel maintenance workers
pub const PARALLEL_WORKERS_SAFETY_DIVISOR: u64 = 2; // max_parallel_maintenance_workers must be < max_parallel_workers / 2

// Maintenance IO concurrency
pub const MAX_MAINTENANCE_IO_CONCURRENCY: u64 = 512;

// Timeouts and delays
pub const MILLISECONDS_PER_SECOND: u64 = 1000;
pub const DEFAULT_DEADLOCK_TIMEOUT: &str = "1s";
pub const DEFAULT_RETRY_DELAY_MS: u64 = 100;

// Retry configuration for transient errors
pub const MAX_REINDEX_RETRIES: u32 = 3; // Maximum number of retry attempts (4 total: 1 initial + 3 retries)
pub const REINDEX_RETRY_DELAY_SECS: u64 = 2; // Delay in seconds between retry attempts

// Bloat threshold limits
pub const MIN_BLOAT_THRESHOLD_PERCENTAGE: u8 = 0;
pub const MAX_BLOAT_THRESHOLD_PERCENTAGE: u8 = 100;


/// Calculate the effective number of maintenance workers
/// Returns the default PostgreSQL value (2) if max_parallel_maintenance_workers is 0
pub fn effective_maintenance_workers(max_parallel_maintenance_workers: u64) -> u64 {
    if max_parallel_maintenance_workers == 0 {
        DEFAULT_POSTGRES_MAINTENANCE_WORKERS
    } else {
        max_parallel_maintenance_workers
    }
}
