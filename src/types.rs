#[derive(Debug)]
pub struct IndexInfo {
    pub schema_name: String,
    pub index_name: String,
    pub index_type: String,
}

#[derive(Debug, Clone)]
pub struct ReindexingCheckResults {
    pub active_vacuum: bool,
    pub inactive_replication_slots: bool,
    pub sync_replication_connection: bool,
}

#[derive(Debug, Clone)]
pub enum ReindexStatus {
    InvalidIndex,
    BelowBloatThreshold,
    Skipped,
    ValidationFailed,
    Failed,
    Success,
}

impl std::fmt::Display for ReindexStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReindexStatus::InvalidIndex => write!(f, "invalid_index"),
            ReindexStatus::BelowBloatThreshold => write!(f, "below_bloat_threshold"),
            ReindexStatus::Skipped => write!(f, "skipped"),
            ReindexStatus::ValidationFailed => write!(f, "validation_failed"),
            ReindexStatus::Failed => write!(f, "failed"),
            ReindexStatus::Success => write!(f, "success"),
        }
    }
}

#[derive(Debug)]
pub struct SharedTableTracker {
    pub tables_being_reindexed: std::collections::HashSet<String>, // table_name
}

impl SharedTableTracker {
    pub fn new() -> Self {
        Self {
            tables_being_reindexed: std::collections::HashSet::new(),
        }
    }
}
