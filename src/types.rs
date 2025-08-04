

#[derive(Debug)]
pub struct IndexInfo {
    pub schema_name: String,
    pub index_name: String,
    pub index_type: String,
}

#[derive(Debug, Clone)]
pub struct ReindexingCheckResults {
    pub active_vacuum: bool,
    pub active_pgreindexer: bool,
    pub inactive_replication_slots: bool,
    pub sync_replication_connection: bool,
}

#[derive(Debug)]
pub struct SharedTableTracker {
    pub tables_being_reindexed: std::collections::HashMap<String, String>, // table_name -> index_name
}

impl SharedTableTracker {
    pub fn new() -> Self {
        Self {
            tables_being_reindexed: std::collections::HashMap::new(),
        }
    }
} 