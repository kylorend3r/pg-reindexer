#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub schema_name: String,
    pub index_name: String,
    pub index_type: String,
    pub table_name: String,
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
    Excluded,
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
            ReindexStatus::Excluded => write!(f, "excluded"),
            ReindexStatus::ValidationFailed => write!(f, "validation_failed"),
            ReindexStatus::Failed => write!(f, "failed"),
            ReindexStatus::Success => write!(f, "success"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Skipped,
}

impl std::fmt::Display for IndexStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexStatus::Pending => write!(f, "pending"),
            IndexStatus::InProgress => write!(f, "in_progress"),
            IndexStatus::Completed => write!(f, "completed"),
            IndexStatus::Failed => write!(f, "failed"),
            IndexStatus::Skipped => write!(f, "skipped"),
        }
    }
}

/// Index filter type for selecting which indexes to reindex
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexFilterType {
    /// Filter for b-tree indexes only
    Btree,
    /// Filter for constraint indexes (primary keys and unique constraints) only
    Constraint,
    /// Include all index types
    All,
}

impl std::fmt::Display for IndexFilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexFilterType::Btree => write!(f, "btree"),
            IndexFilterType::Constraint => write!(f, "constraint"),
            IndexFilterType::All => write!(f, "all"),
        }
    }
}

impl std::str::FromStr for IndexFilterType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "btree" => Ok(IndexFilterType::Btree),
            "constraint" => Ok(IndexFilterType::Constraint),
            "all" | "" => Ok(IndexFilterType::All),
            _ => Err(format!(
                "Invalid index filter type '{}'. Must be one of: 'btree', 'constraint', 'all'",
                s
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub index_info: IndexInfo,
    pub status: IndexStatus,
    pub assigned_worker: Option<usize>,
    pub lock_acquired_at: Option<std::time::Instant>,
}

impl IndexEntry {
    pub fn new(index_info: IndexInfo) -> Self {
        Self {
            index_info,
            status: IndexStatus::Pending,
            assigned_worker: None,
            lock_acquired_at: None,
        }
    }
}

#[derive(Debug)]
pub struct IndexMemoryTable {
    pub indexes: std::collections::HashMap<String, IndexEntry>, // key: "schema.index"
    pub table_locks: std::collections::HashMap<String, usize>,  // table_name -> worker_id
    pub worker_assignments: std::collections::HashMap<usize, Vec<String>>, // worker_id -> list of index keys
}

impl IndexMemoryTable {
    pub fn new() -> Self {
        Self {
            indexes: std::collections::HashMap::new(),
            table_locks: std::collections::HashMap::new(),
            worker_assignments: std::collections::HashMap::new(),
        }
    }

    pub fn add_index(&mut self, index_info: IndexInfo) {
        let key = format!("{}.{}", index_info.schema_name, index_info.index_name);
        let entry = IndexEntry::new(index_info);
        self.indexes.insert(key, entry);
    }

    pub fn get_index_key(&self, schema_name: &str, index_name: &str) -> String {
        format!("{}.{}", schema_name, index_name)
    }

    pub fn get_table_key(&self, schema_name: &str, table_name: &str) -> String {
        format!("{}.{}", schema_name, table_name)
    }

    pub fn lock_table(&mut self, schema_name: &str, table_name: &str, worker_id: usize) -> bool {
        let table_key = self.get_table_key(schema_name, table_name);
        if let std::collections::hash_map::Entry::Vacant(e) = self.table_locks.entry(table_key) {
            e.insert(worker_id);
            true // Successfully locked
        } else {
            false // Table already locked
        }
    }

    pub fn unlock_table(&mut self, schema_name: &str, table_name: &str) {
        let table_key = self.get_table_key(schema_name, table_name);
        self.table_locks.remove(&table_key);
    }

    pub fn assign_index_to_worker(
        &mut self,
        schema_name: &str,
        index_name: &str,
        worker_id: usize,
    ) -> bool {
        let index_key = self.get_index_key(schema_name, index_name);

        if let Some(entry) = self.indexes.get_mut(&index_key)
            && entry.status == IndexStatus::Pending
        {
            entry.status = IndexStatus::InProgress;
            entry.assigned_worker = Some(worker_id);
            entry.lock_acquired_at = Some(std::time::Instant::now());

            // Track worker assignment
            self.worker_assignments
                .entry(worker_id)
                .or_default()
                .push(index_key.clone());
            return true;
        }
        false
    }

    pub fn mark_index_completed(&mut self, schema_name: &str, index_name: &str) {
        let index_key = self.get_index_key(schema_name, index_name);
        if let Some(entry) = self.indexes.get_mut(&index_key) {
            entry.status = IndexStatus::Completed;
        }
    }

    pub fn mark_index_failed(&mut self, schema_name: &str, index_name: &str) {
        let index_key = self.get_index_key(schema_name, index_name);
        if let Some(entry) = self.indexes.get_mut(&index_key) {
            entry.status = IndexStatus::Failed;
        }
    }

    pub fn mark_index_skipped(&mut self, schema_name: &str, index_name: &str) {
        let index_key = self.get_index_key(schema_name, index_name);
        if let Some(entry) = self.indexes.get_mut(&index_key) {
            entry.status = IndexStatus::Skipped;
        }
    }

    pub fn get_pending_indexes(&self) -> Vec<&IndexEntry> {
        self.indexes
            .values()
            .filter(|entry| entry.status == IndexStatus::Pending)
            .collect()
    }

    pub fn get_statistics(&self) -> (usize, usize, usize, usize, usize) {
        let mut pending = 0;
        let mut in_progress = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut skipped = 0;

        for entry in self.indexes.values() {
            match entry.status {
                IndexStatus::Pending => pending += 1,
                IndexStatus::InProgress => in_progress += 1,
                IndexStatus::Completed => completed += 1,
                IndexStatus::Failed => failed += 1,
                IndexStatus::Skipped => skipped += 1,
            }
        }

        (pending, in_progress, completed, failed, skipped)
    }
}
