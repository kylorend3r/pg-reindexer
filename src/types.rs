#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub schema_name: String,
    pub index_name: String,
    pub index_type: String,
    pub table_name: String,
    pub size_bytes: Option<i64>,
    /// For partition indexes, this contains the parent partitioned table name
    pub parent_table_name: Option<String>,
}

/// Information about a partitioned table
#[derive(Debug, Clone)]
pub struct PartitionedTableInfo {
    pub schema_name: String,
    pub table_name: String,
    pub partition_count: i64,
}

/// Information about a table partition
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub schema_name: String,
    pub partition_name: String,
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
    /// All non-constraint indexes regardless of access method (excludes primary keys and unique indexes)
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

/// Log output format (text or JSON)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Text => write!(f, "text"),
            LogFormat::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
            _ => Err(format!(
                "Invalid log format '{}'. Must be one of: 'text', 'json'",
                s
            )),
        }
    }
}

/// PostgreSQL session-level log_statement setting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogStatement {
    #[default]
    All,
    None,
    Ddl,
    Mod,
}

impl LogStatement {
    pub fn as_pg_value(&self) -> &str {
        match self {
            LogStatement::All => "all",
            LogStatement::None => "none",
            LogStatement::Ddl => "ddl",
            LogStatement::Mod => "mod",
        }
    }
}

impl std::fmt::Display for LogStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_pg_value())
    }
}

impl std::str::FromStr for LogStatement {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" => Ok(LogStatement::All),
            "none" => Ok(LogStatement::None),
            "ddl" => Ok(LogStatement::Ddl),
            "mod" => Ok(LogStatement::Mod),
            _ => Err(format!(
                "Invalid log_statement value '{}'. Must be one of: all, none, ddl, mod",
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

    /// Marks all remaining Pending entries as Skipped.
    /// Safe to call only after all workers have returned (no active writers).
    pub fn mark_all_pending_skipped(&mut self) {
        for entry in self.indexes.values_mut() {
            if entry.status == IndexStatus::Pending {
                entry.status = IndexStatus::Skipped;
            }
        }
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

/// Ranking criteria for the plan subcommand
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBy {
    Bloat,
    Size,
    ScanFrequency,
    Age,
}

impl std::str::FromStr for SortBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "bloat" => Ok(SortBy::Bloat),
            "size" => Ok(SortBy::Size),
            "scan-frequency" => Ok(SortBy::ScanFrequency),
            "age" => Ok(SortBy::Age),
            other => Err(format!(
                "Invalid sort criterion '{}'. Valid: bloat, size, scan-frequency, age",
                other
            )),
        }
    }
}

/// Output format for the plan subcommand
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlanOutputFormat {
    #[default]
    Json,
    Csv,
}

impl std::str::FromStr for PlanOutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(PlanOutputFormat::Json),
            "csv" => Ok(PlanOutputFormat::Csv),
            other => Err(format!(
                "Invalid output format '{}'. Valid: json, csv",
                other
            )),
        }
    }
}

/// Index entry produced by the plan subcommand ranked worklist
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlanIndexInfo {
    pub rank: usize,
    pub schema_name: String,
    pub table_name: String,
    pub index_name: String,
    pub index_type: String,
    pub size_bytes: i64,
    pub size_pretty: String,
    pub scan_count: i64,
    /// ISO-8601 timestamp of the last index scan, or null when never scanned
    pub last_scan: Option<String>,
    pub estimated_bloat_percent: f64,
    /// Epoch seconds used internally for Age ranking; excluded from serialized output
    #[serde(skip)]
    pub last_idx_scan_epoch_secs: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::LogStatement;

    #[test]
    fn log_statement_default_is_all() {
        assert_eq!(LogStatement::default(), LogStatement::All);
    }

    #[test]
    fn log_statement_from_str_round_trips() {
        for (input, expected) in [
            ("all", LogStatement::All),
            ("none", LogStatement::None),
            ("ddl", LogStatement::Ddl),
            ("mod", LogStatement::Mod),
        ] {
            let parsed: LogStatement = input.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_pg_value(), input);
        }
    }

    #[test]
    fn log_statement_from_str_case_insensitive() {
        assert_eq!("ALL".parse::<LogStatement>().unwrap(), LogStatement::All);
        assert_eq!("DDL".parse::<LogStatement>().unwrap(), LogStatement::Ddl);
        assert_eq!("MOD".parse::<LogStatement>().unwrap(), LogStatement::Mod);
        assert_eq!("NONE".parse::<LogStatement>().unwrap(), LogStatement::None);
    }

    #[test]
    fn log_statement_invalid_returns_error() {
        assert!("dml".parse::<LogStatement>().is_err());
        assert!("garbage".parse::<LogStatement>().is_err());
        assert!("".parse::<LogStatement>().is_err());
    }

    #[test]
    fn log_statement_display_matches_pg_value() {
        assert_eq!(LogStatement::All.to_string(), "all");
        assert_eq!(LogStatement::None.to_string(), "none");
        assert_eq!(LogStatement::Ddl.to_string(), "ddl");
        assert_eq!(LogStatement::Mod.to_string(), "mod");
    }
}
