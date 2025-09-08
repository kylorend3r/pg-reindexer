use crate::types::{IndexInfo, IndexMemoryTable, IndexStatus};
use crate::logging;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Thread-safe wrapper around IndexMemoryTable
#[derive(Debug)]
pub struct SharedIndexMemoryTable {
    inner: Arc<Mutex<IndexMemoryTable>>,
}

impl SharedIndexMemoryTable {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(IndexMemoryTable::new())),
        }
    }

    /// Initialize the memory table with indexes from database
    pub async fn initialize_with_indexes(&self, indexes: Vec<IndexInfo>) {
        let mut table = self.inner.lock().await;
        for index in indexes {
            table.add_index(index);
        }
    }

    /// Try to acquire a lock on a table and assign an index to a worker
    pub async fn try_acquire_index_for_worker(
        &self,
        worker_id: usize,
        logger: &logging::Logger,
    ) -> Option<IndexInfo> {
        let mut table = self.inner.lock().await;
        
        // Find a pending index whose table is not locked
        let mut candidate_index_key = None;
        
        // First pass: find a candidate index
        for (index_key, entry) in &table.indexes {
            if entry.status == IndexStatus::Pending {
                let table_key = table.get_table_key(&entry.index_info.schema_name, &entry.index_info.table_name);
                
                // Check if table is already locked
                if !table.table_locks.contains_key(&table_key) {
                    candidate_index_key = Some(index_key.clone());
                    break;
                }
            }
        }
        
        // Second pass: try to acquire the candidate
        if let Some(index_key) = candidate_index_key {
            // Clone the necessary data to avoid borrowing issues
            let entry_data = if let Some(entry) = table.indexes.get(&index_key) {
                Some((
                    entry.index_info.schema_name.clone(),
                    entry.index_info.index_name.clone(),
                    entry.index_info.table_name.clone(),
                    entry.index_info.clone(),
                ))
            } else {
                None
            };
            
            if let Some((schema_name, index_name, table_name, index_info)) = entry_data {
                // Try to lock the table and assign the index
                if table.lock_table(&schema_name, &table_name, worker_id) {
                    if table.assign_index_to_worker(&schema_name, &index_name, worker_id) {
                        logger.log(
                            logging::LogLevel::Info,
                            &format!(
                                "Worker {} acquired index {}.{} (table: {}.{})",
                                worker_id, schema_name, index_name, schema_name, table_name
                            ),
                        );
                        return Some(index_info);
                    } else {
                        // Failed to assign, unlock table
                        table.unlock_table(&schema_name, &table_name);
                    }
                }
            }
        }
        
        None
    }

    /// Release a table lock and mark index as completed
    pub async fn release_index_and_mark_completed(
        &self,
        schema_name: &str,
        index_name: &str,
        table_name: &str,
        worker_id: usize,
        logger: &logging::Logger,
    ) {
        let mut table = self.inner.lock().await;
        
        // Mark index as completed
        table.mark_index_completed(schema_name, index_name);
        
        // Unlock the table
        table.unlock_table(schema_name, table_name);
        
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Worker {} completed and released index {}.{} (table: {}.{})",
                worker_id, schema_name, index_name, schema_name, table_name
            ),
        );
    }

    /// Release a table lock and mark index as failed
    pub async fn release_index_and_mark_failed(
        &self,
        schema_name: &str,
        index_name: &str,
        table_name: &str,
        worker_id: usize,
        logger: &logging::Logger,
    ) {
        let mut table = self.inner.lock().await;
        
        // Mark index as failed
        table.mark_index_failed(schema_name, index_name);
        
        // Unlock the table
        table.unlock_table(schema_name, table_name);
        
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Worker {} failed and released index {}.{} (table: {}.{})",
                worker_id, schema_name, index_name, schema_name, table_name
            ),
        );
    }

    /// Release a table lock and mark index as skipped
    pub async fn release_index_and_mark_skipped(
        &self,
        schema_name: &str,
        index_name: &str,
        table_name: &str,
        worker_id: usize,
        logger: &logging::Logger,
    ) {
        let mut table = self.inner.lock().await;
        
        // Mark index as skipped
        table.mark_index_skipped(schema_name, index_name);
        
        // Unlock the table
        table.unlock_table(schema_name, table_name);
        
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Worker {} skipped and released index {}.{} (table: {}.{})",
                worker_id, schema_name, index_name, schema_name, table_name
            ),
        );
    }

    /// Get current statistics
    pub async fn get_statistics(&self) -> (usize, usize, usize, usize, usize) {
        let table = self.inner.lock().await;
        table.get_statistics()
    }

    /// Check if there are any pending indexes
    pub async fn has_pending_indexes(&self) -> bool {
        let table = self.inner.lock().await;
        !table.get_pending_indexes().is_empty()
    }

}

impl Clone for SharedIndexMemoryTable {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
