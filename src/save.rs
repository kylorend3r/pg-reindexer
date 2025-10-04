use crate::types::ReindexStatus;
use anyhow::Result;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct IndexData {
    pub schema_name: String,
    pub index_name: String,
    pub index_type: String,
    pub reindex_status: ReindexStatus,
    pub before_size: Option<i64>,
    pub after_size: Option<i64>,
    pub size_change: Option<i64>,
    pub reindex_duration: Option<f32>,
}

pub async fn save_index_info(client: &Client, index_data: &IndexData) -> Result<()> {
    let query = r#"
        INSERT INTO reindexer.reindex_logbook (schema_name, index_name, index_type, reindex_status, before_size, after_size, size_change, reindex_duration)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    "#;

    // need some details about the error.
    let result = client
        .execute(
            query,
            &[
                &index_data.schema_name,
                &index_data.index_name,
                &index_data.index_type,
                &index_data.reindex_status.to_string(),
                &index_data.before_size,
                &index_data.after_size,
                &index_data.size_change,
                &index_data.reindex_duration,
            ],
        )
        .await;
    if let Err(e) = result {
        println!("Failed to save index info: {:?}", e);
        return Err(e.into());
    }

    Ok(())
}
