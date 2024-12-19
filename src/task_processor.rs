use async_trait::async_trait;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::{Counter, Histogram};
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

#[derive(Clone)]
pub struct ProcessorMetrics {
    pub records_processed: Counter<u64>,
    pub records_failed: Counter<u64>,
    pub retries_exceeded: Counter<u64>,
    pub processing_duration: Histogram<f64>,
}

impl ProcessorMetrics {
    pub fn new(meter: &Meter, name: &str) -> Self {
        Self {
            records_processed: meter
                .u64_counter(format!("{}_records_processed_total", name))
                .build(),
            records_failed: meter
                .u64_counter(format!("{}_records_failed_total", name))
                .build(),
            retries_exceeded: meter
                .u64_counter(format!("{}_retries_exceeded_total", name))
                .build(),
            processing_duration: meter
                .f64_histogram(format!("{}_processing_duration_seconds", name))
                .build(),
        }
    }
}

#[async_trait]
pub trait Processor: Send + Sync {
    fn name(&self) -> &'static str;

    async fn process_record(&self, metrics: &ProcessorMetrics) -> Result<(), sqlx::Error>;
}

pub struct OrderRequestsProcessor {
    pool: Arc<PgPool>,
    max_retries: u32,
}

impl OrderRequestsProcessor {
    pub fn new(pool: Arc<PgPool>, max_retries: u32) -> Self {
        Self { pool, max_retries }
    }

    async fn handle_success(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: uuid::Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE order_requests 
               SET state = 'processed', failed_attempts = 0
               WHERE id = $1"#,
            id
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn handle_failure(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: uuid::Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE order_requests 
               SET failed_attempts = failed_attempts + 1 
               WHERE id = $1"#,
            id
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn handle_exceeded_retries(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: uuid::Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE order_requests 
               SET state = 'failed' 
               WHERE id = $1"#,
            id
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Processor for OrderRequestsProcessor {
    fn name(&self) -> &'static str {
        "OrderRequestsTask"
    }

    #[instrument(skip(self, metrics))]
    async fn process_record(&self, metrics: &ProcessorMetrics) -> Result<(), sqlx::Error> {
        let start_time = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;

        if let Some(row) = sqlx::query!(
            r#"SELECT id, failed_attempts
               FROM order_requests 
               WHERE state = 'pending'
               FOR UPDATE SKIP LOCKED LIMIT 1"#
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            let id = row.id;
            let failed_attempts = row.failed_attempts;

            info!(id = %id, "Processing order...");

            if failed_attempts as u32 >= self.max_retries {
                metrics.retries_exceeded.add(1, &[]);
                self.handle_exceeded_retries(&mut tx, id).await?;
            } else {
                let result = simulate_processing(id).await;

                if result.is_ok() {
                    metrics.records_processed.add(1, &[]);
                    self.handle_success(&mut tx, id).await?;
                } else {
                    metrics.records_failed.add(1, &[]);
                    self.handle_failure(&mut tx, id).await?;
                }
            }
        } else {
            debug!("No unprocessed orders found");
        }

        metrics
            .processing_duration
            .record(start_time.elapsed().as_secs_f64(), &[]);
        tx.commit().await?;
        Ok(())
    }
}

// Simulate processing logic
async fn simulate_processing(id: uuid::Uuid) -> Result<(), sqlx::Error> {
    if rand::random::<u8>() % 2 == 0 {
        Err(sqlx::Error::RowNotFound)
    } else {
        Ok(())
    }
}
