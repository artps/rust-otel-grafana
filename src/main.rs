mod task_processor;
mod task_runner;
mod telemetry;

use sqlx::PgPool;
use std::sync::Arc;
use task_processor::{OrderRequestsProcessor, ProcessorMetrics};
use task_runner::TaskRunner;
use telemetry::init_tracing_subscriber;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let _guard = init_tracing_subscriber();

    let db_url = "postgres://localhost/foo";
    let pool = Arc::new(PgPool::connect(db_url).await?);

    let max_retries = 5;
    let poll_interval_secs = 5;
    let num_workers = 4;

    // Initialize shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Initialize metrics
    let meter = opentelemetry::global::meter("order_requests_processor");
    let metrics = ProcessorMetrics::new(&meter, "order_requests");
    let order_requests_processor = Arc::new(OrderRequestsProcessor::new(pool.clone(), max_retries));

    // Run tasks
    tokio::spawn(async move {
        TaskRunner::run(
            order_requests_processor,
            shutdown_rx.clone(),
            num_workers,
            poll_interval_secs,
            metrics.clone(),
        )
        .await;
    });

    // Gracefully handle shutdown
    tokio::signal::ctrl_c().await?;
    shutdown_tx.send(true)?;

    println!("Shutdown signal received. Exiting...");
    Ok(())
}
