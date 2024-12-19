use crate::task_processor::{Processor, ProcessorMetrics};
use std::{sync::Arc, time::Duration};
use tokio::sync::{watch, Semaphore};
use tracing::{debug, info, instrument, warn};

pub struct TaskRunner;

impl TaskRunner {
    #[instrument(skip(processor, shutdown_rx, metrics))]
    pub async fn run<P: Processor + 'static>(
        processor: Arc<P>,
        shutdown_rx: watch::Receiver<bool>,
        num_workers: usize,
        poll_interval_secs: u64,
        metrics: ProcessorMetrics,
    ) {
        let semaphore = Arc::new(Semaphore::new(num_workers));
        let poll_interval = Duration::from_secs(poll_interval_secs);

        loop {
            if *shutdown_rx.borrow() {
                info!(task = processor.name(), "Shutting down...");
                break;
            }

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(task = processor.name(), "All workers busy; retrying...");
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

            let processor = processor.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let mut backoff = Duration::from_secs(1);

                loop {
                    let result = processor.process_record(&metrics).await;

                    if let Err(e) = result {
                        warn!(
                            task = %processor.name(),
                            error = %e,
                            "Processing failed; retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                    } else {
                        break;
                    }
                }

                drop(permit); // Release semaphore permit
            });
        }
    }
}
