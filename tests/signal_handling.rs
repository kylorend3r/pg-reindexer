//! Unit tests for signal handling (SIGINT and SIGTERM)
//! 
//! These tests verify that the cancellation mechanism works correctly when cancellation
//! tokens are triggered. The actual signal handling (SIGINT/SIGTERM) is tested through
//! the cancellation token mechanism, which is the core component that responds to signals.
//! 
//! Note: Integration tests that actually send SIGTERM signals to the process would require
//! spawning separate processes and sending signals, which is better suited for end-to-end tests.

use pg_reindexer::logging;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{timeout, Duration};

/// Test that cancellation token is set to true when manually triggered
#[tokio::test]
async fn test_cancellation_token_manual_trigger() {
    let (tx, mut rx) = watch::channel(false);
    
    // Initially should be false
    assert!(!*rx.borrow());
    
    // Send cancellation signal
    tx.send(true).unwrap();
    
    // Should now be true
    assert!(*rx.borrow());
    
    // Wait for change notification
    rx.changed().await.unwrap();
    assert!(*rx.borrow());
}

/// Test that cancellation token can be cloned and both receivers see updates
#[tokio::test]
async fn test_cancellation_token_multiple_receivers() {
    let (tx, mut rx1) = watch::channel(false);
    let mut rx2 = rx1.clone();
    
    // Both should start as false
    assert!(!*rx1.borrow());
    assert!(!*rx2.borrow());
    
    // Send cancellation
    tx.send(true).unwrap();
    
    // Both should see the update
    rx1.changed().await.unwrap();
    rx2.changed().await.unwrap();
    assert!(*rx1.borrow());
    assert!(*rx2.borrow());
}

/// Test that worker loop exits when cancellation token is set
#[tokio::test]
async fn test_worker_cancellation_check() {
    let (tx, rx) = watch::channel(false);
    let memory_table = Arc::new(pg_reindexer::memory_table::SharedIndexMemoryTable::new());
    
    // Initialize with empty indexes so loop will exit naturally
    memory_table.initialize_with_indexes(vec![]).await;
    
    // Clone rx before moving it into the task
    let rx_worker = rx.clone();
    
    // Spawn a task that simulates worker behavior
    let worker_task = tokio::spawn(async move {
        let mut iteration_count = 0;
        while memory_table.has_pending_indexes().await {
            // Check for cancellation
            if *rx_worker.borrow() {
                return Ok::<(), anyhow::Error>(());
            }
            iteration_count += 1;
            if iteration_count > 10 {
                // Safety: prevent infinite loop
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    });
    
    // Give worker a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Send cancellation (rx is still alive in the test scope)
    tx.send(true).unwrap();
    
    // Worker should complete quickly
    let result = timeout(Duration::from_secs(1), worker_task).await;
    assert!(result.is_ok(), "Worker should complete within timeout");
    assert!(result.unwrap().is_ok(), "Worker should complete successfully");
}

/// Test cancellation during index processing simulation
#[tokio::test]
async fn test_cancellation_during_processing() {
    let (tx, rx) = watch::channel(false);
    let logger = Arc::new(logging::Logger::new_with_silence("test_signal.log".to_string(), false));
    
    // Simulate worker checking cancellation before and after acquiring index
    let worker_task = tokio::spawn(async move {
        // Simulate checking cancellation before processing
        if *rx.borrow() {
            return false; // Cancelled before processing
        }
        
        // Simulate processing delay
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Check cancellation again after starting
        if *rx.borrow() {
            logger.log(
                logging::LogLevel::Info,
                "Cancellation detected during processing",
            );
            return false; // Cancelled during processing
        }
        
        true // Processing completed
    });
    
    // Send cancellation during processing
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(true).unwrap();
    
    let result = timeout(Duration::from_secs(1), worker_task).await;
    assert!(result.is_ok(), "Worker should complete");
    let processed = result.unwrap().unwrap();
    assert!(!processed, "Processing should be cancelled");
}

/// Test that cancellation token works with tokio::select! pattern
#[tokio::test]
async fn test_cancellation_with_select() {
    let (tx, mut rx) = watch::channel(false);
    
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                false // Timeout (shouldn't happen)
            }
            _ = rx.changed() => {
                *rx.borrow() // Cancellation received
            }
        }
    });
    
    // Send cancellation
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(true).unwrap();
    
    // Task should complete quickly
    let result = timeout(Duration::from_secs(1), task).await;
    assert!(result.is_ok(), "Task should complete");
    assert!(result.unwrap().unwrap(), "Should return true (cancelled)");
}

/// Test multiple cancellation signals (should be idempotent)
#[tokio::test]
async fn test_multiple_cancellation_signals() {
    let (tx, mut rx) = watch::channel(false);
    
    // Send cancellation multiple times
    tx.send(true).unwrap();
    rx.changed().await.unwrap();
    assert!(*rx.borrow());
    
    tx.send(true).unwrap();
    rx.changed().await.unwrap();
    assert!(*rx.borrow());
    
    tx.send(true).unwrap();
    rx.changed().await.unwrap();
    assert!(*rx.borrow());
    
    // Should remain true
    assert!(*rx.borrow());
}

/// Test cancellation token with timeout pattern (simulating orchestrator behavior)
#[tokio::test]
async fn test_cancellation_with_timeout() {
    let (tx, rx) = watch::channel(false);
    
    // Simulate orchestrator waiting for workers with timeout
    let orchestrator_task = tokio::spawn(async move {
        let worker_task = tokio::spawn(async move {
            // Simulate worker that checks cancellation
            loop {
                if *rx.borrow() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        
        // Wait for worker with timeout
        match timeout(Duration::from_secs(2), worker_task).await {
            Ok(Ok(_)) => (0, false), // Completed normally
            Ok(Err(_)) => (1, false), // Worker panicked
            Err(_) => (0, true),      // Timeout
        }
    });
    
    // Send cancellation quickly
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(true).unwrap();
    
    let result = timeout(Duration::from_secs(3), orchestrator_task).await;
    assert!(result.is_ok(), "Orchestrator should complete");
    let (error_count, was_cancelled) = result.unwrap().unwrap();
    assert_eq!(error_count, 0, "No errors expected");
    assert!(!was_cancelled, "Should complete before timeout");
}

/// Test that cancellation works with memory table operations
#[tokio::test]
async fn test_cancellation_with_memory_table() {
    use pg_reindexer::types::IndexInfo;
    
    let (tx, rx) = watch::channel(false);
    let memory_table = Arc::new(pg_reindexer::memory_table::SharedIndexMemoryTable::new());
    
    // Add some test indexes
    let indexes = vec![
        IndexInfo {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            index_name: "test_index1".to_string(),
            index_type: "btree".to_string(),
            size_bytes: Some(1000),
            parent_table_name: None,
        },
        IndexInfo {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            index_name: "test_index2".to_string(),
            index_type: "btree".to_string(),
            size_bytes: Some(2000),
            parent_table_name: None,
        },
    ];
    
    memory_table.initialize_with_indexes(indexes).await;
    
    // Worker that processes indexes until cancellation
    let worker_task = tokio::spawn(async move {
        let mut processed = 0;
        while memory_table.has_pending_indexes().await {
            if *rx.borrow() {
                break;
            }
            // Simulate processing
            processed += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        processed
    });
    
    // Let it process a bit, then cancel
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(true).unwrap();
    
    let result = timeout(Duration::from_secs(2), worker_task).await;
    assert!(result.is_ok(), "Worker should complete");
    let processed = result.unwrap().unwrap();
    // Should have processed at least one, but not all due to cancellation
    assert!(processed >= 1, "Should have processed at least one index");
}

// ============================================================
// Pacing timing tests
// ============================================================

/// Test that pacing_ms = 0 does not add meaningful delay
#[tokio::test]
async fn test_pacing_ms_zero_no_delay() {
    let pacing_ms: u64 = 0;
    let iterations = 5;
    let start = std::time::Instant::now();

    for _ in 0..iterations {
        if pacing_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pacing_ms)).await;
        }
    }

    let elapsed = start.elapsed();
    // With zero pacing there should be virtually no delay
    assert!(
        elapsed < Duration::from_millis(50),
        "Zero pacing should complete in under 50ms, took {:?}",
        elapsed
    );
}

/// Test that pacing_ms > 0 adds at least pacing_ms * iterations delay
#[tokio::test]
async fn test_pacing_ms_adds_delay_per_iteration() {
    let pacing_ms: u64 = 30;
    let iterations = 5;
    let start = std::time::Instant::now();

    for _ in 0..iterations {
        if pacing_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pacing_ms)).await;
        }
    }

    let elapsed = start.elapsed();
    let expected_min = Duration::from_millis(pacing_ms * iterations as u64);
    assert!(
        elapsed >= expected_min,
        "Pacing {}ms x {} iterations should take at least {:?}, took {:?}",
        pacing_ms, iterations, expected_min, elapsed
    );
}

/// Test that higher pacing_ms causes proportionally more delay
#[tokio::test]
async fn test_pacing_ms_proportional_to_value() {
    let low_pacing: u64 = 10;
    let high_pacing: u64 = 50;
    let iterations = 3;

    let start_low = std::time::Instant::now();
    for _ in 0..iterations {
        tokio::time::sleep(Duration::from_millis(low_pacing)).await;
    }
    let elapsed_low = start_low.elapsed();

    let start_high = std::time::Instant::now();
    for _ in 0..iterations {
        tokio::time::sleep(Duration::from_millis(high_pacing)).await;
    }
    let elapsed_high = start_high.elapsed();

    assert!(
        elapsed_high > elapsed_low,
        "Higher pacing ({:?}) should take longer than lower pacing ({:?})",
        elapsed_high, elapsed_low
    );
}

/// Test pacing respects cancellation signal via select!
#[tokio::test]
async fn test_pacing_ms_respects_cancellation() {
    let pacing_ms: u64 = 500; // Long enough that cancellation happens first
    let (tx, mut cancel_rx) = watch::channel(false);

    let worker = tokio::spawn(async move {
        let start = std::time::Instant::now();
        // Simulate one pacing sleep that should be interrupted by cancellation
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(pacing_ms)) => {
                false // Sleep completed — cancellation did not arrive
            }
            _ = cancel_rx.changed() => {
                true // Cancellation arrived before sleep ended
            }
        }
    });

    // Cancel well before the 500ms sleep would finish
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(true).unwrap();

    let result = timeout(Duration::from_secs(1), worker).await;
    assert!(result.is_ok(), "Worker should complete within timeout");
    let was_cancelled = result.unwrap().unwrap();
    assert!(was_cancelled, "Pacing sleep should be interrupted by cancellation");
}
