// io_uring-based async I/O implementation for maximum disk throughput

use io_uring::{IoUring, opcode, types};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;

use crate::primes::SegmentPrimes;
use crate::storage::get_nt_data_dir;

/// Batch writer using io_uring for async I/O
struct UringBatchWriter {
    ring: IoUring,
    file: File,  // Keep file alive to prevent FD from being closed
    pending_buffers: VecDeque<Vec<u8>>,
    offset: u64,
    submitted: usize,
    completed: usize,
}

impl UringBatchWriter {
    fn new(file: File, queue_depth: u32) -> std::io::Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
            file,
            pending_buffers: VecDeque::new(),
            offset: 0,
            submitted: 0,
            completed: 0,
        })
    }

    /// Submit a write operation (non-blocking)
    fn submit_write(&mut self, data: Vec<u8>) -> std::io::Result<()> {
        let len = data.len();

        // Create write operation
        let write_op = opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            data.as_ptr(),
            len as u32,
        )
        .offset(self.offset);

        // Submit to submission queue
        unsafe {
            self.ring
                .submission()
                .push(&write_op.build())
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "submission queue full")
                })?;
        }

        self.pending_buffers.push_back(data); // Keep buffer alive
        self.offset += len as u64;
        self.submitted += 1;

        Ok(())
    }

    /// Submit all pending operations to kernel
    fn submit_batch(&mut self) -> std::io::Result<()> {
        self.ring.submit()?;
        Ok(())
    }

    /// Poll for completions (non-blocking)
    fn poll_completions(&mut self) -> std::io::Result<usize> {
        let mut completed_count = 0;

        while let Some(cqe) = self.ring.completion().next() {
            if cqe.result() < 0 {
                return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            }
            self.pending_buffers.pop_front(); // Free buffer
            self.completed += 1;
            completed_count += 1;
        }

        Ok(completed_count)
    }

    /// Wait for specific number of completions
    fn wait_completions(&mut self, count: usize) -> std::io::Result<()> {
        for _ in 0..count {
            self.ring.submit_and_wait(1)?;
            let cqe =
                self.ring.completion().next().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::Other, "no completion")
                })?;

            if cqe.result() < 0 {
                return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            }
            self.pending_buffers.pop_front();
            self.completed += 1;
        }
        Ok(())
    }

    /// Get number of in-flight operations
    fn in_flight(&self) -> usize {
        self.submitted - self.completed
    }
}

/// Multi-consumer using io_uring for async I/O
/// Provides 2-3× better throughput on disk-bound workloads
pub fn save_primes_multi_consumer_uring(
    rx: Receiver<SegmentPrimes>,
    consumer_id: usize,
    num_consumers: usize,
    total_received: Arc<AtomicUsize>,
    total_sent: Arc<AtomicUsize>,
) -> usize {
    const QUEUE_DEPTH: u32 = 256; // io_uring queue depth
    const MAX_IN_FLIGHT: usize = 200; // Backpressure threshold
    const BATCH_SIZE: usize = 64; // Submit every N segments

    let mut count = 0;

    let data_dir = match get_nt_data_dir().canonicalize() {
        Ok(dir) => {
            if let Err(e) = fs::create_dir_all(&dir) {
                eprintln!("Error creating data directory: {}", e);
                return 0;
            }
            dir
        }
        Err(e) => {
            eprintln!("Error getting data directory: {}", e);
            return 0;
        }
    };

    let filename = format!("primes_{}.bin", consumer_id);
    let primes_path = data_dir.join(&filename);

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening {}: {}", filename, e);
            return 0;
        }
    };

    eprintln!(
        "Consumer {}: Using io_uring (queue depth: {})",
        consumer_id, QUEUE_DEPTH
    );

    let mut writer = match UringBatchWriter::new(file, QUEUE_DEPTH) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Error creating io_uring writer: {}", e);
            return 0;
        }
    };

    // Reordering buffer for out-of-order segments
    let mut segment_buffer: BTreeMap<usize, SegmentPrimes> = BTreeMap::new();
    let mut next_expected_id = consumer_id;

    let memory_report_interval = 1000;
    let mut batch_count = 0;

    // Peak tracking
    let mut peak_buffer_size = 0;
    let mut peak_in_flight = 0;

    // Process segments in order
    for segment_primes in rx {
        let segment_id = segment_primes.segment_id;

        // Increment receive counter
        total_received.fetch_add(1, Ordering::Relaxed);

        segment_buffer.insert(segment_id, segment_primes);

        // Process all consecutive segments for this consumer
        while let Some(seg) = segment_buffer.remove(&next_expected_id) {
            // Convert primes to bytes
            let mut buffer = Vec::with_capacity(seg.primes.len() * 8);
            for &prime in &seg.primes {
                buffer.extend_from_slice(&prime.to_le_bytes());
            }

            count += seg.primes.len();

            // Submit write (non-blocking)
            if let Err(e) = writer.submit_write(buffer) {
                eprintln!("Error submitting write: {}", e);
                break;
            }

            batch_count += 1;
            next_expected_id += num_consumers;

            // Submit batch periodically
            if batch_count >= BATCH_SIZE {
                if let Err(e) = writer.submit_batch() {
                    eprintln!("Error submitting batch: {}", e);
                    break;
                }
                batch_count = 0;
            }

            // Backpressure: if too many in-flight, wait for some to complete
            if writer.in_flight() > MAX_IN_FLIGHT {
                if let Err(e) = writer.wait_completions(100) {
                    eprintln!("Error waiting for completions: {}", e);
                    break;
                }
            }

            // Poll completions (non-blocking)
            if let Err(e) = writer.poll_completions() {
                eprintln!("Error polling completions: {}", e);
                //exit program
                std::process::exit(1);
            }

            // Track peak in-flight
            if writer.in_flight() > peak_in_flight {
                peak_in_flight = writer.in_flight();
            }

            // Periodic memory reporting
            if (next_expected_id / num_consumers) % memory_report_interval == 0 {
                if let Some((rss_mb, _vm_mb)) = crate::storage::get_process_memory_mb() {
                    let sent = total_sent.load(Ordering::Relaxed);
                    let received = total_received.load(Ordering::Relaxed);
                    let gap = sent.saturating_sub(received);
                    eprintln!(
                        "[Consumer {}/{}] Processed {} segments | Sent: {} | Received: {} | Gap: {} | In-flight: {} | RSS={:.2} MB",
                        consumer_id,
                        num_consumers,
                        next_expected_id / num_consumers,
                        sent,
                        received,
                        gap,
                        writer.in_flight(),
                        rss_mb
                    );
                }
            }
        }

        // Track peak buffer size
        if segment_buffer.len() > peak_buffer_size {
            peak_buffer_size = segment_buffer.len();
        }
    }

    // Final batch submission
    if let Err(e) = writer.submit_batch() {
        eprintln!("Error submitting final batch: {}", e);
    }

    // Wait for all remaining completions
    let remaining = writer.in_flight();
    if remaining > 0 {
        if let Err(e) = writer.wait_completions(remaining) {
            eprintln!("Error waiting for final completions: {}", e);
        }
    }

    println!(
        "Consumer {}: Saved {} primes to {} | Peak buffer: {} segments | Peak in-flight: {} ops",
        consumer_id, count, filename, peak_buffer_size, peak_in_flight
    );

    count
}
