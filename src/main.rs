mod chain;
mod pi;
mod primes;
mod primes_bases;
mod random;
mod scan;
mod storage;

use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "nt")]
#[command(about = "Number Theory CLI - Various number theory programs", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Find all prime numbers up to a given limit")]
    Primes {
        #[arg(help = "The upper limit to search for primes")]
        limit: usize,
        #[arg(short, long, default_value = "1", help = "Algorithm variation to use")]
        variation: u32,
        #[arg(long, help = "Save each prime as an individual property file")]
        save_as_property: bool,
        #[arg(
            short,
            long,
            help = "Number of worker threads for parallel processing (variation 8+ only)"
        )]
        workers: Option<usize>,
        #[arg(
            short,
            long,
            help = "Save primes in binary format (8 bytes per prime, little-endian)"
        )]
        binary: bool,
        #[arg(
            long,
            default_value = "2",
            help = "Number of consumer threads for parallel I/O (variation 9 only)"
        )]
        consumers: usize,
    },
    #[command(about = "Find all prime numbers up to a given limit (storing all in memory)")]
    PrimesAllMem {
        #[arg(help = "The upper limit to search for primes")]
        limit: usize,
        #[arg(short, long, default_value = "1", help = "Algorithm variation to use")]
        variation: u32,
        #[arg(long, help = "Save each prime as an individual property file")]
        save_as_property: bool,
    },
    #[command(about = "Output primes from primes.txt as different bases")]
    PrimesBases {
        #[arg(long, help = "Only display palindromes, show dash for non-palindromes")]
        pal_only: bool,
        #[arg(
            long,
            help = "Only show rows containing this specific palindrome value"
        )]
        pal: Option<String>,
    },
    #[command(about = "Calculate and print pi to a specified number of decimal places")]
    Pi {
        #[arg(default_value = "100", help = "Number of decimal places to calculate")]
        digits: usize,
    },
    #[command(about = "Generate random digits and search for prime numbers")]
    Random {
        #[arg(default_value = "100", help = "Number of random digits to generate")]
        digits: usize,
    },
    #[command(about = "Build a chain of overlapping primes")]
    Chain {
        #[arg(
            short,
            long,
            default_value = "4",
            help = "Number of digits that overlap between primes"
        )]
        overlap: usize,
        #[arg(
            short,
            long,
            default_value = "100",
            help = "Target length of the digit chain"
        )]
        length: usize,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::PrimesAllMem {
            limit,
            variation,
            save_as_property,
        } => {
            let start = Instant::now();

            // For variation 5 (segmented sieve), adjust limit to account for small primes range
            let (effective_limit, original_limit) = if variation == 5 {
                if limit < primes::SEGMENT_SIZE_NUMBERS {
                    eprintln!(
                        "Variation 5 (segmented sieve) requires limit >= {}",
                        primes::SEGMENT_SIZE_NUMBERS
                    );
                    eprintln!("For smaller limits, use variation 2 or 4 instead.");
                    return;
                }

                // Calculate sqrt_limit once and use it consistently
                let sqrt_limit = (limit as f64).sqrt() as usize;
                let low = (sqrt_limit + 1) | 1; // First odd after sqrt (where segments start)
                let range_to_cover = if limit >= low { limit - low + 1 } else { 0 };
                let num_segments = (range_to_cover + primes::SEGMENT_SIZE_NUMBERS - 1)
                    / primes::SEGMENT_SIZE_NUMBERS;
                let effective_limit = low + (num_segments * primes::SEGMENT_SIZE_NUMBERS) - 1;

                if effective_limit != limit {
                    println!(
                        "Adjusting limit from {} to {} (sqrt={}, low={}, segments={})",
                        limit, effective_limit, sqrt_limit, low, num_segments
                    );
                }

                (effective_limit, limit)
            } else {
                (limit, limit)
            };

            println!(
                "Finding primes up to {} (variation {})...",
                effective_limit, variation
            );

            let primes = primes::find_primes(effective_limit, variation);

            if save_as_property {
                for &prime in &primes {
                    match storage::save_property(prime, "prime") {
                        Ok(_) => println!("Saved: {}.txt", prime),
                        Err(e) => eprintln!("Error saving {}.txt: {}", prime, e),
                    }
                }
            }

            // Save all primes to primes.txt in XDG_DATA_HOME
            match storage::save_all_primes(&primes) {
                Ok(_) => println!("\nSaved all primes to primes.txt"),
                Err(e) => eprintln!("Error saving primes.txt: {}", e),
            }

            println!("\nTotal: {} primes found", primes.len());

            let duration = start.elapsed();
            let duration_us = duration.as_micros();

            println!(
                "Execution time: {}us ({:.2}ms)",
                duration_us,
                duration_us as f64 / 1000.0
            );

            if let Err(e) = storage::log_execution(
                "primes-all-mem",
                &original_limit.to_string(),
                variation,
                duration_us,
            ) {
                eprintln!("Warning: Failed to log execution: {}", e);
            }
        }
        Commands::Primes {
            limit,
            variation,
            save_as_property,
            workers,
            binary,
            consumers,
        } => {
            let start = Instant::now();

            // For variation 5, 6, 7, 8, or 9, adjust limit to account for small primes range
            let (effective_limit, original_limit, sqrt_limit) = if variation == 5
                || variation == 6
                || variation == 7
                || variation == 8
                || variation == 9
            {
                if limit < primes::SEGMENT_SIZE_NUMBERS {
                    eprintln!(
                        "Variation {} (segmented sieve) requires limit >= {}",
                        variation,
                        primes::SEGMENT_SIZE_NUMBERS
                    );
                    eprintln!("For smaller limits, use variation 2 or 4 instead.");
                    return;
                }

                // Calculate sqrt_limit once and use it consistently
                let sqrt_limit = (limit as f64).sqrt() as usize;
                let low = (sqrt_limit + 1) | 1; // First odd after sqrt (where segments start)
                let range_to_cover = if limit >= low { limit - low + 1 } else { 0 };
                let num_segments = (range_to_cover + primes::SEGMENT_SIZE_NUMBERS - 1)
                    / primes::SEGMENT_SIZE_NUMBERS;
                let effective_limit = low + (num_segments * primes::SEGMENT_SIZE_NUMBERS) - 1;

                if effective_limit != limit {
                    println!(
                        "Adjusting limit from {} to {} (sqrt={}, low={}, segments={})",
                        limit, effective_limit, sqrt_limit, low, num_segments
                    );
                }

                (effective_limit, limit, sqrt_limit)
            } else {
                (limit, limit, 0) // sqrt_limit not needed for other variations
            };

            println!(
                "Finding primes up to {} (variation {})...",
                effective_limit, variation
            );

            // For variation 6, use batched channel; for variation 7, use segment channel;
            // for variation 8, use parallel segment channel; otherwise use single-prime channel
            let consumer_handle = if variation == 6 {
                let (tx, rx) = mpsc::channel::<Vec<usize>>();

                // Spawn consumer thread for batched segments
                let handle = if binary {
                    thread::spawn(move || storage::save_primes_streaming_batched_binary(rx))
                } else {
                    thread::spawn(move || storage::save_primes_streaming_batched(rx))
                };

                // Generate primes and send batched to consumer thread
                primes::find_primes_v6_streaming(effective_limit, sqrt_limit, tx);

                handle
            } else if variation == 7 {
                let (tx, rx) = mpsc::channel::<primes::SegmentData>();

                // Spawn consumer thread for raw segments (unpacking on consumer side)
                let handle = thread::spawn(move || {
                    storage::save_primes_streaming_segments(rx, effective_limit)
                });

                // Generate primes and send raw segments to consumer thread
                primes::find_primes_v7_streaming(effective_limit, sqrt_limit, tx);

                handle
            } else if variation == 8 {
                // Determine number of workers (default to CPU count)
                let num_workers = workers.unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                });

                println!(
                    "Using {} worker threads for parallel processing",
                    num_workers
                );

                let (tx, rx) = mpsc::channel::<primes::SegmentPrimes>();

                // Spawn consumer thread for parallel segments (with reordering)
                let handle = if binary {
                    thread::spawn(move || {
                        storage::save_primes_streaming_segments_parallel_binary(rx)
                    })
                } else {
                    thread::spawn(move || storage::save_primes_streaming_segments_parallel(rx))
                };

                // Generate primes in parallel and send unpacked segments to consumer thread
                primes::find_primes_v8_parallel(effective_limit, sqrt_limit, tx, num_workers);

                handle
            } else if variation == 9 {
                // Variation 9: Multiple consumers for parallel I/O
                // Only binary format supported for v9
                if !binary {
                    eprintln!("Variation 9 requires --binary flag");
                    return;
                }

                if consumers < 1 {
                    eprintln!("Number of consumers must be at least 1");
                    return;
                }

                // Determine number of workers (default to CPU count)
                let num_workers = workers.unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                });

                println!(
                    "Using {} worker threads with {} consumers for parallel I/O",
                    num_workers, consumers
                );

                // Remove all existing primes_*.bin files to avoid leftover files from previous runs
                storage::cleanup_prime_files();

                // Create channels for each consumer
                let mut senders = Vec::new();
                let mut consumer_handles = Vec::new();

                // Create atomic counters to track channel depth
                let total_sent = Arc::new(AtomicUsize::new(0));
                let total_received = Arc::new(AtomicUsize::new(0));

                // Channel capacity: limits buffering to prevent OOM
                // With 15 consumers × 100 capacity = 1,500 segments max = ~240 MB
                const CHANNEL_CAPACITY: usize = 100;

                for consumer_id in 1..=consumers {
                    let (tx, rx) = mpsc::sync_channel::<primes::SegmentPrimes>(CHANNEL_CAPACITY);
                    senders.push(tx);

                    // Spawn consumer thread
                    let total_received_clone = Arc::clone(&total_received);
                    let total_sent_clone = Arc::clone(&total_sent);
                    let handle = thread::spawn(move || {
                        storage::save_primes_multi_consumer_binary(
                            rx,
                            consumer_id,
                            consumers,
                            total_received_clone,
                            total_sent_clone,
                        )
                    });
                    consumer_handles.push(handle);
                }

                // Generate primes and get small_primes back (blocks until producer done)
                let small_primes = primes::find_primes_v9_multi_consumers(
                    effective_limit,
                    sqrt_limit,
                    senders,
                    num_workers,
                    total_sent,
                );

                // Return handle that waits for all consumers and computes total
                // Save small primes in this thread to avoid affecting producer timing
                thread::spawn(move || {
                    // Save small primes while consumers are working
                    let small_count = storage::save_small_primes_binary(&small_primes);

                    // Wait for all consumers to finish
                    let mut consumer_counts = Vec::new();
                    for (i, handle) in consumer_handles.into_iter().enumerate() {
                        let count = handle.join().unwrap();
                        consumer_counts.push((i + 1, count));
                    }

                    let consumers_total: usize = consumer_counts.iter().map(|(_, c)| c).sum();
                    let total = small_count + consumers_total;

                    print!("Total primes: {} (small: {}", total, small_count);
                    for (id, count) in consumer_counts {
                        print!(", consumer{}: {}", id, count);
                    }
                    println!(")");

                    total
                })
            } else {
                let (tx, rx) = mpsc::channel();

                // Spawn consumer thread for individual primes
                let handle =
                    thread::spawn(move || storage::save_primes_streaming(rx, save_as_property));

                // Generate primes and send to consumer thread
                primes::find_primes_streaming(effective_limit, variation, tx);

                handle
            };

            let producer_done = start.elapsed();
            println!(
                "\nProducer finished: {}us ({:.2}ms)",
                producer_done.as_micros(),
                producer_done.as_micros() as f64 / 1000.0
            );

            // Wait for consumer to finish and get prime count
            let prime_count = consumer_handle.join().unwrap();

            let consumer_done = start.elapsed();
            let consumer_lag = consumer_done - producer_done;

            println!(
                "Consumer finished: {}us ({:.2}ms)",
                consumer_done.as_micros(),
                consumer_done.as_micros() as f64 / 1000.0
            );
            println!(
                "Consumer lag: {}us ({:.2}ms)",
                consumer_lag.as_micros(),
                consumer_lag.as_micros() as f64 / 1000.0
            );

            println!("\nTotal: {} primes found", prime_count);

            let duration = start.elapsed();
            let duration_us = duration.as_micros();

            println!(
                "Total execution time: {}us ({:.2}ms)",
                duration_us,
                duration_us as f64 / 1000.0
            );

            if let Err(e) = storage::log_execution(
                "primes",
                &original_limit.to_string(),
                variation,
                duration_us,
            ) {
                eprintln!("Warning: Failed to log execution: {}", e);
            }
        }
        Commands::PrimesBases { pal_only, pal } => {
            primes_bases::run(pal_only, pal);
        }
        Commands::Pi { digits } => {
            pi::calculate_and_print(digits);
        }
        Commands::Random { digits } => {
            random::generate_and_scan(digits);
        }
        Commands::Chain { overlap, length } => {
            chain::build_chain(overlap, length);
        }
    }
}
