use chrono::Local;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use crate::primes::{SegmentData, SegmentPrimes};

pub fn get_nt_data_dir() -> PathBuf {
    let xdg_data_home = env::var("XDG_DATA_HOME")
        .ok()
        .and_then(|path| {
            if path.is_empty() {
                None
            } else {
                Some(PathBuf::from(path))
            }
        })
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(".local/share"))
        })
        .expect("Could not determine data directory");

    xdg_data_home.join("nt")
}

pub fn save_property(number: usize, property: &str) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let filename = format!("{}.txt", number);
    let path = data_dir.join(&filename);

    if path.exists() {
        if let Ok(content) = fs::read_to_string(&path) {
            if content.contains(property) {
                return Ok(());
            }
        }
    }

    fs::write(&path, property)?;
    Ok(())
}

pub fn save_all_primes(primes: &[usize]) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let primes_path = data_dir.join("primes.txt");
    let primes_text = primes
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<String>>()
        .join("\n");

    fs::write(&primes_path, primes_text)?;
    Ok(())
}
pub fn load_all_primes() -> std::io::Result<Vec<usize>> {
    let data_dir = get_nt_data_dir();
    let primes_path = data_dir.join("primes.txt");

    let content = fs::read_to_string(&primes_path)?;
    let primes = content
        .lines()
        .filter_map(|line| line.trim().parse::<usize>().ok())
        .collect();

    Ok(primes)
}

pub fn log_execution(
    subcommand: &str,
    args: &str,
    variation: u32,
    duration_us: u128,
) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let log_path = data_dir.join("execution_log.txt");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");

    writeln!(
        file,
        "{} | {} | {} | v{} | {}us",
        timestamp, subcommand, args, variation, duration_us
    )?;

    Ok(())
}

/// Save primes from a channel, streaming them to primes.txt one at a time
/// Optionally saves each prime as an individual property file
/// Returns the count of primes saved
pub fn save_primes_streaming(rx: Receiver<usize>, save_as_property: bool) -> usize {
    let mut count = 0;

    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::new(file);

    // Process each prime from the channel
    for prime in rx {
        if save_as_property {
            match save_property(prime, "prime") {
                Ok(_) => println!("Saved: {}.txt", prime),
                Err(e) => eprintln!("Error saving {}.txt: {}", prime, e),
            }
        }

        // Append prime to primes.txt (buffered) using itoa for speed
        let mut itoa_buf = itoa::Buffer::new();
        if let Err(e) = writer.write_all(itoa_buf.format(prime).as_bytes()) {
            eprintln!("Error writing to primes.txt: {}", e);
        }
        if let Err(e) = writer.write_all(b"\n") {
            eprintln!("Error writing newline to primes.txt: {}", e);
        }

        count += 1;
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt");
    count
}

/// Save primes from a channel that sends batched segments
/// Receives Vec<usize> instead of individual primes for better performance
/// Optionally saves each prime as an individual property file
/// Returns the count of primes saved
pub fn save_primes_streaming_batched(rx: Receiver<Vec<usize>>) -> usize {
    let mut count = 0;

    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::with_capacity(256 * 1024, file); // 256KB

    // Process each segment of primes from the channel
    let mut itoa_buf = itoa::Buffer::new();
    for segment_primes in rx {
        for prime in segment_primes {
            // Append prime to primes.txt (buffered) using itoa for speed
            if let Err(e) = writer.write_all(itoa_buf.format(prime).as_bytes()) {
                eprintln!("Error writing to primes.txt: {}", e);
            }
            if let Err(e) = writer.write_all(b"\n") {
                eprintln!("Error writing newline to primes.txt: {}", e);
            }

            count += 1;
        }
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt");
    count
}

/// Save primes from raw segment data (variation 7)
/// Unpacks segments on consumer side and saves to primes.txt
/// Optionally saves each prime as an individual property file
/// Returns the count of primes saved
pub fn save_primes_streaming_segments(rx: Receiver<SegmentData>, limit: usize) -> usize {
    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::with_capacity(128 * 1024, file);
    if let Err(e) = writeln!(writer, "2") {
        eprintln!("Error writing to primes.txt: {}", e);
    }
    let mut count = 1;

    // Process each segment from the channel
    let mut itoa_buf = itoa::Buffer::new();
    for segment_data in rx {
        // Unpack and write directly (no intermediate Vec allocation!)
        for word_idx in 0..segment_data.bits.len() {
            let mut word = segment_data.bits[word_idx];

            while word != 0 {
                let bit_idx = word.trailing_zeros() as usize;
                let idx = word_idx * 64 + bit_idx;

                let num = segment_data.low + idx * 2;
                // Append prime to primes.txt (buffered) using itoa for speed
                if num > segment_data.high || num > limit {
                    break;
                }

                if let Err(e) = writer.write_all(itoa_buf.format(num).as_bytes()) {
                    eprintln!("Error writing to primes.txt: {}", e);
                }
                if let Err(e) = writer.write_all(b"\n") {
                    eprintln!("Error writing newline to primes.txt: {}", e);
                }
                count += 1;

                word &= word - 1; // Clear lowest set bit
            }
        }
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt");
    count
}

/// Save primes from unpacked segment data with reordering (variation 8)
/// Receives segments out-of-order from parallel workers and writes in order
/// Segments are already unpacked by workers (producer-side unpacking like v6)
/// Returns the count of primes saved
pub fn save_primes_streaming_segments_parallel(rx: Receiver<SegmentPrimes>) -> usize {
    let mut count = 0;

    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter with larger buffer for better performance
    let mut writer = BufWriter::with_capacity(128 * 1024, file);

    // Buffer for out-of-order segments
    let mut segment_buffer: BTreeMap<usize, SegmentPrimes> = BTreeMap::new();
    let mut next_expected_id = 0;

    // String buffer for batch writing (reused across segments)
    let mut string_buffer = String::with_capacity(2 * 1024 * 1024); // 2MB initial

    // Helper function to process a segment
    let process_segment = |segment_primes: &SegmentPrimes,
                           writer: &mut BufWriter<_>,
                           string_buffer: &mut String|
     -> usize {
        let local_count = segment_primes.primes.len();

        // Batch write: build string then write once
        string_buffer.clear();

        // Pre-allocate estimated capacity (avg ~10 bytes per prime with newline)
        let estimated_size = local_count * 11;
        if string_buffer.capacity() < estimated_size {
            string_buffer.reserve(estimated_size - string_buffer.capacity());
        }

        // Build batch string using itoa (fastest integer formatting)
        let mut itoa_buf = itoa::Buffer::new();
        for &prime in &segment_primes.primes {
            string_buffer.push_str(itoa_buf.format(prime));
            string_buffer.push('\n');
        }

        // Single write call for entire segment
        if let Err(e) = writer.write_all(string_buffer.as_bytes()) {
            eprintln!("Error writing to primes.txt: {}", e);
        }

        local_count
    };

    // Process segments in order
    for segment_primes in rx {
        let segment_id = segment_primes.segment_id;

        // Add to buffer
        segment_buffer.insert(segment_id, segment_primes);

        // Process all consecutive segments starting from next_expected_id
        while let Some(seg) = segment_buffer.remove(&next_expected_id) {
            count += process_segment(&seg, &mut writer, &mut string_buffer);
            next_expected_id += 1;
        }
    }

    // Process any remaining buffered segments (shouldn't happen if producer is correct)
    while let Some((_, seg)) = segment_buffer.pop_first() {
        count += process_segment(&seg, &mut writer, &mut string_buffer);
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt (parallel)");
    count
}

/// Save primes from unpacked segment data with reordering in BINARY format (variation 8)
/// Receives segments out-of-order from parallel workers and writes in order
/// Binary format: 8 bytes per prime (little-endian u64)
/// Returns the count of primes saved
pub fn save_primes_streaming_segments_parallel_binary(rx: Receiver<SegmentPrimes>) -> usize {
    let mut count = 0;

    // Open primes.bin in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.bin");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.bin: {}", e);
            return 0;
        }
    };

    // Use BufWriter with larger buffer for better performance
    let mut writer = BufWriter::with_capacity(128 * 1024, file);

    // Buffer for out-of-order segments
    let mut segment_buffer: BTreeMap<usize, SegmentPrimes> = BTreeMap::new();
    let mut next_expected_id = 0;

    // Helper function to process a segment
    let process_segment = |segment_primes: &SegmentPrimes, writer: &mut BufWriter<_>| -> usize {
        let local_count = segment_primes.primes.len();

        // Write primes as binary (8 bytes each, little-endian)
        for &prime in &segment_primes.primes {
            let bytes = (prime as u64).to_le_bytes();
            if let Err(e) = writer.write_all(&bytes) {
                eprintln!("Error writing to primes.bin: {}", e);
            }
        }

        local_count
    };

    // Process segments in order
    for segment_primes in rx {
        let segment_id = segment_primes.segment_id;

        // Add to buffer
        segment_buffer.insert(segment_id, segment_primes);

        // Process all consecutive segments starting from next_expected_id
        while let Some(seg) = segment_buffer.remove(&next_expected_id) {
            count += process_segment(&seg, &mut writer);
            next_expected_id += 1;
        }
    }

    // Process any remaining buffered segments (shouldn't happen if producer is correct)
    while let Some((_, seg)) = segment_buffer.pop_first() {
        count += process_segment(&seg, &mut writer);
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.bin: {}", e);
    }

    println!("\nSaved all primes to primes.bin (parallel, binary format)");
    count
}

/// Save primes from batched segments in BINARY format (variation 6)
/// Binary format: 8 bytes per prime (little-endian u64)
/// Returns the count of primes saved
pub fn save_primes_streaming_batched_binary(rx: Receiver<Vec<usize>>) -> usize {
    let mut count = 0;

    // Open primes.bin in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.bin");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.bin: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // Process each segment of primes from the channel
    for segment_primes in rx {
        for prime in segment_primes {
            // Write as binary (8 bytes, little-endian)
            let bytes = (prime as u64).to_le_bytes();
            if let Err(e) = writer.write_all(&bytes) {
                eprintln!("Error writing to primes.bin: {}", e);
            }

            count += 1;
        }
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.bin: {}", e);
    }

    println!("\nSaved all primes to primes.bin (binary format)");
    count
}

/// Save small primes to primes_small.bin (for variation 9)
/// Binary format: 8 bytes per prime (little-endian u64)
/// Returns the count of primes saved
pub fn save_small_primes_binary(primes: &[usize]) -> usize {
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes_small.bin");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes_small.bin: {}", e);
            return 0;
        }
    };

    let mut writer = BufWriter::with_capacity(64 * 1024, file);

    for &prime in primes {
        let bytes = (prime as u64).to_le_bytes();
        if let Err(e) = writer.write_all(&bytes) {
            eprintln!("Error writing to primes_small.bin: {}", e);
        }
    }

    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes_small.bin: {}", e);
    }

    let count = primes.len();
    println!("Saved {} small primes to primes_small.bin", count);
    count
}

/// Multi-consumer for variation 9 with N consumers
/// Writes segments to primes_{consumer_id}.bin
/// Each consumer processes segments where (segment_id - 1) % num_consumers == (consumer_id - 1)
/// Binary format: 8 bytes per prime (little-endian u64)
/// Returns the count of primes saved
pub fn save_primes_multi_consumer_binary(
    rx: Receiver<SegmentPrimes>,
    consumer_id: usize,
    num_consumers: usize,
) -> usize {
    let mut count = 0;

    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

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

    let mut writer = BufWriter::with_capacity(128 * 1024, file);

    // Buffer for out-of-order segments
    let mut segment_buffer: BTreeMap<usize, SegmentPrimes> = BTreeMap::new();
    // This consumer handles segments where (segment_id - 1) % num_consumers == (consumer_id - 1)
    // So first segment is consumer_id, next is consumer_id + num_consumers, etc.
    let mut next_expected_id = consumer_id;

    // Helper to process segment
    let process_segment =
        |segment_primes: &SegmentPrimes, writer: &mut BufWriter<_>, filename: &str| -> usize {
            let local_count = segment_primes.primes.len();
            for &prime in &segment_primes.primes {
                let bytes = (prime as u64).to_le_bytes();
                if let Err(e) = writer.write_all(&bytes) {
                    eprintln!("Error writing to {}: {}", filename, e);
                }
            }
            local_count
        };

    // Process segments in order
    for segment_primes in rx {
        let segment_id = segment_primes.segment_id;
        segment_buffer.insert(segment_id, segment_primes);

        // Process all consecutive segments for this consumer
        while let Some(seg) = segment_buffer.remove(&next_expected_id) {
            count += process_segment(&seg, &mut writer, &filename);
            next_expected_id += num_consumers; // Skip to next segment for this consumer
        }
    }

    // Process remaining
    while let Some((_, seg)) = segment_buffer.pop_first() {
        count += process_segment(&seg, &mut writer, &filename);
    }

    if let Err(e) = writer.flush() {
        eprintln!("Error flushing {}: {}", filename, e);
    }

    println!(
        "Consumer {}: Saved {} primes to {}",
        consumer_id, count, filename
    );
    count
}
