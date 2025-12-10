use std::sync::{Arc, mpsc::{Sender, SyncSender}};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

// Segment size constants for variation 5+ (segmented sieve)
pub const SEGMENT_SIZE_BITS: usize = 32 * 1024 * 8; // 32KB in bits = 262,144 odd numbers
pub const SEGMENT_SIZE_NUMBERS: usize = SEGMENT_SIZE_BITS * 2; // 524,288 actual numbers

/// Raw segment data for variation 7 (consumer-side unpacking)
#[derive(Clone)]
pub struct SegmentData {
    pub bits: Vec<u64>,
    pub low: usize,
    pub high: usize,
}

/// Unpacked segment primes for variation 8 (producer-side unpacking)
#[derive(Clone)]
pub struct SegmentPrimes {
    pub primes: Vec<usize>,
    pub segment_id: usize, // For ordering in parallel processing
}

pub fn find_primes_streaming(limit: usize, variation: u32, sender: Sender<usize>) {
    match variation {
        1 => find_primes_v1_streaming(limit, sender),
        2 => find_primes_v2_streaming(limit, sender),
        3 => find_primes_v3_streaming(limit, sender),
        4 => find_primes_v4_streaming(limit, sender),
        5 => find_primes_v5_streaming(limit, sender),
        _ => {
            eprintln!("Unknown variation {}, using variation 1", variation);
            find_primes_v1_streaming(limit, sender)
        }
    }
}

/// Variation 1: Basic Sieve of Eratosthenes
///
/// Classic implementation that marks all composite numbers.
/// - Time complexity: O(n log log n)
/// - Space complexity: O(n) - 1 byte per number
/// - Processes all numbers including even numbers
/// - Simple and straightforward implementation
/// Variation 1 with streaming: sends primes as they're found
fn find_primes_v1_streaming(limit: usize, sender: Sender<usize>) {
    if limit < 2 {
        return;
    }

    let mut is_prime = vec![true; limit + 1];
    is_prime[0] = false;
    is_prime[1] = false;

    for i in 2..=((limit as f64).sqrt() as usize) {
        if is_prime[i] {
            let mut j = i * i;
            while j <= limit {
                is_prime[j] = false;
                j += i;
            }
        }
    }

    for (num, &prime) in is_prime.iter().enumerate() {
        if prime {
            if sender.send(num).is_err() {
                break; // Receiver dropped, stop sending
            }
        }
    }
}

/// Variation 2: Odd-Only Sieve of Eratosthenes
///
/// Optimized version that only processes odd numbers (skips all evens except 2).
/// - Time complexity: O(n log log n) - same asymptotic, but ~40% faster in practice
/// - Space complexity: O(n/2) - half the memory usage
/// - Index mapping: is_prime[i] represents the number (2*i + 3)
/// - Only marks odd multiples of odd primes
/// - Best general-purpose optimization with simple implementation
fn find_primes_v2_streaming(limit: usize, sender: Sender<usize>) {
    if limit < 2 {
        return;
    }
    if limit == 2 {
        let _ = sender.send(2);
        return;
    }

    // Send 2 first
    if sender.send(2).is_err() {
        return;
    }

    // Array size is half since we only track odd numbers
    let size = (limit - 1) / 2;
    let mut is_prime = vec![true; size];

    let sqrt_limit = ((limit as f64).sqrt() as usize - 1) / 2;

    for i in 0..=sqrt_limit {
        if is_prime[i] {
            let p = 2 * i + 3;
            let mut j = (p * p - 3) / 2;
            while j < size {
                is_prime[j] = false;
                j += p;
            }
        }
    }

    for (i, &is_p) in is_prime.iter().enumerate() {
        if is_p {
            if sender.send(2 * i + 3).is_err() {
                break; // Receiver dropped, stop sending
            }
        }
    }
}

/// Variation 3: Streaming optimization that sends primes as soon as they're confirmed
///
/// Sends primes in two phases:
/// 1. During sieving: Sends primes <= sqrt(limit) immediately before using them as divisors
/// 2. After sieving: Sends remaining primes > sqrt(limit) that survived the sieve
///
/// Benefits:
/// - Consumer can start processing primes while sieve is still running
/// - Better concurrency between producer and consumer
/// - Lower peak memory usage in channel
fn find_primes_v3_streaming(limit: usize, sender: Sender<usize>) {
    if limit < 2 {
        return;
    }
    if limit == 2 {
        let _ = sender.send(2);
        return;
    }

    // Send 2 immediately
    if sender.send(2).is_err() {
        return;
    }

    // Array size is half since we only track odd numbers
    let size = (limit - 1) / 2;
    let mut is_prime = vec![true; size];

    let sqrt_limit = ((limit as f64).sqrt() as usize - 1) / 2;

    // Phase 1: Sieve and send small primes (up to sqrt(limit))
    for i in 0..=sqrt_limit {
        if is_prime[i] {
            let p = 2 * i + 3;

            // Performance Issues
            //
            // Cache locality disruption of hot loop
            // - Sieving is cache-friendly (sequential array access)
            // - sender.send() accesses channel internals (different memory)
            // - CPU cache thrashing between sieve array and channel
            // Send this prime immediately - we know it's prime for certain
            if sender.send(p).is_err() {
                return; // Receiver dropped, stop
            }
            // Downsides of sending inside the sieving loop:
            // - Breaks cache locality
            // - Adds call overhead
            // - Prevents compiler optimizations

            // Now mark its multiples as composite
            let mut j = (p * p - 3) / 2;
            while j < size {
                is_prime[j] = false;
                j += p;
            }
        }
    }

    // Phase 2: Send remaining primes (above sqrt(limit))
    for i in (sqrt_limit + 1)..size {
        if is_prime[i] {
            if sender.send(2 * i + 3).is_err() {
                break; // Receiver dropped, stop sending
            }
        }
    }
}

/// Variation 4: Bit-packed Odd-Only Sieve with Streaming
///
/// Maximum memory efficiency with streaming: Only odd numbers + bit packing + sends as found.
/// - Memory: 1 bit per odd number (16x savings vs Vec<bool>)
/// - Time complexity: O(n log log n) + bit manipulation overhead
/// - Space complexity: O(n/128) - 16x compression
/// - Index mapping: bit i represents number (2*i + 3)
/// - Streams results to consumer as they're found
fn find_primes_v4_streaming(limit: usize, sender: Sender<usize>) {
    if limit < 2 {
        return;
    }
    if limit == 2 {
        let _ = sender.send(2);
        return;
    }

    // Send 2 immediately
    if sender.send(2).is_err() {
        return;
    }

    // Only track odd numbers: 3, 5, 7, 9, 11, ...
    // Index i represents number (2*i + 3)
    let odd_count = (limit - 1) / 2;
    let size = (odd_count + 63) / 64; // Number of u64 words needed
    let mut is_prime = vec![!0_u64; size]; // All bits set to 1 (true)

    // Helper: Get bit at position idx
    #[inline]
    fn get_bit(bits: &[u64], idx: usize) -> bool {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (bits[word_idx] & (1_u64 << bit_idx)) != 0
    }

    // Helper: Clear bit at position idx
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    let sqrt_limit = ((limit as f64).sqrt() as usize - 1) / 2;

    // Sieve odd numbers
    for i in 0..=sqrt_limit.min(odd_count - 1) {
        if get_bit(&is_prime, i) {
            let p = 2 * i + 3;

            // Mark odd multiples of p as composite
            let mut j = (p * p - 3) / 2;
            while j < odd_count {
                clear_bit(&mut is_prime, j);
                j += p;
            }
        }
    }

    // Send all odd primes (optimized: iterate word-by-word, skip to set bits)
    for word_idx in 0..is_prime.len() {
        let mut word = is_prime[word_idx];

        while word != 0 {
            let bit_idx = word.trailing_zeros() as usize;
            let i = word_idx * 64 + bit_idx;

            if i >= odd_count {
                break; // Past the end of valid bits
            }

            if sender.send(2 * i + 3).is_err() {
                return; // Receiver dropped, stop sending
            }
            word &= word - 1; // Clear the lowest set bit
        }
    }
}

/// Variation 5: Segmented Sieve with Streaming
///
/// Combines segmented processing with streaming output.
/// - Memory: O(sqrt(n) + segment_size) instead of O(n)
/// - Segments are bit-packed and odd-only for efficiency
/// - Streams primes as each segment completes
/// - Best for very large limits (billions+)
/// - Segment size: 32KB (fits in L1 cache)
fn find_primes_v5_streaming(limit: usize, sender: Sender<usize>) {
    if limit < 2 {
        return;
    }
    if limit == 2 {
        let _ = sender.send(2);
        return;
    }

    // Step 1: Find small primes up to sqrt(limit) using v2 (odd-only)
    let sqrt_limit = (limit as f64).sqrt() as usize;
    let small_primes = find_primes_v2(sqrt_limit);

    // Send all small primes first
    for &prime in &small_primes {
        if sender.send(prime).is_err() {
            return; // Receiver dropped
        }
    }

    // Step 2: Process segments (limit is already rounded to segment boundary)

    // Helper function for bit operations
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    // Start from first odd number after sqrt_limit
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Allocate segment buffer once (always full segment size)
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;
    let mut segment = vec![0_u64; segment_words];

    while low <= limit {
        // Each segment is exactly SEGMENT_SIZE_NUMBERS (aligned boundary)
        let high = low + SEGMENT_SIZE_NUMBERS - 1;

        // Reinitialize entire segment (all bits to 1 = prime)
        segment.fill(!0_u64);

        // Step 3: For each small prime > 2, mark its multiples in this segment
        for &p in small_primes.iter().skip(1) {
            // Find first odd multiple of p in [low, high]
            let mut start = ((low + p - 1) / p) * p;
            if start % 2 == 0 {
                start += p; // Make it odd
            }

            // Mark multiples as composite
            while start <= high {
                let idx = (start - low) / 2;
                clear_bit(&mut segment, idx);
                start += p * 2; // Skip to next odd multiple
            }
        }

        // Step 4: Send primes from this segment
        for word_idx in 0..segment_words {
            let mut word = segment[word_idx];

            while word != 0 {
                let bit_idx = word.trailing_zeros() as usize;
                let idx = word_idx * 64 + bit_idx;

                let num = low + idx * 2;

                if num < limit {
                    if sender.send(num).is_err() {
                        return; // Receiver dropped, stop sending
                    }
                }

                word &= word - 1; // Clear lowest set bit
            }
        }

        // Move to next segment
        low = high + 2; // Next odd number
    }
}

/// Variation 6: Segmented Sieve with Batched Streaming
///
/// Sends entire segments as Vec<usize> for reduced channel overhead.
/// - Memory: O(sqrt(n) + segment_size) instead of O(n)
/// - Segments are bit-packed and odd-only for efficiency
/// - Sends one Vec per segment (massive reduction in channel overhead)
/// - Best for very large limits (billions+) with parallelization potential
/// - Segment size: 32KB (fits in L1 cache)
pub fn find_primes_v6_streaming(limit: usize, sqrt_limit: usize, sender: Sender<Vec<usize>>) {
    if limit < 2 {
        return;
    }
    if limit == 2 {
        let _ = sender.send(vec![2]);
        return;
    }

    // Step 1: Find small primes up to sqrt_limit using v2 (odd-only)
    let small_primes = find_primes_v2(sqrt_limit);

    // Send all small primes as first batch
    if sender.send(small_primes.clone()).is_err() {
        return; // Receiver dropped
    }

    // Step 2: Process segments (limit is already rounded to segment boundary)

    // Helper function for bit operations
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    // Start from first odd number after sqrt_limit
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Allocate segment buffer once (always full segment size)
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;
    let mut segment = vec![0_u64; segment_words];

    while low <= limit {
        // Each segment is exactly SEGMENT_SIZE_NUMBERS (aligned boundary)
        let high = low + SEGMENT_SIZE_NUMBERS - 1;

        // Reinitialize entire segment (all bits to 1 = prime)
        segment.fill(!0_u64);

        // Step 3: For each small prime > 2, mark its multiples in this segment
        for &p in small_primes.iter().skip(1) {
            // Find first odd multiple of p in [low, high]
            let mut start = ((low + p - 1) / p) * p;
            if start % 2 == 0 {
                start += p; // Make it odd
            }

            // Mark multiples as composite
            while start <= high {
                let idx = (start - low) / 2;
                clear_bit(&mut segment, idx);
                start += p * 2; // Skip to next odd multiple
            }
        }

        // Step 4: Collect primes from this segment into a Vec
        let mut segment_primes = Vec::new();
        for word_idx in 0..segment_words {
            let mut word = segment[word_idx];

            while word != 0 {
                let bit_idx = word.trailing_zeros() as usize;
                let idx = word_idx * 64 + bit_idx;

                let num = low + idx * 2;
                if num < limit {
                    segment_primes.push(num);
                }

                word &= word - 1; // Clear lowest set bit
            }
        }

        // Send entire segment at once
        if sender.send(segment_primes).is_err() {
            return; // Receiver dropped, stop sending
        }

        // Move to next segment
        low = high + 2; // Next odd number
    }
}

/// Variation 7: Segmented Sieve with Raw Segment Streaming
///
/// Sends raw bit-packed segments for consumer-side unpacking.
/// - Memory: O(sqrt(n) + segment_size) instead of O(n)
/// - Segments are bit-packed and odd-only for efficiency
/// - Sends raw Vec<u64> per segment (consumer unpacks in parallel)
/// - ~10% faster producer than v6 (no unpacking overhead)
/// - Best for very large limits with parallel consumers
/// - Segment size: 32KB (fits in L1 cache)
pub fn find_primes_v7_streaming(limit: usize, sqrt_limit: usize, sender: Sender<SegmentData>) {
    // Step 1: Find small primes up to sqrt_limit using v2 (odd-only)
    let small_primes = find_primes_v2(sqrt_limit);

    // Send small primes as a packed segment (consumer will unpack)
    // For simplicity, we'll pack them into a pseudo-segment format
    let small_primes_bits = pack_primes_to_bits(&small_primes);
    if sender
        .send(SegmentData {
            bits: small_primes_bits,
            low: 3,
            high: sqrt_limit,
        })
        .is_err()
    {
        return; // Receiver dropped
    }

    // Step 2: Process segments

    // Helper function for bit operations
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    // Start from first odd number after sqrt_limit
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Allocate segment buffer once (always full segment size)
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;
    let mut segment = vec![0_u64; segment_words];

    while low <= limit {
        // Each segment is exactly SEGMENT_SIZE_NUMBERS (aligned boundary)
        let high = low + SEGMENT_SIZE_NUMBERS - 1;

        // Reinitialize entire segment (all bits to 1 = prime)
        segment.fill(!0_u64);

        // Step 3: For each small prime > 2, mark its multiples in this segment
        for &p in small_primes.iter().skip(1) {
            // Find first odd multiple of p in [low, high]
            let mut start = ((low + p - 1) / p) * p;
            if start % 2 == 0 {
                start += p; // Make it odd
            }

            // Mark multiples as composite
            while start <= high {
                let idx = (start - low) / 2;
                clear_bit(&mut segment, idx);
                start += p * 2; // Skip to next odd multiple
            }
        }

        // Step 4: Send raw segment (no unpacking!)
        if sender
            .send(SegmentData {
                bits: segment.clone(),
                low,
                high,
            })
            .is_err()
        {
            return; // Receiver dropped, stop sending
        }

        // Move to next segment
        low = high + 2; // Next odd number
    }
}

/// Helper to pack a list of primes into bit-packed format
/// Used by v7 for the initial small_primes batch
fn pack_primes_to_bits(primes: &[usize]) -> Vec<u64> {
    if primes.is_empty() {
        return vec![];
    }

    // Special handling for primes that include 2
    let has_two = primes.first() == Some(&2);
    let odd_primes: Vec<usize> = primes.iter().copied().filter(|&p| p > 2).collect();

    if odd_primes.is_empty() {
        // Only prime 2
        return vec![1_u64];
    }

    let min_odd = odd_primes[0];
    let max_odd = odd_primes[odd_primes.len() - 1];

    // Calculate size needed for odd-only bit array
    let range = max_odd - min_odd;
    let bits_needed = range / 2 + 1;
    let words_needed = (bits_needed + 63) / 64;

    let mut bits = vec![0_u64; words_needed];

    // Set bits for each odd prime
    for &prime in &odd_primes {
        let idx = (prime - min_odd) / 2;
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] |= 1_u64 << bit_idx;
    }

    // If we have prime 2, prepend it as a special marker
    // Consumer needs to handle this specially
    if has_two {
        // For now, just include it in the range and rely on consumer
        // to check low/high bounds
    }

    bits
}

/// Variation 8: Parallelized Segmented Sieve with Batched Streaming
///
/// Multiple worker threads generate segments in parallel, consumer reorders.
/// - Memory: O(sqrt(n) + segment_size * num_workers) peak
/// - Segments processed in parallel by worker pool
/// - Workers unpack segments to Vec<usize> before sending (like v6)
/// - Consumer reorders and writes segments sequentially
/// - Best for very large limits on multi-core systems
/// - Segment size: 32KB (fits in L1 cache per core)
/// - Scales linearly with CPU cores
pub fn find_primes_v8_parallel(
    limit: usize,
    sqrt_limit: usize,
    sender: Sender<SegmentPrimes>,
    num_workers: usize,
) {
    if limit < 2 {
        return;
    }

    // Step 1: Find small primes up to sqrt_limit using v2 (odd-only)
    let small_primes = Arc::new(find_primes_v2(sqrt_limit));

    // Send small primes as first segment (already unpacked)
    if sender
        .send(SegmentPrimes {
            primes: (*small_primes).clone(),
            segment_id: 0,
        })
        .is_err()
    {
        return; // Receiver dropped
    }

    // Step 2: Calculate segment ranges
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Calculate total number of segments
    let total_range = if limit >= low {
        limit - low + 1
    } else {
        return; // No segments needed
    };
    let total_segments = (total_range + SEGMENT_SIZE_NUMBERS - 1) / SEGMENT_SIZE_NUMBERS;

    // Step 3: Spawn worker threads
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;

    thread::scope(|scope| {
        for worker_id in 0..num_workers {
            let sender = sender.clone();
            let small_primes = Arc::clone(&small_primes);

            scope.spawn(move || {
                // Helper function for bit operations
                #[inline]
                fn clear_bit(bits: &mut [u64], idx: usize) {
                    let word_idx = idx / 64;
                    let bit_idx = idx % 64;
                    bits[word_idx] &= !(1_u64 << bit_idx);
                }

                // Allocate segment buffer for this worker
                let mut segment = vec![0_u64; segment_words];

                // Process segments assigned to this worker
                for segment_idx in (worker_id..total_segments).step_by(num_workers) {
                    let seg_low = low + segment_idx * SEGMENT_SIZE_NUMBERS;
                    let seg_high = (seg_low + SEGMENT_SIZE_NUMBERS - 1).min(limit);

                    // Reinitialize segment (all bits to 1 = prime)
                    segment.fill(!0_u64);

                    // Mark composites using small primes
                    for &p in small_primes.iter().skip(1) {
                        // Find first odd multiple of p in [seg_low, seg_high]
                        let mut start = ((seg_low + p - 1) / p) * p;
                        if start % 2 == 0 {
                            start += p; // Make it odd
                        }

                        // Mark multiples as composite
                        while start <= seg_high {
                            let idx = (start - seg_low) / 2;
                            clear_bit(&mut segment, idx);
                            start += p * 2; // Skip to next odd multiple
                        }
                    }

                    // Unpack segment into Vec<usize> (producer-side unpacking like v6)
                    let mut segment_primes = Vec::new();
                    for word_idx in 0..segment_words {
                        let mut word = segment[word_idx];

                        while word != 0 {
                            let bit_idx = word.trailing_zeros() as usize;
                            let idx = word_idx * 64 + bit_idx;

                            let num = seg_low + idx * 2;
                            if num <= seg_high {
                                segment_primes.push(num);
                            }

                            word &= word - 1; // Clear lowest set bit
                        }
                    }

                    // Send unpacked primes with proper ID (segment_idx + 1, since 0 is small primes)
                    if sender
                        .send(SegmentPrimes {
                            primes: segment_primes,
                            segment_id: segment_idx + 1,
                        })
                        .is_err()
                    {
                        return; // Receiver dropped, stop this worker
                    }
                }
            });
        }
    });
}

/// Variation 9 with N consumers: Parallel Segmented Sieve with Multiple Consumers
/// Distributes segments across N consumers for maximum I/O parallelization
/// - Parallel workers compute segments
/// - Segments distributed round-robin to N consumers
/// - Each consumer writes to primes_{id}.bin
pub fn find_primes_v9_multi_consumers(
    limit: usize,
    sqrt_limit: usize,
    senders: Vec<SyncSender<SegmentPrimes>>,
    num_workers: usize,
) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }

    let num_consumers = senders.len();
    if num_consumers == 0 {
        return vec![];
    }

    // Step 1: Find small primes up to sqrt_limit using v2 (odd-only)
    let small_primes = Arc::new(find_primes_v2(sqrt_limit));

    // Step 2: Calculate segment ranges
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Calculate total number of segments
    let total_range = if limit >= low {
        limit - low + 1
    } else {
        return vec![];
    };
    let total_segments = (total_range + SEGMENT_SIZE_NUMBERS - 1) / SEGMENT_SIZE_NUMBERS;

    // Step 3: Spawn worker threads with atomic work queue
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;
    let next_segment = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        for _worker_id in 0..num_workers {
            let senders = senders.clone();
            let small_primes = Arc::clone(&small_primes);
            let next_segment = Arc::clone(&next_segment);

            scope.spawn(move || {
                // Helper function for bit operations
                #[inline]
                fn clear_bit(bits: &mut [u64], idx: usize) {
                    let word_idx = idx / 64;
                    let bit_idx = idx % 64;
                    bits[word_idx] &= !(1_u64 << bit_idx);
                }

                // Allocate segment buffer for this worker
                let mut segment = vec![0_u64; segment_words];

                // Workers pull segments sequentially from atomic counter
                loop {
                    let segment_idx = next_segment.fetch_add(1, Ordering::Relaxed);
                    if segment_idx >= total_segments {
                        break;
                    }
                    let seg_low = low + segment_idx * SEGMENT_SIZE_NUMBERS;
                    let seg_high = (seg_low + SEGMENT_SIZE_NUMBERS - 1).min(limit);

                    // Reinitialize segment (all bits to 1 = prime)
                    segment.fill(!0_u64);

                    // Mark composites using small primes
                    for &p in small_primes.iter().skip(1) {
                        // Find first odd multiple of p in [seg_low, seg_high]
                        let mut start = ((seg_low + p - 1) / p) * p;
                        if start % 2 == 0 {
                            start += p; // Make it odd
                        }

                        // Mark multiples as composite
                        while start <= seg_high {
                            let idx = (start - seg_low) / 2;
                            clear_bit(&mut segment, idx);
                            start += p * 2; // Skip to next odd multiple
                        }
                    }

                    // Unpack segment into Vec<usize>
                    let mut segment_primes = Vec::new();
                    for word_idx in 0..segment_words {
                        let mut word = segment[word_idx];

                        while word != 0 {
                            let bit_idx = word.trailing_zeros() as usize;
                            let idx = word_idx * 64 + bit_idx;

                            let num = seg_low + idx * 2;
                            if num <= seg_high {
                                segment_primes.push(num);
                            }

                            word &= word - 1; // Clear lowest set bit
                        }
                    }

                    // Segment numbering starts at 1 (0 is reserved for small primes)
                    let segment_id = segment_idx + 1;
                    let segment_data = SegmentPrimes {
                        primes: segment_primes,
                        segment_id,
                    };

                    // Route to consumer based on segment_id: segment S → consumer ((S-1) % N)
                    let consumer_idx = ((segment_id - 1) % num_consumers) as usize;
                    if senders[consumer_idx].send(segment_data).is_err() {
                        break; // Receiver dropped, stop this worker
                    }
                }
            });
        }
    });

    // Clone from Arc to return (Arc will be dropped when thread::scope ends)
    (*small_primes).clone()
}

pub fn find_primes(limit: usize, variation: u32) -> Vec<usize> {
    match variation {
        1 => find_primes_v1(limit),
        2 => find_primes_v2(limit),
        3 => find_primes_v3(limit),
        4 => find_primes_v4(limit),
        5 => find_primes_v5(limit),
        _ => {
            eprintln!("Unknown variation {}, using variation 1", variation);
            find_primes_v1(limit)
        }
    }
}

/// Variation 1: Basic Sieve of Eratosthenes
///
/// Classic implementation that marks all composite numbers.
/// - Time complexity: O(n log log n)
/// - Space complexity: O(n) - 1 byte per number
/// - Processes all numbers including even numbers
/// - Simple and straightforward implementation
fn find_primes_v1(limit: usize) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }

    let mut is_prime = vec![true; limit + 1];
    is_prime[0] = false;
    is_prime[1] = false;

    for i in 2..=((limit as f64).sqrt() as usize) {
        if is_prime[i] {
            let mut j = i * i;
            while j <= limit {
                is_prime[j] = false;
                j += i;
            }
        }
    }

    is_prime
        .iter()
        .enumerate()
        .filter_map(|(num, &prime)| if prime { Some(num) } else { None })
        .collect()
}

/// Variation 2: Odd-Only Sieve of Eratosthenes
///
/// Optimized version that only processes odd numbers (skips all evens except 2).
/// - Time complexity: O(n log log n) - same asymptotic, but ~40% faster in practice
/// - Space complexity: O(n/2) - half the memory usage
/// - Index mapping: is_prime[i] represents the number (2*i + 3)
/// - Only marks odd multiples of odd primes
/// - Best general-purpose optimization with simple implementation
fn find_primes_v2(limit: usize) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }
    if limit == 2 {
        return vec![2];
    }

    // Start with 2, then find all odd primes
    let mut primes = vec![2];

    // Array size is half since we only track odd numbers
    let size = (limit - 1) / 2;
    let mut is_prime = vec![true; size];

    let sqrt_limit = ((limit as f64).sqrt() as usize - 1) / 2;

    for i in 0..=sqrt_limit {
        if is_prime[i] {
            let p = 2 * i + 3;
            let mut j = (p * p - 3) / 2;
            while j < size {
                is_prime[j] = false;
                j += p;
            }
        }
    }

    for (i, &is_p) in is_prime.iter().enumerate() {
        if is_p {
            primes.push(2 * i + 3);
        }
    }

    primes
}

/// Variation 4: Odd-Only + Bit-packed (Combined v2 and v3)
///
/// Maximum memory efficiency: Only odd numbers + bit packing
/// - Memory: 1 bit per odd number (16x savings vs Vec<bool>)
/// - Time complexity: O(n log log n) + bit manipulation overhead
/// - Space complexity: O(n/128) - 16x compression
/// - Index mapping: bit i represents number (2*i + 3)
fn find_primes_v4(limit: usize) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }
    if limit == 2 {
        return vec![2];
    }

    // Start with 2
    let mut primes = vec![2];

    // Only track odd numbers: 3, 5, 7, 9, 11, ...
    // Index i represents number (2*i + 3)
    let odd_count = (limit - 1) / 2;
    let size = (odd_count + 63) / 64; // Number of u64 words needed
    let mut is_prime = vec![!0_u64; size]; // All bits set to 1 (true)

    // Helper: Get bit at position idx
    #[inline]
    fn get_bit(bits: &[u64], idx: usize) -> bool {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (bits[word_idx] & (1_u64 << bit_idx)) != 0
    }

    // Helper: Clear bit at position idx
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    let sqrt_limit = ((limit as f64).sqrt() as usize - 1) / 2;

    // Sieve odd numbers
    for i in 0..=sqrt_limit.min(odd_count - 1) {
        if get_bit(&is_prime, i) {
            let p = 2 * i + 3;

            // Mark odd multiples of p as composite
            let mut j = (p * p - 3) / 2;
            while j < odd_count {
                clear_bit(&mut is_prime, j);
                j += p;
            }
        }
    }

    // Collect all odd primes (optimized: iterate word-by-word, skip to set bits)
    for word_idx in 0..is_prime.len() {
        let mut word = is_prime[word_idx];

        while word != 0 {
            let bit_idx = word.trailing_zeros() as usize;
            let i = word_idx * 64 + bit_idx;

            if i >= odd_count {
                break; // Past the end of valid bits
            }

            primes.push(2 * i + 3);
            word &= word - 1; // Clear the lowest set bit
        }
    }

    primes
}

/// Variation 5: Segmented Sieve with Bit-packing and Odd-only
///
/// Processes primes in segments to minimize peak memory usage.
/// - Memory: O(sqrt(n) + segment_size) instead of O(n)
/// - Segments are bit-packed and odd-only for efficiency
/// - Best for very large limits (billions+)
/// - Segment size: 32KB (fits in L1 cache)
/// - Time complexity: O(n log log n)
/// - Space complexity: O(sqrt(n)) peak memory
fn find_primes_v5(limit: usize) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }
    if limit == 2 {
        return vec![2];
    }

    // Step 1: Find small primes up to sqrt(limit) using v2 (odd-only)
    let sqrt_limit = (limit as f64).sqrt() as usize;
    let small_primes = find_primes_v2(sqrt_limit);

    // Start with all small primes
    let mut all_primes = small_primes.clone();

    // Step 2: Process segments (limit is already rounded to segment boundary)

    // Helper function for bit operations
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    // Start from first odd number after sqrt_limit
    let mut low = (sqrt_limit + 1) | 1; // Make odd
    if low % 2 == 0 {
        low += 1;
    }

    // Allocate segment buffer once (always full segment size)
    let segment_words = (SEGMENT_SIZE_BITS + 63) / 64;
    let mut segment = vec![0_u64; segment_words];

    while low <= limit {
        // Each segment is exactly SEGMENT_SIZE_NUMBERS (aligned boundary)
        let high = low + SEGMENT_SIZE_NUMBERS - 1;

        // Reinitialize entire segment (all bits to 1 = prime)
        segment.fill(!0_u64);

        // Step 3: For each small prime > 2, mark its multiples in this segment
        for &p in small_primes.iter().skip(1) {
            // Find first odd multiple of p in [low, high]
            let mut start = ((low + p - 1) / p) * p;
            if start % 2 == 0 {
                start += p; // Make it odd
            }

            // Mark multiples as composite
            while start <= high {
                let idx = (start - low) / 2;
                clear_bit(&mut segment, idx);
                start += p * 2; // Skip to next odd multiple
            }
        }

        // Step 4: Collect primes from this segment
        for word_idx in 0..segment_words {
            let mut word = segment[word_idx];

            while word != 0 {
                let bit_idx = word.trailing_zeros() as usize;
                let idx = word_idx * 64 + bit_idx;

                let num = low + idx * 2;
                all_primes.push(num);

                word &= word - 1; // Clear lowest set bit
            }
        }

        // Move to next segment
        low = high + 2; // Next odd number
    }

    all_primes
}

/// Variation 3: Bit-packed Sieve using Vec<u64>
///
/// Uses 1 bit per number (8x memory savings vs Vec<bool>)
/// - Memory: 1 bit per number (packed in u64 words)
/// - Time complexity: O(n log log n) + bit manipulation overhead
/// - Space complexity: O(n/64) - 8x compression vs Vec<bool>
/// - Aligned for 64-bit operations
fn find_primes_v3(limit: usize) -> Vec<usize> {
    if limit < 2 {
        return vec![];
    }

    // Each u64 holds 64 bits
    let size = (limit + 64) / 64;
    let mut is_prime = vec![!0_u64; size]; // All bits set to 1 (true)

    // Helper: Get bit at position idx
    #[inline]
    fn get_bit(bits: &[u64], idx: usize) -> bool {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (bits[word_idx] & (1_u64 << bit_idx)) != 0
    }

    // Helper: Clear bit at position idx
    #[inline]
    fn clear_bit(bits: &mut [u64], idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        bits[word_idx] &= !(1_u64 << bit_idx);
    }

    // 0 and 1 are not prime
    clear_bit(&mut is_prime, 0);
    clear_bit(&mut is_prime, 1);

    // Sieve of Eratosthenes
    for i in 2..=((limit as f64).sqrt() as usize) {
        if get_bit(&is_prime, i) {
            let mut j = i * i;
            while j <= limit {
                clear_bit(&mut is_prime, j);
                j += i;
            }
        }
    }

    // Collect all primes
    let mut primes = Vec::new();
    for i in 0..=limit {
        if get_bit(&is_prime, i) {
            primes.push(i);
        }
    }

    primes
}
