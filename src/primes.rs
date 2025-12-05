use std::sync::mpsc::Sender;

pub fn find_primes_streaming(limit: usize, variation: u32, sender: Sender<usize>) {
    match variation {
        1 => find_primes_v1_streaming(limit, sender),
        2 => find_primes_v2_streaming(limit, sender),
        3 => find_primes_v3_streaming(limit, sender),
        4 => find_primes_v4_streaming(limit, sender),
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
                break;  // Past the end of valid bits
            }

            if sender.send(2 * i + 3).is_err() {
                return; // Receiver dropped, stop sending
            }
            word &= word - 1;  // Clear the lowest set bit
        }
    }
}

pub fn find_primes(limit: usize, variation: u32) -> Vec<usize> {
    match variation {
        1 => find_primes_v1(limit),
        2 => find_primes_v2(limit),
        3 => find_primes_v3(limit),
        4 => find_primes_v4(limit),
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
                break;  // Past the end of valid bits
            }

            primes.push(2 * i + 3);
            word &= word - 1;  // Clear the lowest set bit
        }
    }

    primes
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
