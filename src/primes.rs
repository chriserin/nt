pub fn find_primes(limit: usize, variation: u32) -> Vec<usize> {
    match variation {
        1 => find_primes_v1(limit),
        2 => find_primes_v2(limit),
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
