use crate::storage;

pub fn scan_for_primes(digit_str: &str) {
    // Load primes from primes.txt
    let primes = match storage::load_all_primes() {
        Ok(primes) => primes,
        Err(e) => {
            eprintln!("Error loading primes.txt: {}", e);
            return;
        }
    };

    // Filter to only primes with 4 or more digits
    let primes: Vec<usize> = primes.into_iter().filter(|p| *p >= 1000).collect();

    println!("Digits to scan: {} digits", digit_str.len());
    println!("Number of primes (4+ digits) loaded: {}", primes.len());
    println!();

    let mut found_primes = Vec::new();

    // Check each prime to see if it appears in the digit string
    for prime in &primes {
        let prime_str = prime.to_string();

        // Find all occurrences of this prime
        let mut start = 0;
        while let Some(pos) = digit_str[start..].find(&prime_str) {
            let actual_pos = start + pos;
            found_primes.push((*prime, actual_pos));
            start = actual_pos + 1;
        }
    }

    // Sort by position
    found_primes.sort_by_key(|(_, pos)| *pos);

    println!("Found {} prime occurrences:", found_primes.len());
    println!();
    println!("Prime\tPosition\tContext");
    println!("-----\t--------\t-------");

    for (prime, pos) in found_primes.iter().take(50) {
        let prime_str = prime.to_string();
        let context_start = pos.saturating_sub(3);
        let context_end = (pos + prime_str.len() + 3).min(digit_str.len());
        let context = &digit_str[context_start..context_end];

        // Highlight the prime in context
        let prefix = &context[0..(pos - context_start)];
        let suffix = &context[(pos - context_start + prime_str.len())..];

        println!("{}\t{}\t\t{}[{}]{}", prime, pos, prefix, prime_str, suffix);
    }

    if found_primes.len() > 50 {
        println!("\n... and {} more", found_primes.len() - 50);
    }
}
