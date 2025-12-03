use crate::scan;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

pub fn generate_and_scan(digits: usize) {
    // Generate random digits
    let random_digits = generate_random_digits(digits);

    println!("Generated {} random digits:", digits);
    println!("{}", random_digits);
    println!();

    // Scan for primes
    println!("Scanning for primes in random digits...");
    scan::scan_for_primes(&random_digits);
}

fn generate_random_digits(count: usize) -> String {
    let random_state = RandomState::new();
    let mut digits = String::with_capacity(count);

    for i in 0..count {
        let mut hasher = random_state.build_hasher();
        i.hash(&mut hasher);
        let random_value = hasher.finish();
        let digit = random_value % 10;
        digits.push_str(&digit.to_string());
    }

    digits
}
