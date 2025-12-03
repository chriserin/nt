use crate::storage;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

fn shuffle<T>(vec: &mut Vec<T>) {
    let random_state = RandomState::new();
    let len = vec.len();

    for i in (1..len).rev() {
        // Generate random index from 0 to i (inclusive)
        let mut hasher = random_state.build_hasher();
        i.hash(&mut hasher);
        let j = (hasher.finish() as usize) % (i + 1);

        vec.swap(i, j);
    }
}

pub fn build_chain(overlap: usize, target_length: usize) {
    // Load primes from primes.txt
    let primes = match storage::load_all_primes() {
        Ok(primes) => primes,
        Err(e) => {
            eprintln!("Error loading primes.txt: {}", e);
            return;
        }
    };

    // Filter primes that have at least 'overlap' digits
    let min_digits = overlap + 1; // Need at least overlap + 1 digits to be useful
    let valid_primes: Vec<String> = primes
        .into_iter()
        .map(|p| p.to_string())
        .filter(|p| p.len() >= min_digits)
        .collect();

    if valid_primes.is_empty() {
        eprintln!(
            "No primes with at least {} digits found in primes.txt",
            min_digits
        );
        return;
    }

    println!("Building chain with {} digit overlap...", overlap);
    println!("Target length: {} digits", target_length);
    println!("Available primes: {}", valid_primes.len());
    println!();

    // Build index: map from first N digits to list of primes starting with those digits
    let mut prefix_index: HashMap<String, Vec<String>> = HashMap::new();
    for prime in &valid_primes {
        if prime.len() >= overlap {
            let prefix = prime[..overlap].to_string();
            prefix_index
                .entry(prefix)
                .or_insert_with(Vec::new)
                .push(prime.clone());
        }
    }

    // Try to build a chain starting from different primes
    let mut best_chain = String::new();
    let mut best_primes = Vec::new();
    let mut attempts = 0;

    for start_prime in &valid_primes {
        attempts += 1;
        let (chain, chain_primes) =
            build_chain_from_start(start_prime, overlap, target_length, &prefix_index);

        if chain.len() > best_chain.len() {
            best_chain = chain;
            best_primes = chain_primes;
        }

        // If we reached target, we're done
        if best_chain.len() >= target_length {
            break;
        }
    }

    if best_chain.is_empty() {
        println!("Attempted chains: {}", attempts);
        println!("Could not build a chain. Try reducing the overlap value.");
        return;
    }

    println!("Attempted chains: {}", attempts);

    // Truncate to target length if we exceeded it
    if best_chain.len() > target_length {
        best_chain.truncate(target_length);
    }

    println!("Successfully built chain of {} digits!", best_chain.len());
    println!("\nChain:");
    println!("{}", best_chain);
    println!("\nPrimes used ({}):", best_primes.len());
    for (i, prime) in best_primes.iter().enumerate() {
        println!("{}. {}", i + 1, prime);
    }
}

fn build_chain_from_start(
    start_prime: &str,
    overlap: usize,
    target_length: usize,
    prefix_index: &HashMap<String, Vec<String>>,
) -> (String, Vec<String>) {
    let mut chain = start_prime.to_string();
    let mut used_primes = vec![start_prime.to_string()];
    let mut used_set = std::collections::HashSet::new();
    used_set.insert(start_prime.to_string());

    while chain.len() < target_length {
        // Get the last 'overlap' digits of current chain
        let chain_len = chain.len();
        if chain_len < overlap {
            break;
        }

        let suffix = &chain[chain_len - overlap..];

        // Find primes that start with this suffix
        let mut candidates = match prefix_index.get(suffix) {
            Some(primes) => primes.clone(),
            None => break, // No matching primes found
        };

        shuffle(&mut candidates);

        // Find a prime we haven't used yet
        let next_prime = candidates.iter().find(|p| !used_set.contains(*p));

        match next_prime {
            Some(prime) => {
                // Append the non-overlapping part
                let non_overlapping = &prime[overlap..];
                chain.push_str(non_overlapping);
                used_primes.push(prime.clone());
                used_set.insert(prime.clone());
            }
            None => break, // No unused primes found
        }
    }

    (chain, used_primes)
}
