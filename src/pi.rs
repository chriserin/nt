use rug::Float;
use rug::ops::Pow;
use crate::storage;

pub fn calculate_and_print(digits: usize) {
    // Calculate precision needed in bits (roughly 3.32 bits per decimal digit)
    let precision = ((digits as f64) * 3.32 * 1.5) as u32;

    // Use Machin's formula: π/4 = 4*arctan(1/5) - arctan(1/239)
    let pi = machin_formula(precision);

    // Print pi to the requested number of decimal places
    println!("π to {} decimal places:", digits);
    let pi_str = pi.to_string_radix(10, Some(digits));
    println!("{}", pi_str);

    // Scan for primes in pi digits
    println!("\nScanning for primes in π...");
    scan_for_primes(&pi_str);
}

fn scan_for_primes(pi_str: &str) {
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

    // Remove the "3." prefix to work with just the digits
    let pi_digits = pi_str.replace("3.", "3");

    println!("Pi digits to scan: {} digits", pi_digits.len());
    println!("Number of primes (4+ digits) loaded: {}", primes.len());
    println!();

    let mut found_primes = Vec::new();

    // Check each prime to see if it appears in pi
    for prime in &primes {
        let prime_str = prime.to_string();

        // Find all occurrences of this prime in pi
        let mut start = 0;
        while let Some(pos) = pi_digits[start..].find(&prime_str) {
            let actual_pos = start + pos;
            found_primes.push((*prime, actual_pos));
            start = actual_pos + 1;
        }
    }

    // Sort by position
    found_primes.sort_by_key(|(_, pos)| *pos);

    println!("Found {} prime occurrences in π:", found_primes.len());
    println!();
    println!("Prime\tPosition\tContext");
    println!("-----\t--------\t-------");

    for (prime, pos) in found_primes.iter().take(50) {
        let prime_str = prime.to_string();
        let context_start = pos.saturating_sub(3);
        let context_end = (pos + prime_str.len() + 3).min(pi_digits.len());
        let context = &pi_digits[context_start..context_end];

        // Highlight the prime in context
        let prefix = &context[0..(pos - context_start)];
        let suffix = &context[(pos - context_start + prime_str.len())..];

        println!("{}\t{}\t\t{}[{}]{}", prime, pos, prefix, prime_str, suffix);
    }

    if found_primes.len() > 50 {
        println!("\n... and {} more", found_primes.len() - 50);
    }
}

pub(crate) fn machin_formula(precision: u32) -> Float {
    // π/4 = 4*arctan(1/5) - arctan(1/239)
    let five = Float::with_val(precision, 5);
    let two_thirty_nine = Float::with_val(precision, 239);
    let four = Float::with_val(precision, 4);

    let one_over_five = Float::with_val(precision, Float::with_val(precision, 1) / &five);
    let one_over_239 = Float::with_val(precision, Float::with_val(precision, 1) / &two_thirty_nine);

    let arctan_1_5 = arctan_series(&one_over_five, precision);
    let arctan_1_239 = arctan_series(&one_over_239, precision);

    let pi_over_4 = &four * arctan_1_5 - arctan_1_239;
    let pi = pi_over_4 * Float::with_val(precision, 4);

    pi
}

fn arctan_series(x: &Float, precision: u32) -> Float {
    // arctan(x) = x - x^3/3 + x^5/5 - x^7/7 + ...
    let mut sum = Float::with_val(precision, 0);
    let mut term = Float::with_val(precision, x);
    let x_squared = Float::with_val(precision, x.pow(2));
    let mut n = 1u32;
    let mut sign = 1; // Alternating sign: +1, -1, +1, -1, ...

    let tolerance = Float::with_val(precision, 10).pow(-(precision as i32) / 3);

    loop {
        let term_abs = Float::with_val(precision, term.abs_ref());

        if term_abs < tolerance {
            break;
        }

        let term_divided = Float::with_val(precision, &term / n);
        if sign == 1 {
            sum += &term_divided;
        } else {
            sum -= &term_divided;
        }

        term *= &x_squared;
        n += 2;
        sign *= -1; // Alternate the sign

        // Safety check to prevent infinite loops
        if n > 100000 {
            break;
        }
    }

    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const ACCURATE_PI: &str = "3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117068";

    #[test]
    fn test_pi_calculation() {
        let precision = 512;
        let pi = machin_formula(precision);
        let pi_str = pi.to_string_radix(10, Some(100));

        // Check that pi matches the first 100 digits
        assert!(
            pi_str.starts_with(ACCURATE_PI),
            "Expected pi to start with {}, but got {}",
            ACCURATE_PI,
            pi_str
        );
    }

    #[test]
    fn test_arctan_series() {
        let precision = 64;
        let x = Float::with_val(precision, 1.0);
        let result = arctan_series(&x, precision);

        // arctan(1) should be π/4 ≈ 0.7853981633974483
        let pi_over_4 = result.to_f64();
        assert!((pi_over_4 - 0.7853981633974483).abs() < 0.0001);
    }
}
