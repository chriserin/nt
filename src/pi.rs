use rug::Float;
use rug::ops::Pow;
use crate::scan;

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
    // Remove the "3." prefix to work with just the digits
    let pi_digits = pi_str.replace("3.", "3");
    scan::scan_for_primes(&pi_digits);
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
