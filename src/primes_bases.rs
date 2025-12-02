use crate::storage;

pub fn run(pal_only: bool, pal: Option<String>) {
    match storage::load_all_primes() {
        Ok(primes) => {
            // Track palindrome counts for each base (index 0 = base 2, index 60 = base 62)
            let mut base_palindrome_counts = vec![0; 61];

            // Print header
            let header = vec![
                "10", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43",
                "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57",
                "58", "59", "60", "61", "62", "total",
            ];
            println!("{}", header.join("\t"));

            // Print each prime with tab-separated columns
            for prime in primes {
                let base_representations: Vec<String> =
                    (2..=62).map(|base| to_base(prime, base)).collect();

                // Count palindromes (skip base 10 in base_representations to avoid double counting)
                let mut palindrome_count = 0;
                if is_palindrome(&prime.to_string()) {
                    palindrome_count += 1;
                    base_palindrome_counts[8] += 1; // base 10 is at index 8
                }
                for (i, repr) in base_representations.iter().enumerate() {
                    let base = i + 2; // base_representations[0] is base 2
                    if base == 10 {
                        continue; // Skip base 10 to avoid double counting
                    }
                    if is_palindrome(repr) {
                        palindrome_count += 1;
                        base_palindrome_counts[i] += 1;
                    }
                }

                // Filter by specific palindrome value if provided
                if let Some(ref pal_value) = pal {
                    let mut found_match = false;

                    // Check base 10
                    if is_palindrome(&prime.to_string()) && prime.to_string() == *pal_value {
                        found_match = true;
                    }

                    // Check other bases
                    if !found_match {
                        for repr in &base_representations {
                            if is_palindrome(repr) && repr == pal_value {
                                found_match = true;
                                break;
                            }
                        }
                    }

                    if !found_match {
                        continue; // Skip this row
                    }
                }

                let mut row = vec![prime.to_string()];

                // Add base representations (bases 2-36)
                for (i, repr) in base_representations.iter().enumerate() {
                    let base = i + 2;
                    if base == 10 {
                        row.push(colorize_duplicate_base10(repr));
                    } else {
                        row.push(format_value(repr, pal_only));
                    }
                }

                // Add palindrome count
                row.push(palindrome_count.to_string());

                println!("{}", row.join("\t"));
            }

            // Print footer with totals
            let total_palindromes: usize = base_palindrome_counts.iter().sum();
            let mut footer = vec![base_palindrome_counts[8].to_string()]; // base 10

            // Add counts for bases 2-24
            for (_i, &count) in base_palindrome_counts.iter().enumerate() {
                footer.push(count.to_string());
            }

            // Add total palindromes
            footer.push(total_palindromes.to_string());

            println!("{}", footer.join("\t"));
        }
        Err(e) => eprintln!("Error loading primes.txt: {}", e),
    }
}

fn to_base(mut num: usize, base: usize) -> String {
    if num == 0 {
        return "0".to_string();
    }

    let mut digits = Vec::new();
    while num > 0 {
        let digit = num % base;
        let digit_char = if digit < 10 {
            (digit as u8 + b'0') as char
        } else if digit < 36 {
            (digit as u8 - 10 + b'A') as char
        } else {
            // For bases > 36, use lowercase letters (36='a', 37='b', etc.)
            (digit as u8 - 36 + b'a') as char
        };
        digits.push(digit_char);
        num /= base;
    }
    digits.reverse();
    digits.iter().collect()
}

fn is_palindrome(s: &str) -> bool {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();

    // Don't count single character strings as palindromes
    if len <= 1 {
        return false;
    }

    for i in 0..len / 2 {
        if chars[i] != chars[len - 1 - i] {
            return false;
        }
    }
    true
}

fn colorize_if_palindrome(s: &str) -> String {
    if is_palindrome(s) {
        format!("\x1b[1;93m{}\x1b[0m", s)
    } else {
        s.to_string()
    }
}

fn colorize_duplicate_base10(s: &str) -> String {
    // Color duplicate base 10 in dim gray
    format!("\x1b[90m{}\x1b[0m", s)
}

fn format_value(s: &str, pal_only: bool) -> String {
    if pal_only {
        if is_palindrome(s) {
            colorize_if_palindrome(s)
        } else {
            "-".to_string()
        }
    } else {
        colorize_if_palindrome(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_palindrome_empty_string() {
        assert!(!is_palindrome(""));
    }

    #[test]
    fn test_is_palindrome_single_char() {
        assert!(!is_palindrome("a"));
        assert!(!is_palindrome("1"));
    }

    #[test]
    fn test_is_palindrome_two_chars_same() {
        assert!(is_palindrome("aa"));
        assert!(is_palindrome("11"));
        assert!(is_palindrome("ZZ"));
    }

    #[test]
    fn test_is_palindrome_two_chars_different() {
        assert!(!is_palindrome("ab"));
        assert!(!is_palindrome("12"));
    }

    #[test]
    fn test_is_palindrome_odd_length() {
        assert!(is_palindrome("aba"));
        assert!(is_palindrome("12321"));
        assert!(is_palindrome("racecar"));
        assert!(is_palindrome("A1A"));
    }

    #[test]
    fn test_is_palindrome_even_length() {
        assert!(is_palindrome("abba"));
        assert!(is_palindrome("1221"));
        assert!(is_palindrome("ABCCBA"));
    }

    #[test]
    fn test_is_palindrome_non_palindromes() {
        assert!(!is_palindrome("abc"));
        assert!(!is_palindrome("123"));
        assert!(!is_palindrome("hello"));
        assert!(!is_palindrome("12345"));
    }

    #[test]
    fn test_is_palindrome_case_sensitive() {
        // Function is case-sensitive
        // These are NOT palindromes because corresponding chars differ in case
        assert!(!is_palindrome("Aa"));
        assert!(!is_palindrome("Aba"));
        assert!(!is_palindrome("aBA"));
        // These ARE palindromes because all corresponding chars match
        assert!(is_palindrome("ABA"));
        assert!(is_palindrome("aba"));
        assert!(is_palindrome("AbA")); // First and last are both 'A'
    }

    #[test]
    fn test_is_palindrome_base_representations() {
        // Test with actual base representations
        assert!(is_palindrome("101"));  // binary palindrome
        assert!(is_palindrome("1111")); // binary palindrome
        assert!(!is_palindrome("1010")); // not a palindrome
        assert!(is_palindrome("121"));  // base-3 palindrome
    }

    #[test]
    fn test_to_base_basic() {
        // Base 2 (binary)
        assert_eq!(to_base(5, 2), "101");
        assert_eq!(to_base(10, 2), "1010");

        // Base 10 (decimal)
        assert_eq!(to_base(123, 10), "123");

        // Base 16 (hexadecimal)
        assert_eq!(to_base(255, 16), "FF");
        assert_eq!(to_base(16, 16), "10");
    }

    #[test]
    fn test_to_base_extended() {
        // Base 36 (0-9, A-Z)
        assert_eq!(to_base(35, 36), "Z");
        assert_eq!(to_base(36, 36), "10");

        // Base 37-62 (using lowercase letters for values 36+)
        assert_eq!(to_base(36, 37), "a"); // value 36 in base 37 is 'a'
        assert_eq!(to_base(37, 37), "10");

        // Base 62 (0-9, A-Z, a-z)
        assert_eq!(to_base(61, 62), "z"); // value 61 in base 62 is 'z'
        assert_eq!(to_base(62, 62), "10");
        assert_eq!(to_base(0, 62), "0");

        // Additional base 62 examples
        assert_eq!(to_base(10, 62), "A");  // value 10 is 'A'
        assert_eq!(to_base(35, 62), "Z");  // value 35 is 'Z'
        assert_eq!(to_base(36, 62), "a");  // value 36 is 'a'
    }

    #[test]
    fn test_to_base_digit_ranges() {
        // Verify digit representations
        // 0-9 use '0'-'9'
        assert_eq!(to_base(9, 10), "9");

        // 10-35 use 'A'-'Z' for bases > 10
        assert_eq!(to_base(10, 16), "A");
        assert_eq!(to_base(15, 16), "F");
        assert_eq!(to_base(35, 36), "Z");

        // 36-61 use 'a'-'z' for bases > 36
        assert_eq!(to_base(36, 62), "a");
        assert_eq!(to_base(61, 62), "z");
    }
}
