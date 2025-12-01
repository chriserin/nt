use crate::storage;

pub fn run(pal_only: bool, pal: Option<String>) {
    match storage::load_all_primes() {
        Ok(primes) => {
            // Track palindrome counts for each base (index 0 = base 2, index 34 = base 36)
            let mut base_palindrome_counts = vec![0; 35];

            // Print header
            let header = vec![
                "10", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                "30", "31", "32", "33", "34", "35", "36", "total",
            ];
            println!("{}", header.join("\t"));

            // Print each prime with tab-separated columns
            for prime in primes {
                let base_representations: Vec<String> =
                    (2..=36).map(|base| to_base(prime, base)).collect();

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
        } else {
            (digit as u8 - 10 + b'A') as char
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
