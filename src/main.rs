mod primes;
mod primes_bases;
mod storage;

use clap::{Parser, Subcommand};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "nt")]
#[command(about = "Number Theory CLI - Various number theory programs", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Find all prime numbers up to a given limit")]
    Primes {
        #[arg(help = "The upper limit to search for primes")]
        limit: usize,
        #[arg(short, long, default_value = "1", help = "Algorithm variation to use")]
        variation: u32,
    },
    #[command(about = "Output primes from primes.txt as different bases")]
    PrimesBases {
        #[arg(long, help = "Only display palindromes, show dash for non-palindromes")]
        pal_only: bool,
        #[arg(long, help = "Only show rows containing this specific palindrome value")]
        pal: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Primes { limit, variation } => {
            let start = Instant::now();

            let primes = primes::find_primes(limit, variation);

            println!(
                "Finding primes up to {} (variation {})...",
                limit, variation
            );

            for &prime in &primes {
                match storage::save_property(prime, "prime") {
                    Ok(_) => println!("Saved: {}.txt", prime),
                    Err(e) => eprintln!("Error saving {}.txt: {}", prime, e),
                }
            }

            // Save all primes to primes.txt in XDG_DATA_HOME
            match storage::save_all_primes(&primes) {
                Ok(_) => println!("\nSaved all primes to primes.txt"),
                Err(e) => eprintln!("Error saving primes.txt: {}", e),
            }

            println!("\nTotal: {} primes found", primes.len());

            let duration = start.elapsed();
            let duration_us = duration.as_micros();

            println!(
                "Execution time: {}us ({:.2}ms)",
                duration_us,
                duration_us as f64 / 1000.0
            );

            if let Err(e) =
                storage::log_execution("primes", &limit.to_string(), variation, duration_us)
            {
                eprintln!("Warning: Failed to log execution: {}", e);
            }
        }
        Commands::PrimesBases { pal_only, pal } => {
            primes_bases::run(pal_only, pal);
        }
    }
}
