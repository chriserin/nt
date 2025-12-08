use chrono::Local;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

pub fn get_nt_data_dir() -> PathBuf {
    let xdg_data_home = env::var("XDG_DATA_HOME")
        .ok()
        .and_then(|path| {
            if path.is_empty() {
                None
            } else {
                Some(PathBuf::from(path))
            }
        })
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(".local/share"))
        })
        .expect("Could not determine data directory");

    xdg_data_home.join("nt")
}

pub fn save_property(number: usize, property: &str) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let filename = format!("{}.txt", number);
    let path = data_dir.join(&filename);

    if path.exists() {
        if let Ok(content) = fs::read_to_string(&path) {
            if content.contains(property) {
                return Ok(());
            }
        }
    }

    fs::write(&path, property)?;
    Ok(())
}

pub fn save_all_primes(primes: &[usize]) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let primes_path = data_dir.join("primes.txt");
    let primes_text = primes
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<String>>()
        .join("\n");

    fs::write(&primes_path, primes_text)?;
    Ok(())
}
pub fn load_all_primes() -> std::io::Result<Vec<usize>> {
    let data_dir = get_nt_data_dir();
    let primes_path = data_dir.join("primes.txt");

    let content = fs::read_to_string(&primes_path)?;
    let primes = content
        .lines()
        .filter_map(|line| line.trim().parse::<usize>().ok())
        .collect();

    Ok(primes)
}

pub fn log_execution(
    subcommand: &str,
    args: &str,
    variation: u32,
    duration_us: u128,
) -> std::io::Result<()> {
    let data_dir = get_nt_data_dir();
    fs::create_dir_all(&data_dir)?;

    let log_path = data_dir.join("execution_log.txt");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");

    writeln!(
        file,
        "{} | {} | {} | v{} | {}us",
        timestamp, subcommand, args, variation, duration_us
    )?;

    Ok(())
}

/// Save primes from a channel, streaming them to primes.txt one at a time
/// Optionally saves each prime as an individual property file
/// Returns the count of primes saved
pub fn save_primes_streaming(rx: Receiver<usize>, save_as_property: bool) -> usize {
    let mut count = 0;

    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::new(file);

    // Process each prime from the channel
    for prime in rx {
        if save_as_property {
            match save_property(prime, "prime") {
                Ok(_) => println!("Saved: {}.txt", prime),
                Err(e) => eprintln!("Error saving {}.txt: {}", prime, e),
            }
        }

        // Append prime to primes.txt (buffered)
        if let Err(e) = writeln!(writer, "{}", prime) {
            eprintln!("Error writing to primes.txt: {}", e);
        }

        count += 1;
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt");
    count
}

/// Save primes from a channel that sends batched segments
/// Receives Vec<usize> instead of individual primes for better performance
/// Optionally saves each prime as an individual property file
/// Returns the count of primes saved
pub fn save_primes_streaming_batched(rx: Receiver<Vec<usize>>, save_as_property: bool) -> usize {
    let mut count = 0;

    // Open primes.txt in write mode (truncate)
    let data_dir = get_nt_data_dir();
    if let Err(e) = fs::create_dir_all(&data_dir) {
        eprintln!("Error creating data directory: {}", e);
        return 0;
    }

    let primes_path = data_dir.join("primes.txt");

    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&primes_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening primes.txt: {}", e);
            return 0;
        }
    };

    // Use BufWriter to buffer writes in memory
    let mut writer = BufWriter::new(file);

    // Process each segment of primes from the channel
    for segment_primes in rx {
        for prime in segment_primes {
            if save_as_property {
                match save_property(prime, "prime") {
                    Ok(_) => println!("Saved: {}.txt", prime),
                    Err(e) => eprintln!("Error saving {}.txt: {}", prime, e),
                }
            }

            // Append prime to primes.txt (buffered)
            if let Err(e) = writeln!(writer, "{}", prime) {
                eprintln!("Error writing to primes.txt: {}", e);
            }

            count += 1;
        }
    }

    // Flush buffer before returning
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing primes.txt: {}", e);
    }

    println!("\nSaved all primes to primes.txt");
    count
}
