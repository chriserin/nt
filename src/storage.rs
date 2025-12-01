use chrono::Local;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

pub fn get_nt_data_dir() -> PathBuf {
    let xdg_data_home = env::var("XDG_DATA_HOME")
        .ok()
        .and_then(|path| if path.is_empty() { None } else { Some(PathBuf::from(path)) })
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

pub fn log_execution(subcommand: &str, args: &str, variation: u32, duration_us: u128) -> std::io::Result<()> {
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
