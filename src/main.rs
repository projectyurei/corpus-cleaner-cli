mod filters;
mod processor;

use anyhow::{Result, Context};
use clap::Parser;
use std::path::PathBuf;
use std::fs;
use processor::{ProcessorConfig, run_processing};

#[derive(Parser, Debug)]
#[command(name = "corpus-cleaner-cli")]
#[command(author = "Yurei AI")]
#[command(version = "1.0")]
#[command(about = "Separating the Signal from the Noise.", long_about = None)]
struct Args {
    /// Path to input directory or file (.jsonl)
    #[arg(short, long)]
    input: PathBuf,

    /// Path to output directory
    #[arg(short, long)]
    output: PathBuf,

    /// Number of worker threads (default: available cores)
    #[arg(short, long, default_value_t = 0)]
    threads: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args = Args::parse();
    
    // Banner
    println!("{}", "========================================");
    println!("{}", "   YUREI AI :: CORPUS CLEANER CLI       ");
    println!("{}", "   High-Performance Data Refinery       ");
    println!("{}", "========================================");
    println!("Input:  {:?}", args.input);
    println!("Output: {:?}", args.output);
    println!("Threads: {}", if args.threads == 0 { "Auto".to_string() } else { args.threads.to_string() });
    println!("----------------------------------------");

    // Ensure output directory exists
    if !args.output.exists() {
        fs::create_dir_all(&args.output).context("Failed to create output directory")?;
    }

    let config = ProcessorConfig {
        input_path: args.input,
        output_path: args.output,
        threads: args.threads,
    };

    match run_processing(config) {
        Ok(_) => {
            println!("{}", "========================================");
            println!("{}", "   PROCESSING COMPLETE                  ");
            println!("{}", "========================================");
        }
        Err(e) => {
            eprintln!("Error: {:#}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
