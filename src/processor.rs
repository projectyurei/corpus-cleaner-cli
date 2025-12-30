use crate::filters::{Pipeline, StatusFilter, Filter};
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub struct ProcessorConfig {
    pub input_path: PathBuf,
    pub output_path: PathBuf,
    pub threads: usize,
}

pub fn run_processing(config: ProcessorConfig) -> Result<()> {
    // 1. Configure Rayon
    rayon::ThreadPoolBuilder::new()
        .num_threads(config.threads)
        .build_global()
        .context("Failed to build message thread pool")?;

    // 2. Discover Files
    let files = discover_files(&config.input_path)?;
    println!("Found {} files to process.", files.len());

    if files.is_empty() {
        return Ok(());
    }

    // 3. Setup Progress Bar
    let pb = Arc::new(ProgressBar::new(files.len() as u64));
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
        .progress_chars("#>-"));

    // 4. Setup Global Pipeline (Stateless parts)
    // Note: Deduplicator is stateful and shared.
    let dedup = crate::filters::Deduplicator::new();
    // We clone the pipeline config or create new per thread? 
    // Filters are `Send + Sync`, so we can share a reference or clone.
    // The pipeline struct itself holds ownership. We can construct it inside the loop or share an Arc.
    // Let's construct it per file to avoid locking contention on the filter list itself (though it's read-only).
    
    // 5. Parallel Processing
    let start_time = Instant::now();
    
    files.par_iter().for_each(|file_path| {
        let pb = pb.clone();
        let dedup = dedup.clone();
        
        match process_file(file_path, &config.output_path, dedup) {
            Ok(events_count) => {
                pb.inc(1);
                pb.set_message(format!("Processed {} events", events_count));
            },
            Err(e) => {
                pb.println(format!("Error processing {:?}: {}", file_path, e));
            }
        }
    });

    pb.finish_with_message("Done");
    println!("Processing complete in {:.2?}", start_time.elapsed());
    
    Ok(())
}

fn discover_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if path.is_file() {
        files.push(path.to_path_buf());
    } else {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                // Filter extensions if needed, e.g. .jsonl, .json
                if let Some(ext) = path.extension() {
                    if ext == "jsonl" || ext == "json" {
                        files.push(path);
                    }
                }
            }
        }
    }
    Ok(files)
}

fn process_file(input_path: &Path, output_dir: &Path, dedup: crate::filters::Deduplicator) -> Result<u64> {
    // Determine output path
    let file_name = input_path.file_name().context("No filename")?;
    let output_path = output_dir.join(file_name);
    
    // Create Pipeline
    let pipeline = Pipeline::new()
        .add_filter(StatusFilter)
        // Add other filters here
        .with_deduplication(dedup);

    let input_file = File::open(input_path)?;
    let reader = BufReader::new(input_file);
    
    // Ensure output directory exists (might need mutex if creating recursively, but standard create_dir_all is robust enough usually or do it beforehand)
    // We did not create output dir in main. Assuming main does it or we do it here.
    // fs::create_dir_all(output_dir)?; // Do this once in main ideally.

    let output_file = File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);
    
    let mut count = 0;
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        
        // Parse
        // Optimization: Use serde_json::from_str::<Value>
        match serde_json::from_str::<Value>(&line) {
            Ok(tx) => {
                if pipeline.process(&tx) {
                    serde_json::to_writer(&mut writer, &tx)?;
                    writer.write_all(b"\n")?;
                    count += 1;
                }
            }
            Err(_) => {
                // Malformed JSON filter technically handles this by failing parse.
                // We just skip.
            }
        }
    }
    
    writer.flush()?;
    Ok(count)
}
