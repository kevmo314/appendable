use std::fs::File;
use std::io::{BufRead, BufReader};
use clap::Parser;
use anyhow::{Context, Result};
use serde_json::Value;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short)]
    file_path: String
}

fn main() -> Result<()>{
    let args = Args::parse();
    let file_path = args.file_path;

    let jsonl_file = File::open(&file_path)?;
    let reader = BufReader::new(jsonl_file);

    let mut array: Vec<Value> = vec![];

    for line in reader.lines() {
        let line = line?;
        let json: Value = serde_json::from_str(&line)?;
        array.push(json);
    }

    let output_path = file_path.replace(".jsonl", ".json").to_owned();
    let json_string = serde_json::to_string_pretty(&array)
        .with_context(|| "Failed to serialize JSON data")?;

    std::fs::write(&output_path, json_string.as_bytes())
        .with_context(|| format!("Failed to write to file: {}", output_path))?;

    return Ok(())
}
