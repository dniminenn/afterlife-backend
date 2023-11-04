use ignore::WalkBuilder;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;
use dotenv::dotenv;
use std::env;

const PROCESSED_FILE: &str = "processed_files.txt";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    println!("Starting metadata watcher");
    let mut processed: HashSet<String> = HashSet::new();
    let processed_file_path = PROCESSED_FILE;

    // Load previously processed files
    if Path::new(&processed_file_path).exists() {
        let file = File::open(&processed_file_path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(id) = line {
                processed.insert(id);
            }
        }
    }

    loop {
        let data_path = env::var("AFTERLIFE_PATH_METADATA");
        let walker = WalkBuilder::new(data_path.unwrap())
            .add_custom_ignore_filename(".ignore")
            .build();

        for result in walker {
            if let Ok(entry) = result {
                if entry.file_type().map_or(false, |ft| ft.is_file()) {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("json") {
                        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

                        if !processed.contains(&file_name) {
                            process_new_file(path)?;
                            processed.insert(file_name.clone());

                            // Append the processed file to the record
                            let mut file = File::create(&processed_file_path)?;
                            for id in &processed {
                                writeln!(file, "{}", id)?;
                            }
                        }
                    }
                }
            }
        }
        println!("Done, sleeping for 60 seconds");
        thread::sleep(Duration::from_secs(60));
    }
}

fn process_new_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Processing new file: {:?}", path);

    let command_line = "\
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/metadatafetcher.py && \
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/cloudflarize.py && \
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/rarity_scorer.py";

    let status = Command::new("bash")
        .arg("-c")
        .arg(command_line)
        .status()?;

    if status.success() {
        println!("Python scripts executed successfully.");
    } else {
        eprintln!("Python scripts failed with exit status: {:?}", status.code());
    }

    Ok(())
}

