use ignore::WalkBuilder;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;
use dotenv::dotenv;
use std::env;
use std::error::Error;

const PROCESSED_FILE: &str = "processed_files.txt";

fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    println!("Starting metadata watcher");
    let mut processed: HashSet<String> = HashSet::new();
    let processed_file_path = PROCESSED_FILE;

    if Path::new(&processed_file_path).exists() {
        let file = File::open(&processed_file_path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(id) = line {
                processed.insert(id);
            }
        }
    }

    let data_path = env::var("AFTERLIFE_PATH_METADATA")?;
    let base_path = Path::new(&data_path);

    loop {
        let mut new_files = Vec::new();
        let walker = WalkBuilder::new(&base_path)
            .add_custom_ignore_filename(".ignore")
            .build();

        for result in walker {
            if let Ok(entry) = result {
                if entry.file_type().map_or(false, |ft| ft.is_file()) {
                    let path = entry.path();
                    if let Some("json") = path.extension().and_then(|s| s.to_str()) {
                        let relative_path = path.strip_prefix(base_path)
                            .unwrap_or(path)
                            .with_extension("")
                            .to_string_lossy()
                            .into_owned();

                        if !processed.contains(&relative_path) {
                            new_files.push(relative_path);
                        }
                    }
                }
            }
        }

        // Process new files if there are any and no script is running
        if !new_files.is_empty() && !are_scripts_running()? {
            process_files()?;

            // Mark new files as processed
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&processed_file_path)?;

            for file_id in new_files {
                writeln!(file, "{}", file_id)?;
                processed.insert(file_id);
            }
        }

        println!("Done, sleeping for 60 seconds");
        thread::sleep(Duration::from_secs(60));
    }
}

fn are_scripts_running() -> Result<bool, Box<dyn Error>> {
    let scripts_regex = "metadatafetcher.py|cloudflarize.py|rarity_scorer.py";

    let pgrep_output = Command::new("pgrep")
        .arg("-f")
        .arg(scripts_regex)
        .output()?;

    Ok(!pgrep_output.stdout.is_empty())
}

fn process_files() -> Result<(), Box<dyn Error>> {
    // Execute the scripts
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
