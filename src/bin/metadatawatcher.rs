use afterlife_backend::common::database;
use dotenv::dotenv;
use eth_checksum::checksum;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use std::time::Duration;
use std::{env, fs};

const PROCESSED_TOKENS_FILE: &str = "processed_tokens.txt";
const LAST_PROCESSED_BLOCKS_FILE: &str = "last_processed_blocks.txt";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    println!("Starting metadata watcher");

    let client = database::connect()
        .await
        .expect("Failed to connect to database");
    let data_path = env::var("AFTERLIFE_PATH_METADATA")?;

    // Initialize last_processed_blocks with chain names from the database
    let mut last_processed_blocks = get_last_processed_blocks()?;
    if last_processed_blocks.is_empty() {
        let chain_rows = client
            .query(
                "
            SELECT DISTINCT name
            FROM chains
        ",
                &[],
            )
            .await?;

        for row in chain_rows {
            let chain_name: String = row.get("name");
            last_processed_blocks.insert(chain_name, 0); // Start from block 0
        }
    }

    // Read the already processed tokens into the HashSet
    let processed_content =
        fs::read_to_string(PROCESSED_TOKENS_FILE).unwrap_or_else(|_| "".to_string());
    let mut processed: HashSet<String> = processed_content
        .lines()
        .map(|line| line.to_string())
        .collect();
    let mut updated_block_numbers: HashSet<(String, i32)> = HashSet::new();

    // Main loop
    loop {
        // Retrieve events from the database based on the last processed block
        let mut new_tokens = Vec::new();
        for (chain, &last_block) in &last_processed_blocks {
            let rows = client
                .query(
                    "
        SELECT
            e.to_address,
            e.from_address,
            ch.name AS chain_name,
            c.address AS contract_address,
            e.ids AS ids,
            e.block_number AS block_number
        FROM events e
        INNER JOIN contracts c ON e.contract_id = c.id
        INNER JOIN chains ch ON c.chain_id = ch.id
        WHERE ch.name = $1 AND e.block_number > $2
    ",
                    &[&chain, &last_block],
                )
                .await?;

            for row in &rows {
                let ids: Vec<u64> = serde_json::from_str(row.get("ids"))?;

                // If file for token exists in data_path, add token to new_tokens
                ids.into_iter().for_each(|id| {
                    let file_name = format!(
                        "{}/{}/{}/{}.json",
                        data_path,
                        chain,
                        checksum(row.get("contract_address")),
                        id
                    );
                    if !processed.contains(&file_name) {
                        new_tokens.push((chain.clone(), file_name));
                    }
                });
            }
            updated_block_numbers.extend(rows.iter().map(|row| {
                let chain_name: String = row.get("chain_name");
                let block_number: i32 = row.get("block_number");
                (chain_name, block_number)
            }));
        }

        let mut max_block_numbers: HashMap<String, i32> = HashMap::new();
        for (chain, block_number) in &updated_block_numbers {
            let entry = max_block_numbers.entry(chain.clone()).or_insert(0);
            *entry = i32::max(*entry, block_number.clone());
        }

        updated_block_numbers.clear();

        // Update last_processed_blocks with the max block numbers
        last_processed_blocks.extend(max_block_numbers);

        // Process new tokens if there are any and no script is running
        if !new_tokens.is_empty() && !are_scripts_running()? {
            launch_metadata_scripts()?;

            // mark new tokens as processed in PROCESSED_FILE
            let mut processed_file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(PROCESSED_TOKENS_FILE)?;

            for (_, token_file) in &new_tokens {
                writeln!(processed_file, "{}", token_file)?;
                processed.insert(token_file.clone());
            }

            // update last processed blocks in LAST_PROCESSED_BLOCKS_FILE
            update_last_processed_blocks(&last_processed_blocks)?;
            update_processed_tokens(&new_tokens)?;
        }

        println!("Done, sleeping for 60 seconds");
        tokio::time::sleep(Duration::from_secs(60)).await;
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

fn launch_metadata_scripts() -> Result<(), Box<dyn Error>> {
    // Execute the scripts
    let command_line = "\
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/metadatafetcher.py && \
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/cloudflarize.py && \
        /home/jr/afterlife-awesome/venv/bin/python /home/jr/afterlife-awesome/rarity_scorer.py";

    let status = Command::new("bash").arg("-c").arg(command_line).status()?;

    if status.success() {
        println!("Python scripts executed successfully.");
    } else {
        eprintln!(
            "Python scripts failed with exit status: {:?}",
            status.code()
        );
    }

    Ok(())
}

fn get_last_processed_blocks() -> Result<HashMap<String, i32>, Box<dyn Error>> {
    let mut last_processed_blocks: HashMap<String, i32> = HashMap::new();

    let file = File::open(LAST_PROCESSED_BLOCKS_FILE)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let mut split = line.split(" ");
        let chain = split.next().unwrap();
        let block_number = split.next().unwrap().parse::<i32>()?;
        last_processed_blocks.insert(chain.to_string(), block_number);
    }

    Ok(last_processed_blocks)
}

fn update_last_processed_blocks(
    last_processed_blocks: &HashMap<String, i32>,
) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(LAST_PROCESSED_BLOCKS_FILE)?;

    for (chain, block_number) in last_processed_blocks {
        writeln!(file, "{} {}", chain, block_number)?;
    }

    Ok(())
}

fn update_processed_tokens(new_tokens: &[(String, String)]) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(PROCESSED_TOKENS_FILE)?;

    for (_, token_file) in new_tokens {
        writeln!(file, "{}", token_file)?;
    }

    Ok(())
}
