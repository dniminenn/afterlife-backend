use serde_json;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::{env, fs};
use tokio::fs::{read_to_string, File};
use tokio::io;
use tokio::io::{AsyncReadExt, BufReader};

pub async fn read_file(path: &Path) -> io::Result<String> {
    let file = File::open(path).await?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).await?;
    Ok(contents)
}

pub async fn load_users_data() -> HashMap<String, Vec<String>> {
    let env_users_file =
        env::var("AFTERLIFE_FILE_USERS").unwrap_or_else(|_| "users.json".to_owned());
    let file_path = Path::new(&env_users_file);
    let data = read_to_string(file_path)
        .await
        .expect("Failed to read users file");
    serde_json::from_str(&data).expect("Failed to parse users data")
}

pub async fn get_addresses_by_input(input: &str) -> Result<HashSet<String>, String> {
    let users_data = load_users_data().await;
    let mut reverse_mapping: HashMap<String, HashSet<String>> = HashMap::new();

    // Iterate over references to avoid moving users_data
    for (username, addresses) in &users_data {
        for address in addresses {
            reverse_mapping
                .entry(address.to_lowercase())
                .or_default()
                .insert(username.clone());
        }
    }

    let input_lower = input.to_lowercase();
    let addresses = match reverse_mapping.get(&input_lower) {
        Some(usernames) => usernames.iter().cloned().collect(),
        None => {
            // If the input is a username, gather and return all associated addresses
            let mut found_addresses = HashSet::new();
            if let Some(addresses) = users_data.get(input) {
                for address in addresses {
                    found_addresses.insert(address.clone());
                }
            }
            found_addresses
        }
    };

    Ok(addresses)
}
