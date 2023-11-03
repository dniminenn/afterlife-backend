use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{BufReader, AsyncReadExt};
use serde_json;
use tokio::io;
use std::{env, fs};

pub async fn read_file(path: &Path) -> io::Result<String> {
    let file = File::open(path).await?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).await?;
    Ok(contents)
}

pub async fn load_users_data() -> HashMap<String, String> {
    let env_users_file = env::var("AFTERLIFE_FILE_USERS").unwrap_or_else(|_| String::from("users.json"));
    let file_path = Path::new(&env_users_file);
    let data = read_file(file_path).await.unwrap_or_else(|_| String::new());
    let users: HashMap<String, Vec<String>> = serde_json::from_str(&data).unwrap_or_default();

    let mut address_to_username = HashMap::new();
    for (username, addresses) in users {
        for address in addresses {
            address_to_username.insert(address, username.clone());
        }
    }
    address_to_username
}