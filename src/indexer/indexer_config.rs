use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Chain {
    pub id: u32,
    pub name: String,
    pub rpc_url: String,
    pub chunk_size: usize,
    pub contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,
    pub address: String,
    pub startblock: i32,
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerConfig {
    pub chains: Vec<Chain>,
}

impl IndexerConfig {
    pub fn from_env() -> Result<Self, serde_yaml::Error> {
        let path = env::var("AFTERLIFE_PATH_IDXCFG")
            .expect("Environment variable AFTERLIFE_PATH_IDXCFG not set");
        let file = File::open(&path).expect("Failed to open file");
        let mut buf_reader = BufReader::new(file);
        let mut content = String::new();
        buf_reader
            .read_to_string(&mut content)
            .expect("Failed to read file");
        serde_yaml::from_str(&content)
    }

    pub fn get_earliest_start_block_for_chain(&self, chain: &Chain) -> i32 {
        let mut earliest_start_block = i32::MAX;
        for contract in &chain.contracts {
            if contract.startblock < earliest_start_block {
                earliest_start_block = contract.startblock;
            }
        }
        earliest_start_block
    }
}
