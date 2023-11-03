use crate::indexer::indexer_config::{Chain, Contract};
use crate::indexer::queries::Event;
use web3::types::{BlockNumber, FilterBuilder, Log, H160, H256, U256};
use web3::transports::Http;
use web3::Web3;
use web3::error::{Error as Web3Error, TransportError};
use std::convert::TryInto;
use std::convert::From;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use crate::indexer::log_decode::{decode_erc1155_transfer_batch, decode_erc1155_transfer_single};

const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(10);
const MAX_RETRY_COUNT: usize = 5;

const TRANSFER_TOPIC: H256 = H256([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b,
    0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16,
    0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);
const TRANSFER_SINGLE_TOPIC: H256 = H256([
    0xc3, 0xd5, 0x81, 0x68, 0xc5, 0xae, 0x73, 0x97,
    0x73, 0x1d, 0x06, 0x3d, 0x5b, 0xbf, 0x3d, 0x65,
    0x78, 0x54, 0x42, 0x73, 0x43, 0xf4, 0xc0, 0x83,
    0x24, 0x0f, 0x7a, 0xac, 0xaa, 0x2d, 0x0f, 0x62
]);

const TRANSFER_BATCH_TOPIC: H256 = H256([
    0x4a, 0x39, 0xdc, 0x06, 0xd4, 0xc0, 0xdb, 0xc6,
    0x4b, 0x70, 0xaf, 0x90, 0xfd, 0x69, 0x8a, 0x23,
    0x3a, 0x51, 0x8a, 0xa5, 0xd0, 0x7e, 0x59, 0x5d,
    0x98, 0x3b, 0x8c, 0x05, 0x26, 0xc8, 0xf7, 0xfb
]);

#[derive(Debug)]
pub enum EventFetcherError {
    Web3Error(web3::Error),
    Custom(Box<dyn std::error::Error>),
    FromHexError(fixed_hash::rustc_hex::FromHexError),  // new variant
}

impl From<fixed_hash::rustc_hex::FromHexError> for EventFetcherError {
    fn from(err: fixed_hash::rustc_hex::FromHexError) -> Self {
        EventFetcherError::FromHexError(err)
    }
}

impl From<web3::Error> for EventFetcherError {
    fn from(err: web3::Error) -> Self {
        EventFetcherError::Web3Error(err)
    }
}

impl From<Box<dyn Error>> for EventFetcherError {
    fn from(err: Box<dyn Error>) -> Self {
        EventFetcherError::Custom(err)
    }
}

#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: H160,
    pub to: H160,
    pub token_id: U256,
}

#[derive(Debug, Clone)]
pub struct TransferSingle {
    pub operator: H160,
    pub from: H160,
    pub to: H160,
    pub id: U256,
    pub value: U256,
}

#[derive(Debug, Clone)]
pub struct TransferBatch {
    pub operator: H160,
    pub from: H160,
    pub to: H160,
    pub ids: Vec<U256>,
    pub values: Vec<U256>,
}

pub struct EventFetcher<'a> {
    chain: &'a Chain,
    web3: Web3<Http>,
    last_processed_block: usize,
}

impl<'a> EventFetcher<'a> {
    pub fn new(chain: &'a Chain, last_processed_block: usize) -> Self {
        let http = Http::new(&chain.rpc_url).expect("RPC initialization failed");
        let web3 = Web3::new(http);

        Self {
            chain,
            web3,
            last_processed_block,
        }
    }

 /*   pub async fn execute(&self) -> Result<(Vec<Event>, (usize, usize)), EventFetcherError> {
        let mut events = Vec::new();
        let current_block = self.web3.eth().block_number().await?.as_u64() as usize;

        let look_back_start_block = if current_block <= self.last_processed_block + 2000 {
            // If we are within one chunk of the last processed block, look back a full chunk
            self.last_processed_block.saturating_sub(2000)
        } else {
            // If we are beyond one chunk, start at the last processed block
            self.last_processed_block
        };

        //println!("Look back start block: {}", look_back_start_block);
        //println!("Last processed: {}", self.last_processed_block);
        //println!("Current block: {}", current_block);

        let start_block = std::cmp::max(
            look_back_start_block,
            self.chain.contracts.iter().map(|c| c.startblock).min().unwrap_or(0) as usize,
        );

        let mut from_block = usize::MAX;
        let mut to_block = 0;

        let chunks: Vec<(usize, usize)> = (start_block..current_block)
            .step_by(self.chain.chunk_size)
            .map(|start| {
                let end = std::cmp::min(start + self.chain.chunk_size - 1, current_block);
                (start, end)
            })
            .collect();

        let total_chunks = chunks.len() as f64; // Cast to f64 for floating-point division
        //println!("Total chunks: {}, start block: {}, current block: {}", total_chunks, start_block, current_block);
        let mut current_chunk = 0; // Initialize a counter for the current chunk

        for (chunk_start, chunk_end) in chunks {
            //println!("Fetching logs for chunk {} to {} for chain {}", chunk_start, chunk_end, self.chain.name);
            from_block = std::cmp::min(from_block, chunk_start);
            to_block = std::cmp::max(to_block, chunk_end);

            let addresses: Vec<H160> = self.chain.contracts.iter()
                .filter_map(|contract| contract.address.parse().ok())
                .collect();

            let filter = FilterBuilder::default()
                .from_block(BlockNumber::Number(chunk_start.into()))
                .to_block(BlockNumber::Number(chunk_end.into()))
                .address(addresses)
                .topics(Some(vec![TRANSFER_TOPIC, TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC]), None, None, None)
                .build();

            let mut retry_delay = INITIAL_RETRY_DELAY;
            let mut attempts = 0;

            loop {
                match self.web3.eth().logs(filter.clone()).await {
                    Ok(logs) => {
                        for log in logs {
                            let contract_address = log.address;
                            if let Some(contract) = self.chain.contracts.iter().find(|&c| {
                                c.address.parse::<H160>().unwrap_or_default() == contract_address
                            }) {
                                let event = if log.topics[0] == TRANSFER_TOPIC {
                                    self.erc721_to_dbevent(&log, contract)?
                                } else if log.topics[0] == TRANSFER_SINGLE_TOPIC {
                                    self.erc1155_to_single_dbevent(&log, contract)?
                                } else if log.topics[0] == TRANSFER_BATCH_TOPIC {
                                    self.erc1155_to_batch_dbevent(&log, contract)?
                                } else {
                                    continue;
                                };
                                events.push(event);
                            }
                        }
                        break; // Exit the retry loop on success
                    },
                    Err(e) => {
                        if attempts >= MAX_RETRY_COUNT {
                            eprintln!("Failed to fetch logs after {} attempts: {:?}", MAX_RETRY_COUNT, e);
                            panic!("Failed to fetch logs after {} attempts: {:?}", MAX_RETRY_COUNT, e);
                            //break; // Break the loop to avoid crashing and continue with the next chunk
                        } else {
                            match &e {
                                Web3Error::Transport(TransportError::Code(code)) => {
                                    eprintln!("Transport error with code {}: {}. Retrying in {:?}... (Attempt {} of {})", code, e, retry_delay, attempts + 1, MAX_RETRY_COUNT);
                                }
                                Web3Error::Transport(TransportError::Message(message)) => {
                                    eprintln!("Transport error with message {}: {}. Retrying in {:?}... (Attempt {} of {})", message, e, retry_delay, attempts + 1, MAX_RETRY_COUNT);
                                }
                                _ => {
                                    eprintln!("Error fetching logs: {}. Retrying in {:?}... (Attempt {} of {})", e, retry_delay, attempts + 1, MAX_RETRY_COUNT);
                                }
                            }
                            tokio::time::sleep(retry_delay).await;
                            retry_delay *= 2;
                            attempts += 1;
                        }
                    }
                }
            }
            current_chunk += 1; // Increment the chunk counter
            let progress_percentage = (current_chunk as f64 / total_chunks) * 100.0;
            println!("Progress: {:.2}% for chain {}", progress_percentage, self.chain.name);

            // Ensure that the percentage does not exceed 100% due to rounding
            if current_chunk as usize == total_chunks as usize {
                println!("Progress: 100% for chain {}", self.chain.name);
            }
        }

        // Ensure that the return type matches the function signature
        Ok((events, (from_block, to_block)))
    }*/
    pub async fn execute(&self) -> Result<(Vec<Event>, (usize, usize)), EventFetcherError> {
        let mut events = Vec::new();
        let current_block = self.web3.eth().block_number().await?.as_u64() as usize;

        let look_back_start_block = if current_block <= self.last_processed_block + 2000 {
            // If we are within one chunk of the last processed block, look back a full chunk
            self.last_processed_block.saturating_sub(2000)
        } else {
            // If we are beyond one chunk, start at the last processed block
            self.last_processed_block
        };

        //println!("Look back start block: {}", look_back_start_block);
        //println!("Last processed: {}", self.last_processed_block);
        //println!("Current block: {}", current_block);

        let start_block = std::cmp::max(
            look_back_start_block,
            self.chain.contracts.iter().map(|c| c.startblock).min().unwrap_or(0) as usize,
        );

        let mut from_block = usize::MAX;
        let mut to_block = 0;

        let chunks: Vec<(usize, usize)> = (start_block..current_block)
            .step_by(self.chain.chunk_size)
            .map(|start| {
                let end = std::cmp::min(start + self.chain.chunk_size - 1, current_block);
                (start, end)
            })
            .collect();

        let current_chunk = Arc::new(AtomicUsize::new(0));
        let total_chunks = chunks.len() as f64; // Cast to f64 for floating-point division

        let semaphore = Arc::new(Semaphore::new(20)); // limit to 2 concurrent tasks

        let mut tasks = FuturesUnordered::new();

        for (chunk_start, chunk_end) in chunks {
            let current_chunk_clone = Arc::clone(&current_chunk);
            let addresses: Vec<H160> = self.chain.contracts.iter()
                .filter_map(|contract| contract.address.parse().ok())
                .collect();

            let filter = FilterBuilder::default()
                .from_block(BlockNumber::Number(chunk_start.into()))
                .to_block(BlockNumber::Number(chunk_end.into()))
                .address(addresses)
                .topics(Some(vec![TRANSFER_TOPIC, TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC]), None, None, None)
                .build();

            let web3 = self.web3.clone();
            let semaphore_clone = semaphore.clone();

            tasks.push(async move {
                let _permit = semaphore_clone.acquire_owned().await.expect("Failed to acquire semaphore permit");
                let mut retry_delay = INITIAL_RETRY_DELAY;
                let mut attempts = 0;

                loop {
                    match web3.eth().logs(filter.clone()).await {
                        Ok(logs) => {
                            let mut events_chunk = Vec::new();
                            for log in logs {
                                // Your logic to convert logs to events goes here
                                let contract_address = log.address;
                                if let Some(contract) = self.chain.contracts.iter().find(|&c| {
                                    c.address.parse::<H160>().unwrap_or_default() == contract_address
                                }) {
                                    let event = if log.topics[0] == TRANSFER_TOPIC {
                                        self.erc721_to_dbevent(&log, contract)?
                                    } else if log.topics[0] == TRANSFER_SINGLE_TOPIC {
                                        self.erc1155_to_single_dbevent(&log, contract)?
                                    } else if log.topics[0] == TRANSFER_BATCH_TOPIC {
                                        self.erc1155_to_batch_dbevent(&log, contract)?
                                    } else {
                                        eprintln!("Unknown topic: {:?}", log.topics[0]);
                                        eprintln!("Log: {:?}", log);
                                        continue;
                                    };
                                    events_chunk.push(event);
                                }
                            }
                            // After processing each chunk, we increment the counter
                            let task_chunk_index = current_chunk_clone.fetch_add(1, Ordering::SeqCst);

                            // We calculate the progress
                            let progress = ((task_chunk_index + 1) as f64 / total_chunks) * 100.0;
                            //println!("Chunk {} of {} completed. Progress: {:.2}%", task_chunk_index + 1, total_chunks, progress);
                            return Ok((events_chunk, (chunk_start, chunk_end)));
                        },
                        Err(e) => {
                            if attempts >= MAX_RETRY_COUNT {
                                panic!("Failed to fetch logs after {} attempts: {:?}", MAX_RETRY_COUNT, e);
                                return Err(EventFetcherError::from(e));
                            }
                            eprintln!("Error fetching logs: {}. Retrying in {:?}... (Attempt {} of {})", e, retry_delay, attempts + 1, MAX_RETRY_COUNT);
                            sleep(retry_delay).await;
                            retry_delay *= 2;
                            attempts += 1;
                        }
                    }
                }
            });
        }

        while let Some(result) = tasks.next().await {
            match result {
                Ok((mut events_chunk, (chunk_start, chunk_end))) => {
                    from_block = std::cmp::min(from_block, chunk_start);
                    to_block = std::cmp::max(to_block, chunk_end);

                    events.append(&mut events_chunk);
                },
                Err(e) => {
                    // Handle any errors that arose within the spawned tasks
                    panic!("Error fetching logs: {:?}", e)
                }
            }
        }

        Ok((events, (from_block, to_block)))
    }


    fn erc721_to_dbevent(&self, log: &Log, contract: &Contract) -> Result<Event, EventFetcherError> {

        let from_address: H160 = log.topics[1].try_into().unwrap();
        let to_address: H160 = log.topics[2].try_into().unwrap();
        // id is topics[3]
        let id: u64 = U256::from_big_endian(&log.topics[3].0).as_u64();
        let ids: Vec<u64> = vec![id];
        let values: Vec<u64> = vec![1];  // For ERC721, the value is always 1

        Ok(Event::new(
            contract.clone(),
            format!("{:?}", from_address),
            format!("{:?}", to_address),
            format!("{:?}", to_address),
            ids,
            values,
            log.block_number.unwrap().as_u64(),

            format!("{:?}", log.transaction_hash.unwrap()),

        ).map_err(|e| EventFetcherError::Custom(e.into()))?)
    }

    fn erc1155_to_single_dbevent(&self, log: &Log, contract: &Contract) -> Result<Event, EventFetcherError> {
        //println!("ERC1155 single event: {:?}", log);
        let operator: H160 = log.topics[1].try_into().unwrap();
        let from_address: H160 = log.topics[2].try_into().unwrap();
        let to_address: H160 = log.topics[3].try_into().unwrap();

        let (id, value) = decode_erc1155_transfer_single(&log)
            .map_err(|e| EventFetcherError::Custom(e.into()))?;

        let ids: Vec<u64> = vec![id];
        let values: Vec<u64> = vec![value];

        // format!("{:?}", operator) will make the type printable but it will be lowercase
        // to get the checksum address, we need to parse it and then print it


        Ok(Event::new(
            contract.clone(),
            format!("{:?}", operator),
            format!("{:?}", from_address),
            format!("{:?}", to_address),
            ids,
            values,
            log.block_number.unwrap().as_u64(),
            format!("{:?}", log.transaction_hash.unwrap())
        ).map_err(|e| EventFetcherError::Custom(e.into()))?)
    }

    fn erc1155_to_batch_dbevent(&self, log: &Log, contract: &Contract) -> Result<Event, EventFetcherError> {
        //println!("ERC1155 batch event: {:?}", log);
        let operator: H160 = log.topics[1].try_into().unwrap();
        let from_address: H160 = log.topics[2].try_into().unwrap();
        let to_address: H160 = log.topics[3].try_into().unwrap();

        // Assuming the rest of the data field is ids concatenated with values
        //println!("Data: {:?}", log.data.0);

        let (ids, values) = decode_erc1155_transfer_batch(&log)
            .map_err(|e| EventFetcherError::Custom(e.into()))?;

        Ok(Event::new(
            contract.clone(),
            format!("{:?}", operator),
            format!("{:?}", from_address),
            format!("{:?}", to_address),
            ids,
            values,
            log.block_number.unwrap().as_u64(),
            format!("{:?}", log.transaction_hash.unwrap())
        ).map_err(|e| EventFetcherError::Custom(e.into()))?)
    }
}