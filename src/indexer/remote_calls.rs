use crate::indexer::indexer_config::{Chain, Contract};
use crate::indexer::queries::Event;
use web3::types::{BlockNumber, FilterBuilder, Log, H160, H256, U256};
use web3::transports::Http;
use web3::Web3;
use std::convert::TryInto;
use std::convert::From;
use std::error::Error;
use std::str::FromStr;

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

    pub async fn execute(&self) -> Result<Vec<Event>, EventFetcherError> {
        let mut events = Vec::new();
        let current_block = self.web3.eth().block_number().await?.as_u64() as usize;

        let look_back_start_block = if self.last_processed_block >= self.chain.chunk_size {
            self.last_processed_block - self.chain.chunk_size
        } else {
            0
        };

        let start_block = std::cmp::max(
            look_back_start_block,
            self.chain.contracts.iter().map(|c| c.startblock).min().unwrap_or(0),
        );

        for chunk_start in (start_block..=current_block).step_by(self.chain.chunk_size) {
            let chunk_end = std::cmp::min(chunk_start + self.chain.chunk_size, current_block);

            let addresses: Result<Vec<H160>, _> = self.chain.contracts.iter()
                .map(|contract| contract.address.parse())
                .collect();

            let addresses = addresses?;
            println!("Fetching events for {} contracts from block {} to {}", addresses.len() ,chunk_start, chunk_end);

            let filter = FilterBuilder::default()
                .from_block(BlockNumber::Number(chunk_start.into()))
                .to_block(BlockNumber::Number(chunk_end.into()))
                .address(addresses)
                .build();

            let logs = self.web3.eth().logs(filter).await?;

            for log in logs {

                let contract = self.chain.contracts.iter().find(|&c| {
                    // Directly parse and compare H160 types
                    c.address.parse::<H160>().unwrap_or_default() == log.address
                });


                if contract.is_some() {
                    let event;

                    if log.topics[0] == TRANSFER_TOPIC {
                        event = self.erc721_to_dbevent(&log)?;
                    } else if log.topics[0] == TRANSFER_SINGLE_TOPIC {
                        event = self.erc1155_to_single_dbevent(&log)?;
                    } else if log.topics[0] == TRANSFER_BATCH_TOPIC {
                        event = self.erc1155_to_batch_dbevent(&log)?;
                    } else {
                        //println!("Unknown event topic: {:?}", log.topics[0]);
                        continue;
                    }

                    events.push(event);
                }
            }
        }
        Ok(events)
    }


    fn erc721_to_dbevent(&self, log: &Log) -> Result<Event, EventFetcherError> {
        //println!("ERC721 event: {:?}", log);
        let from_address: H160 = log.topics[1].try_into().unwrap();
        let to_address: H160 = log.topics[2].try_into().unwrap();
        let ids: Vec<u64> = vec![U256::from_big_endian(&log.data.0).as_u64()];
        let values: Vec<u64> = vec![1];  // For ERC721, the value is always 1

        Ok(Event::new(
            "ERC721".to_string(),
            format!("{:?}", from_address),
            format!("{:?}", to_address),
            ids,
            values,
            log.block_number.unwrap().as_u64(),
            format!("{:?}", log.transaction_hash.unwrap())
        ).map_err(|e| EventFetcherError::Custom(e.into()))?)
    }

    fn erc1155_to_single_dbevent(&self, log: &Log) -> Result<Event, EventFetcherError> {
        //println!("ERC1155 single event: {:?}", log);
        let operator: H160 = log.topics[1].try_into().unwrap();
        let from_address: H160 = log.topics[2].try_into().unwrap();
        let to_address: H160 = log.topics[3].try_into().unwrap();

        let ids: Vec<u64> = vec![U256::from(&log.data.0[0..32]).as_u64()];
        let values: Vec<u64> = vec![U256::from(&log.data.0[32..64]).as_u64()];

        Ok(Event::new(
            format!("{:?}", operator),
            format!("{:?}", from_address),
            format!("{:?}", to_address),
            ids,
            values,
            log.block_number.unwrap().as_u64(),
            format!("{:?}", log.transaction_hash.unwrap())
        ).map_err(|e| EventFetcherError::Custom(e.into()))?)
    }

    fn erc1155_to_batch_dbevent(&self, log: &Log) -> Result<Event, EventFetcherError> {
        //println!("ERC1155 batch event: {:?}", log);
        let operator: H160 = log.topics[1].try_into().unwrap();
        let from_address: H160 = log.topics[2].try_into().unwrap();
        let to_address: H160 = log.topics[3].try_into().unwrap();

        // Assuming the rest of the data field is ids concatenated with values
        let ids_values = &log.data.0;
        let half_len = ids_values.len() / 2;
        let ids_data = &ids_values[0..half_len];
        let values_data = &ids_values[half_len..];

        let ids: Vec<u64> = ids_data.chunks(32).map(|chunk| U256::from(chunk).as_u64()).collect();
        let values: Vec<u64> = values_data.chunks(32).map(|chunk| U256::from(chunk).as_u64()).collect();

        Ok(Event::new(
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
