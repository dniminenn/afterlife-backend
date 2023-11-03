use crate::indexer;
use indexer::indexer_config::{Chain, Contract};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::From;
use std::convert::TryInto;
use std::result::Result;
use tokio_postgres::{Client, Error};

/* DB SCHEMA
1. chains:
   - id: integer (Primary Key)
   - name: character varying

2. contracts:
   - id: integer (Primary Key)
   - chain_id: integer (Foreign Key -> chains.id)
   - name: character varying
   - address: character varying
   - type: character varying
   - last_processed_block: integer

3. events:
   - id: integer (Primary Key)
   - contract_id: integer (Foreign Key -> contracts.id)
   - operator: character varying
   - from_address: character varying
   - to_address: character varying
   - ids: character varying (JSON list of integers, e.g., "[99, 104, 105]")
   - values: character varying (JSON list of integers, e.g., "[1, 2, 3...]")
   - block_number: integer
   - transaction_hash: character varying

Relationships:

- contracts.chain_id REFERENCES chains.id
- events.contract_id REFERENCES contracts.id
*/

// Event struct

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub contract: Contract,
    pub operator: String,
    pub from_address: String,
    pub to_address: String,
    pub ids: Vec<u64>,
    pub values: Vec<u64>,
    pub block_number: u64,
    pub transaction_hash: String,
}

// Implement the Event struct, verify ids and values are the same length, and implement the From trait for the Event struct
impl Event {
    pub fn new(
        contract: Contract,
        operator: String,
        from_address: String,
        to_address: String,
        ids: Vec<u64>,
        values: Vec<u64>,
        block_number: u64,
        transaction_hash: String,
    ) -> Result<Self, &'static str> {
        if ids.len() != values.len() {
            return Err("ids and values must be the same length");
        }

        Ok(Event {
            contract,
            operator,
            from_address,
            to_address,
            ids,
            values,
            block_number,
            transaction_hash,
        })
    }
    // Convert string containing JSON list of integers to Vec<u64>
    pub fn from_json(
        contract: Contract,
        operator: String,
        from_address: String,
        to_address: String,
        ids: String,
        values: String,
        block_number: u64,
        transaction_hash: String,
    ) -> Result<Self, &'static str> {
        let ids: Vec<u64> = serde_json::from_str(&ids).unwrap();
        let values: Vec<u64> = serde_json::from_str(&values).unwrap();

        Event::new(
            contract,
            operator,
            from_address,
            to_address,
            ids,
            values,
            block_number,
            transaction_hash,
        )
    }
}

pub async fn get_last_processed_block(contract: &Contract, client: &Client) -> Result<i32, Error> {
    let row = client
        .query_one(
            "SELECT last_processed_block FROM contracts WHERE LOWER(address) = $1",
            &[&contract.address],
        )
        .await?;

    Ok(row.get(0))
}

pub async fn get_earliest_last_processed_block(
    chain: &Chain,
    client: &Client,
) -> Result<i32, Error> {
    let row = client
        .query_one(
            "SELECT MIN(last_processed_block) FROM contracts WHERE chain_id = $1",
            &[&(chain.id as i32)],
        )
        .await?;

    Ok(row.get(0))
}

async fn update_last_processed_block(
    contract: &Contract,
    chain: &Chain,
    last_processed_block: u64,
    client: &Client,
) -> Result<(), Error> {
    client
        .execute(
            "UPDATE contracts SET last_processed_block = $1 WHERE LOWER(address) = $2 AND chain_id = $3",
            &[&(last_processed_block as i32), &contract.address.to_lowercase(), &(chain.id as i32)],
        )
        .await?;

    Ok(())
}

pub async fn add_event(
    chain: &Chain,
    contract: &Contract,
    contract_id: i32,
    event: &Event,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let ids_as_json = serde_json::to_string(&event.ids)?;
    let values_as_json = serde_json::to_string(&event.values)?;

    client
            .execute(
                "INSERT INTO events (contract_id, operator, from_address, to_address, ids, values, block_number, transaction_hash) \
                VALUES ($1, LOWER($2), LOWER($3), LOWER($4), $5, $6, $7, LOWER($8))",
                &[
                    &contract_id,
                    &event.operator,
                    &event.from_address,
                    &event.to_address,
                    &ids_as_json,
                    &values_as_json,
                    &(event.block_number as i32),
                    &event.transaction_hash,
                ],
            )
            .await?;

    Ok(())
}

pub async fn contract_and_chain_to_contractid(
    contract: &Contract,
    chain: &Chain,
    client: &Client,
) -> Result<i32, Error> {
    let chain_id: i32 = match client
        .query_one(
            "SELECT id FROM chains WHERE LOWER(name) = $1",
            &[&chain.name.to_lowercase()],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => {
            //println!("Adding chain with name {}", chain.name);
            client
                .query_one(
                    "INSERT INTO chains (name, rpc_url, chunk_size) VALUES ($1, $2, $3) RETURNING id",
                    &[&chain.name, &chain.rpc_url, &(chain.chunk_size as i32)],
                )
                .await?
                .get(0)
        }
    };
    let contract_id: i32 = match client
        .query_one(
            "SELECT id FROM contracts WHERE LOWER(address) = $1 AND chain_id = $2",
            &[&contract.address.to_lowercase(), &chain_id],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => {
            //println!("Adding contract with address {} and name {}", contract.address, contract.name);
            client
                .query_one(
                    "INSERT INTO contracts (chain_id, name, address, type, last_processed_block) VALUES ($1, $2, $3, $4, $5) RETURNING id",
                    &[&chain_id, &contract.name, &contract.address.to_lowercase(), &contract.r#type, &(contract.startblock as i32)],
                )
                .await?
                .get(0)
        }
    };
    Ok(contract_id)
}

pub async fn nuke_and_process_events_for_chain(
    chain: &Chain,
    new_events_by_contract: &HashMap<i32, Vec<Event>>, // key is contract address
    from_block: u64,
    to_block: u64,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    for contract in &chain.contracts {
        let contract_id = contract_and_chain_to_contractid(contract, chain, client).await?;
        if let Some(new_events) = new_events_by_contract.get(&contract_id) {

            // Delete old events between from_block and now
            client
                .execute(
                    "DELETE FROM events WHERE contract_id = $1 AND block_number >= $2 AND block_number <= $3",
                    &[&contract_id, &(from_block as i32), &(to_block as i32)],
                )
                .await?;

            // Add new events
            for event in new_events {
                add_event(chain, contract, contract_id, event, client).await?;
            }
        }

        update_last_processed_block(&contract, &chain, to_block, client).await?;
    }

    Ok(())
}
