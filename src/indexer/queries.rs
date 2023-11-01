use tokio_postgres::{Client, Error};
use std::result::Result;
use serde::{Deserialize, Serialize};
use indexer::indexer_config::{Chain, Contract};
use crate::indexer;
use std::convert::From;
use std::convert::TryInto;

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
    pub fn new(operator: String, from_address: String, to_address: String, ids: Vec<u64>, values: Vec<u64>, block_number: u64, transaction_hash: String) -> Result<Self, &'static str> {
        if ids.len() != values.len() {
            return Err("ids and values must be the same length");
        }

        Ok(Event {
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
    pub fn from_json(operator: String, from_address: String, to_address: String, ids: String, values: String, block_number: u64, transaction_hash: String) -> Result<Self, &'static str> {
        let ids: Vec<u64> = serde_json::from_str(&ids).unwrap();
        let values: Vec<u64> = serde_json::from_str(&values).unwrap();

        Event::new(operator, from_address, to_address, ids, values, block_number, transaction_hash)
    }

}

pub async fn get_last_processed_block(contract: &Contract, client: &Client) -> Result<i32, Error> {
    let row = client
        .query_one(
            "SELECT last_processed_block FROM contracts WHERE address = $1",
            &[&contract.address],
        )
        .await?;

    Ok(row.get(0))
}

pub async fn get_earliest_last_processed_block(chain: &Chain, client: &Client) -> Result<i32, Error> {
    let row = client
        .query_one(
            "SELECT MIN(last_processed_block) FROM contracts WHERE chain_id = $1",
            &[&(chain.id as i32)],
        )
        .await?;

    Ok(row.get(0))
}

pub async fn update_last_processed_block(contract: &Contract, last_processed_block: u64, client: &Client) -> Result<(), Error> {
    client
        .execute(
            "UPDATE contracts SET last_processed_block = $1 WHERE address = $2",
            &[&(last_processed_block as i32), &contract.address],
        )
        .await?;

    Ok(())
}

pub async fn add_event(contract: &Contract, event: &Event, client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    if !event_exists(contract, &event.transaction_hash, client).await? {
        let ids_as_json = serde_json::to_string(&event.ids).map_err(Box::new)?;
        let values_as_json = serde_json::to_string(&event.values).map_err(Box::new)?;

        client
            .execute(
                "INSERT INTO events (contract_id, operator, from_address, to_address, ids, values, block_number, transaction_hash) \
                SELECT id, $1, $2, $3, $4, $5, $6, $7 FROM contracts WHERE address = $8",
                &[&event.operator, &event.from_address, &event.to_address, &ids_as_json, &values_as_json, &(event.block_number as i32), &event.transaction_hash, &contract.address]
            )
            .await.map_err(Box::new)?;
    }

    Ok(())
}

pub async fn event_exists(contract: &Contract, transaction_hash: &str, client: &Client) -> Result<bool, Error> {
    let row = client
        .query_opt(
            "SELECT 1 FROM events e \
            JOIN contracts c ON e.contract_id = c.id \
            WHERE c.address = $1 AND e.transaction_hash = $2",
            &[&contract.address, &transaction_hash],
        )
        .await?;

    Ok(row.is_some())
}

pub async fn remove_duplicates(contract: &Contract, client: &Client) -> Result<(), Error> {
    client
        .execute(
            "DELETE FROM events \
            WHERE id NOT IN ( \
                SELECT MIN(id) \
                FROM events e \
                JOIN contracts c ON e.contract_id = c.id \
                WHERE c.address = $1 \
                GROUP BY e.transaction_hash \
            )",
            &[&contract.address],
        )
        .await?;

    Ok(())
}

use std::collections::HashSet;

pub async fn process_events_for_chunk(
    contract: &Contract,
    new_events: &[Event],
    from_block: u64,
    to_block: u64,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get existing events from database in that block range
    let mut stmt = client.prepare(
        "SELECT transaction_hash FROM events e \
         JOIN contracts c ON e.contract_id = c.id \
         WHERE c.address = $1 AND e.block_number BETWEEN $2 AND $3"
    ).await?;

    let rows = client.query(&stmt, &[&contract.address, &(from_block as i32), &(to_block as i32)]).await?;
    let existing_hashes: HashSet<String> = rows.iter().map(|row| row.get(0)).collect();

    // New event hashes
    let new_hashes: HashSet<String> = new_events.iter().map(|e| e.transaction_hash.clone()).collect();

    // Events to delete are existing_hashes - new_hashes
    let to_delete: Vec<&String> = existing_hashes.difference(&new_hashes).collect();

    for hash in to_delete {
        client.execute(
            "DELETE FROM events WHERE transaction_hash = $1",
            &[&hash],
        ).await?;
    }

    // Add new events (could be optimized)
    for event in new_events {
        add_event(contract, event, client).await?;
    }

    Ok(())
}
