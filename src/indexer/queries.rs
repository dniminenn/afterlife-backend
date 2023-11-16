use crate::indexer;
use indexer::indexer_config::{Chain, Contract};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::result::Result;
use tokio_postgres::{Client, Error, GenericClient};
extern crate primitive_types;
use eth_checksum::checksum;
use web3::types::U256;

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
    pub ids: Vec<U256>,
    pub values: Vec<U256>,
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
        ids: Vec<U256>,
        values: Vec<U256>,
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
}

fn u256_vec_to_json_decimal(vec: &Vec<U256>) -> Result<String, serde_json::Error> {
    let decimal_strings: Vec<String> = vec.iter().map(|u| u.to_string()).collect();
    let string = serde_json::to_string(&decimal_strings);
    let stripped = string.unwrap().replace("\"", "");
    Ok(stripped)
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

pub async fn contract_and_chain_to_contractid<C>(
    contract: &Contract,
    chain: &Chain,
    client_or_transaction: &C,
) -> Result<i32, Error>
where
    C: GenericClient,
{
    let chain_id: i32 = match client_or_transaction
        .query_one(
            "SELECT id FROM chains WHERE LOWER(name) = $1",
            &[&chain.name.to_lowercase()],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => client_or_transaction
            .query_one(
                "INSERT INTO chains (name, rpc_url, chunk_size) VALUES ($1, $2, $3) RETURNING id",
                &[&chain.name, &chain.rpc_url, &(chain.chunk_size as i32)],
            )
            .await?
            .get(0),
    };

    let contract_id: i32 = match client_or_transaction
        .query_one(
            "SELECT id FROM contracts WHERE LOWER(address) = $1 AND chain_id = $2",
            &[&contract.address.to_lowercase(), &chain_id],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => {
            client_or_transaction
                .query_one(
                    "INSERT INTO contracts (chain_id, name, address, type, last_processed_block) VALUES ($1, $2, $3, $4, $5) RETURNING id",
                    &[&chain_id, &contract.name, &contract.address.to_lowercase(), &contract.r#type, &(contract.startblock )],
                )
                .await?
                .get(0)
        }
    };

    Ok(contract_id)
}

pub async fn nuke_and_process_events_for_chain(
    chain: &Chain,
    new_events_by_contract: &HashMap<i32, Vec<Event>>, // key is contract_id
    from_block: u64,
    to_block: u64,
    client: &mut Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let transaction = client.transaction().await?;

    for contract in &chain.contracts {
        let contract_id = contract_and_chain_to_contractid(contract, chain, &transaction).await?;

        if let Some(new_events) = new_events_by_contract.get(&contract_id) {
            transaction
                .execute(
                    "DELETE FROM events WHERE contract_id = $1 AND block_number >= $2 AND block_number <= $3",
                    &[&contract_id, &(from_block as i32), &(to_block as i32)],
                )
                .await?;

            for event in new_events {
                let ids_as_json = u256_vec_to_json_decimal(&event.ids)?;
                let values_as_json = u256_vec_to_json_decimal(&event.values)?;
                let operator_address = checksum(&event.operator);
                let from_address = checksum(&event.from_address);
                let to_address = checksum(&event.to_address);
                let transaction_hash = &event.transaction_hash;

                transaction
                    .execute(
                        "INSERT INTO events (contract_id, operator, from_address, to_address, ids, values, block_number, transaction_hash) \
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[
                            &contract_id,
                            &operator_address,
                            &from_address,
                            &to_address,
                            &ids_as_json,
                            &values_as_json,
                            &(event.block_number as i32),
                            &transaction_hash,
                        ],
                    )
                    .await?;
            }
        }

        transaction
            .execute(
                "UPDATE contracts SET last_processed_block = $1 WHERE id = $2",
                &[&(to_block as i32), &contract_id],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(())
}
