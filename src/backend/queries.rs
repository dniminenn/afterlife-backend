use serde_json::from_str;
use std::collections::HashMap;
use std::option::Option;
use tokio_postgres::Row;
use warp::Filter;
use web3::types::U256;

const DEAD_ADDRESS: &str = "0x000000000000000000000000000000000000dEaD";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug)]
pub struct Event {
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub ids: Option<String>,
    pub values: Option<String>,
}

async fn row_to_event(row: Row) -> Event {
    Event {
        from_address: row.get("from_address"),
        to_address: row.get("to_address"),
        ids: row.get("ids"),
        values: row.get("values"),
    }
}
pub async fn get_entire_collection_for_address(
    client: &tokio_postgres::Client,
    chain_name: &str,
    contract_address: &str,
    wallet_address: &str,
) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send>> {
    let wallet_address_lowercase = wallet_address.to_lowercase();
    let rows = client
        .query(
            r#"
            SELECT e.from_address, e.to_address, e.ids, e.values
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE LOWER(c.address) = $1 AND LOWER(ch.name) = $2 AND (e.from_address = $3 OR e.to_address = $3)
            "#,
            &[&contract_address.to_lowercase(), &chain_name.to_lowercase(), &wallet_address_lowercase],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    let mut balances = HashMap::new();
    let mut events = Vec::new();

    for row in rows {
        events.push(row_to_event(row).await);
    }

    // To address events - incoming transfers
    for event in &events {
        if let Some(to_address) = &event.to_address {
            if to_address == &wallet_address_lowercase {
                let ids: Vec<u64> = event
                    .ids
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                let values: Vec<u64> = event
                    .values
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();

                for (&id, &value) in ids.iter().zip(values.iter()) {
                    *balances.entry(id).or_insert(0) += value;
                }
            }
        }
    }

    // From address events - outgoing transfers
    for event in &events {
        if let Some(from_address) = &event.from_address {
            if from_address == &wallet_address_lowercase {
                let ids: Vec<u64> = event
                    .ids
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                let values: Vec<u64> = event
                    .values
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();

                for (&id, &value) in ids.iter().zip(values.iter()) {
                    *balances.entry(id).or_insert(0) = balances
                        .get(&id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(value);
                }
            }
        }
    }

    // Remove any items that have a zero balance
    balances.retain(|_, &mut value| value > 0);

    Ok(balances)
}

pub async fn get_entire_collection(
    client: &tokio_postgres::Client,
    chain_name: &str,
    contract_address: &str,
) -> Result<Vec<u64>, Box<dyn std::error::Error + Send>> {
    println!("Get entire collection for {} on {}", contract_address.to_lowercase(), chain_name);
    let rows = client
        .query(
            r#"
            SELECT e.from_address, e.to_address, e.ids, e.values
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE LOWER(c.address) = $1 AND LOWER(ch.name) = $2 AND (
                LOWER(e.from_address) = $3 OR
                LOWER(e.to_address) = $3 OR
                LOWER(e.to_address) = $4
            )
            "#,
            &[&contract_address.to_lowercase(), &chain_name.to_lowercase(), &ZERO_ADDRESS.to_lowercase(), &DEAD_ADDRESS.to_lowercase()],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    let mut existing_tokens = HashMap::new();  // HashMap<u64, i64>
    for row in rows {
        let event = row_to_event(row).await;
        let ids: Vec<u64> = event.ids
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let values: Vec<u64> = event.values
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let multiplier = if event.from_address.as_deref() == Some(ZERO_ADDRESS) {
            1i64
        } else if event.to_address.as_deref() == Some(DEAD_ADDRESS) || event.to_address.as_deref() == Some(ZERO_ADDRESS) {
            -1i64
        } else {
            continue;
        };

        for (&id, &value) in ids.iter().zip(values.iter()) {
            *existing_tokens.entry(id).or_insert(0) += value as i64 * multiplier;
        }
    }

    let result: Vec<u64> = existing_tokens.into_iter().filter_map(|(id, count)| {
        if count > 0 { Some(id) } else { None }
    }).collect();

    Ok(result)
}



pub async fn get_token_owners(
    client: &tokio_postgres::Client,
    chain_name: &str,
    contract_address: &str,
    token_id: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send>> {

    // Retrieve token events from the database
    let rows = client
        .query(
            r#"
            SELECT e.from_address, e.to_address, e.ids, e.values
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE LOWER(c.address) = $1 AND LOWER(ch.name) = $2
            "#,
            &[&contract_address.to_lowercase(), &chain_name.to_lowercase()],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    let mut events = Vec::new();
    for row in rows {
        events.push(row_to_event(row).await);
    }

    // Process events to determine token owners and their balances
    let mut owners: HashMap<String, i64> = HashMap::new();  // Using i64 to allow for negative values temporarily

    for event in events {
        let ids: Vec<u64> = event
            .ids.as_deref()
            .and_then(|s| from_str(s).ok())
            .unwrap_or_default();
        let values: Vec<i64> = event
            .values.as_deref()
            .and_then(|s| from_str::<Vec<u64>>(s).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|v| v as i64)  // Convert u64 values to i64 for arithmetic
            .collect();

        for (&id, &value) in ids.iter().zip(values.iter()) {
            if id == token_id {
                if let Some(to_address) = &event.to_address {
                    *owners.entry(to_address.clone()).or_insert(0) += value;
                }
                if let Some(from_address) = &event.from_address {
                    *owners.entry(from_address.clone()).or_insert(0) -= value;
                }
            }
        }
    }
    // Remove 0x000000000000000000000000000000000000dEaD from results
    owners.remove("0x000000000000000000000000000000000000dead");

    // Filter out the addresses with zero balances and collect the owners
    let owner_addresses: Vec<String> = owners
        .into_iter()
        .filter(|&(_, value)| value > 0)
        .map(|(address, _)| address)
        .collect();

    Ok(owner_addresses)
}

/*pub async fn get_token_owners(
    client: &tokio_postgres::Client,
    chain_name: &str,
    contract_address: &str,
    token_id: u64,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error + Send>> {
    // Retrieve token events from the database
    let rows = client
        .query(
            r#"
            SELECT e.from_address, e.to_address, e.ids, e.values
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE LOWER(c.address) = $1 AND LOWER(ch.name) = $2
            "#,
            &[&contract_address.to_lowercase(), &chain_name.to_lowercase()],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    let mut events = Vec::new();
    for row in rows {
        events.push(row_to_event(row).await);
    }

    // Process events to determine token owners and their balances
    let mut owners: HashMap<String, u64> = HashMap::new();  // Use u64 for balances

    for event in &events {
        let ids: Vec<u64> = event.ids.as_ref()
            .and_then(|s| from_str(s).ok())
            .unwrap_or_default();
        let values: Vec<u64> = event.values.as_ref()
            .and_then(|s| from_str(s).ok())
            .unwrap_or_default();

        for (id, value) in ids.iter().zip(values.iter()) {
            if *id == token_id {
                if let Some(to_address) = &event.to_address {
                    *owners.entry(to_address.clone()).or_insert(0) += value;
                }
                if let Some(from_address) = &event.from_address {
                    *owners.entry(from_address.clone()).or_insert(0) = owners.get(from_address).or(Some(&0)).unwrap().saturating_sub(*value);
                }
            }
        }
    }

    // Remove 0x000000000000000000000000000000000000dEaD from results
    owners.remove("0x000000000000000000000000000000000000dead");

    // Remove any addresses with a zero balance
    owners.retain(|_, &mut value| value > 0);

    println!("Found {} owners", owners.len());

    Ok(owners)
}*/

