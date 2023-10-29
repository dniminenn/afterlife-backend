use serde_json::from_str;
use std::collections::HashMap;
use std::option::Option;
use tokio_postgres::Row;
use warp::Filter;

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
    let rows = client
        .query(
            r#"
            SELECT e.from_address, e.to_address, e.ids, e.values
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE c.address = $1 AND ch.name = $2 AND (e.from_address = $3 OR e.to_address = $3)
            "#,
            &[&contract_address, &chain_name, &wallet_address],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    let mut balances = HashMap::new();
    let mut events = Vec::new();

    for row in rows {
        events.push(row_to_event(row).await);
    }

    let to_address_events: Vec<&Event> = events
        .iter()
        .filter(|e| Some(wallet_address) == e.to_address.as_deref())
        .collect();

    for event in to_address_events {
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

    let from_address_events: Vec<&Event> = events
        .iter()
        .filter(|e| Some(wallet_address) == e.from_address.as_deref())
        .collect();

    for event in from_address_events {
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

    balances.retain(|_, &mut value| value > 0);

    Ok(balances)
}

pub async fn get_entire_collection(
    client: &tokio_postgres::Client,
    chain_name: &str,
    contract_address: &str,
) -> Result<Vec<u64>, Box<dyn std::error::Error + Send>> {
    // Query to get all unique token ids for the specified chain and contract address
    let rows = client
        .query(
            r#"
            SELECT DISTINCT jsonb_array_elements_text(e.ids::jsonb) as token_id_string
            FROM events e
            JOIN contracts c ON e.contract_id = c.id
            JOIN chains ch ON c.chain_id = ch.id
            WHERE c.address = $1 AND ch.name = $2
            "#,
            &[&contract_address, &chain_name],
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    // Map the retrieved rows to Vec<u64> and then sort
    let mut token_ids: Vec<u64> = rows
        .iter()
        .filter_map(|row| {
            row.get::<_, Option<String>>("token_id_string")
                .and_then(|id_string| id_string.parse::<u64>().ok())
        })
        .collect();

    token_ids.sort(); // Sorting the token IDs

    Ok(token_ids)
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
            WHERE c.address = $1 AND ch.name = $2
            "#,
            &[&contract_address, &chain_name],
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
    owners.remove("0x000000000000000000000000000000000000dEaD");

    // Filter out the addresses with zero balances and collect the owners
    let owner_addresses: Vec<String> = owners
        .into_iter()
        .filter(|&(_, value)| value > 0)
        .map(|(address, _)| address)
        .collect();

    Ok(owner_addresses)
}

