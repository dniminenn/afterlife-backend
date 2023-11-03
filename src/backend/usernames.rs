use web3::types::Address;
use crate::common::file_loader::load_users_data;
use eth_checksum::checksum;
use std::collections::{HashMap, HashSet};

pub async fn get_username_or_checksummed_address(
    wallet_address: &str,
) -> Result<Option<String>, String> {
    let address = wallet_address.parse::<Address>().map_err(|_| "Invalid address".to_string())?;
    let users_data = load_users_data().await;

    let mut address_to_username = HashMap::new();
    for (username, addresses) in &users_data {
        for addr in addresses {
            address_to_username.insert(addr.to_lowercase(), username.clone());
        }
    }

    let address_str = format!("{:?}", address).to_lowercase();
    // Return the username if found, otherwise return the checksummed address
    Ok(address_to_username
        .get(&address_str)
        .cloned()
        .or_else(|| Some(checksum(&address_str))))
}

pub async fn get_addresses_for_username(username: &str) -> HashSet<String> {
    let users_data = load_users_data().await;
    let mut found_addresses = HashSet::new();
    // check if username is a valid address
    // Let's see first if there's a match for the username
    if let Some(addresses) = users_data.get(username) { // we have a match!
        for address in addresses {
            found_addresses.insert(address.clone());
        }
    } else { // we don't have a match, but maybe the username is a valid address
        let isvalid = username.parse::<Address>().is_ok();
        if isvalid {
            // we have a valid address, maybe it's in the users_data, let's check
            // and get other addresses associated with it
            if let Some(addresses) = users_data.get(username) {
                for address in addresses {
                    found_addresses.insert(address.clone());
                }
            } else {
                // we have a valid address, but it's not in the users_data, so we just return it
                found_addresses.insert(username.to_string());
            }
        }
    }
    // return the addresses, if any, if not, it will be an empty HashSet
    found_addresses
}