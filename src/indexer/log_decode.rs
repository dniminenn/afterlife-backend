extern crate ethabi;
use ethabi::{Event, RawLog, Token};
use web3::types::{Log, U256};

fn token_to_u256(token: &Token) -> Option<U256> {
    if let Token::Uint(value) = token {
        Some(*value)
    } else {
        None
    }
}

// Function to decode batch event using predefined ABI
pub(crate) fn decode_erc1155_transfer_batch(
    log: &Log,
) -> Result<(Vec<U256>, Vec<U256>), ethabi::Error> {
    // Define the ERC1155 TransferBatch event signature
    let event = Event {
        name: "TransferBatch".into(),
        inputs: vec![
            ethabi::EventParam {
                name: "operator".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "from".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "to".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "ids".into(),
                kind: ethabi::ParamType::Array(Box::new(ethabi::ParamType::Uint(256))),
                indexed: false,
            },
            ethabi::EventParam {
                name: "values".into(),
                kind: ethabi::ParamType::Array(Box::new(ethabi::ParamType::Uint(256))),
                indexed: false,
            },
        ],
        anonymous: false,
    };

    // Create a RawLog from the log's topics and data
    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.0.clone(),
    };

    // Decode the log
    let decoded = event.parse_log(raw_log)?;

    // Extract the 'ids' and 'values' from the tokens
    // Extract the 'ids' and 'values' from the tokens using the token_to_u256 function
    let ids = if let Token::Array(tokens) = &decoded.params[3].value {
        tokens.iter().filter_map(token_to_u256).collect()
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    let values = if let Token::Array(tokens) = &decoded.params[4].value {
        tokens.iter().filter_map(token_to_u256).collect()
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    Ok((ids, values))
}

// Function to decode single event using predefined ABI
pub(crate) fn decode_erc1155_transfer_single(log: &Log) -> Result<(U256, U256), ethabi::Error> {
    // Define the ERC1155 TransferSingle event signature
    let event = Event {
        name: "TransferSingle".into(),
        inputs: vec![
            ethabi::EventParam {
                name: "operator".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "from".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "to".into(),
                kind: ethabi::ParamType::Address,
                indexed: true,
            },
            ethabi::EventParam {
                name: "id".into(),
                kind: ethabi::ParamType::Uint(256),
                indexed: false,
            },
            ethabi::EventParam {
                name: "value".into(),
                kind: ethabi::ParamType::Uint(256),
                indexed: false,
            },
        ],
        anonymous: false,
    };

    // Create a RawLog from the log's topics and data
    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.0.clone(),
    };

    // Decode the log
    let decoded = event.parse_log(raw_log)?;

    // Extract the 'id' and 'value' from the tokens
    let id = if let Some(id_u256) = token_to_u256(&decoded.params[3].value) {
        id_u256
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    let value = if let Some(value_u256) = token_to_u256(&decoded.params[4].value) {
        value_u256
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    Ok((id, value))
}
