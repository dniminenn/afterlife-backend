extern crate ethabi;
use ethabi::{Event, RawLog, Token};
use web3::types::Log;

// Function to decode batch event using predefined ABI
pub(crate) fn decode_erc1155_transfer_batch(log: &Log) -> Result<(Vec<u64>, Vec<u64>), ethabi::Error> {
    // Define the ERC1155 TransferBatch event signature
    let event = Event {
        name: "TransferBatch".into(),
        inputs: vec![
            ethabi::EventParam { name: "operator".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "from".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "to".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "ids".into(), kind: ethabi::ParamType::Array(Box::new(ethabi::ParamType::Uint(256))), indexed: false },
            ethabi::EventParam { name: "values".into(), kind: ethabi::ParamType::Array(Box::new(ethabi::ParamType::Uint(256))), indexed: false },
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
    let ids = if let Token::Array(tokens) = &decoded.params[3].value {
        tokens.iter().map(|token| if let Token::Uint(value) = token {
            value.as_u64()
        } else {
            0 // Handle error appropriately
        }).collect()
    } else {
        vec![] // Handle error appropriately
    };

    let values = if let Token::Array(tokens) = &decoded.params[4].value {
        tokens.iter().map(|token| if let Token::Uint(value) = token {
            value.as_u64()
        } else {
            0 // Handle error appropriately
        }).collect()
    } else {
        vec![] // Handle error appropriately
    };

    Ok((ids, values))
}

// Function to decode single event using predefined ABI
pub(crate) fn decode_erc1155_transfer_single(log: &Log) -> Result<(u64, u64), ethabi::Error> {
    // Define the ERC1155 TransferSingle event signature
    let event = Event {
        name: "TransferSingle".into(),
        inputs: vec![
            ethabi::EventParam { name: "operator".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "from".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "to".into(), kind: ethabi::ParamType::Address, indexed: true },
            ethabi::EventParam { name: "id".into(), kind: ethabi::ParamType::Uint(256), indexed: false },
            ethabi::EventParam { name: "value".into(), kind: ethabi::ParamType::Uint(256), indexed: false },
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
    let id = if let Token::Uint(value) = &decoded.params[3].value {
        value.as_u64()
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    let value = if let Token::Uint(value) = &decoded.params[4].value {
        value.as_u64()
    } else {
        return Err(ethabi::Error::InvalidData); // Handle error appropriately
    };

    Ok((id, value))
}