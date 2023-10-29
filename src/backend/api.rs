use crate::backend;
use crate::common;
use backend::queries;
use common::file_loader::{read_file, load_users_data};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::{env, fs};
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::Client;
use warp::reject::{Reject, Rejection};
use warp::Filter;

#[derive(Debug)]
struct CustomReject(String);

impl Reject for CustomReject {}

#[derive(serde::Serialize)]
struct ErrorResponse {
    message: String,
}

pub async fn run_server(client: Client) {
    let client = Arc::new(client);

    let cors = warp::cors()
        .allow_methods(vec!["POST", "GET"])
        .allow_headers(vec!["Authorization", "Content-Type"]);

    let routes = warp::path!(String / String / "collection" / String)
        .and(warp::get())
        .and(with_db(client.clone()))
        .and_then(handle_get_collection_for_address)
        .or(
            warp::path!(String / String / "collection")
                .and(warp::get())
                .and(with_db(client.clone()))
                .and_then(handle_get_entire_collection),
        )
        .or(
            warp::path!(String / String / "owners" / u64)
                .and(warp::get())
                .and(with_db(client.clone()))
                .and_then(handle_get_token_owners)
        )
        .or(
            warp::path!("get-username")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(handle_get_username_by_wallet),
        )
        .map(|reply| warp::reply::with_header(reply, "Access-Control-Allow-Origin", "*"))
        .with(cors);

    warp::serve(routes.recover(handle_custom_rejection))
        .run(([127, 0, 0, 1], 3030))
        .await;
}

fn with_db(
    client: Arc<Client>,
) -> impl Filter<Extract = (Arc<Client>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || client.clone())
}

async fn handle_custom_rejection(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(custom_err) = err.find::<CustomReject>() {
        let error_response = ErrorResponse {
            message: custom_err.0.clone(),
        };
        let json = warp::reply::json(&error_response);
        return Ok(warp::reply::with_status(
            json,
            warp::http::StatusCode::BAD_REQUEST,
        ));
    }

    let error_response = ErrorResponse {
        message: "Unhandled error".to_string(),
    };
    let json = warp::reply::json(&error_response);
    Ok(warp::reply::with_status(
        json,
        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
    ))
}

fn build_rarity_map(rarity_data: Result<String, std::io::Error>) -> HashMap<u64, (f64, u64)> {
    let mut rarity_map: HashMap<u64, (f64, u64)> = HashMap::new();
    if let Ok(rarity_json) = rarity_data {
        if let Ok(rarities) = serde_json::from_str::<Vec<Value>>(&rarity_json) {
            for rarity in rarities {
                if let Some(rarity_obj) = rarity.as_object() {
                    if let (Some(token_id), Some(rarity_score), Some(rarity_index)) = (
                        rarity_obj.get("token_id").and_then(|v| v.as_u64()),
                        rarity_obj.get("rarity_score").and_then(|v| v.as_f64()),
                        rarity_obj.get("rarity_index").and_then(|v| v.as_u64()),
                    ) {
                        rarity_map.insert(token_id, (rarity_score, rarity_index));
                    }
                }
            }
        }
    }
    rarity_map
}

fn build_token_details(
    token_id: u64,
    metadata: Result<String, std::io::Error>,
    rarity_map: &HashMap<u64, (f64, u64)>,
) -> Option<(u64, Value)> {
    if let Ok(metadata) = metadata {
        if let Ok(token_details) = serde_json::from_str::<Value>(&metadata) {
            if let Some(token_details_map) = token_details.as_object() {
                let mut filtered_details = HashMap::new();
                if let Some(description) = token_details_map.get("description") {
                    filtered_details.insert("description".to_owned(), description.clone());
                }
                if let Some(attributes) = token_details_map.get("attributes") {
                    filtered_details.insert("attributes".to_owned(), attributes.clone());
                }
                if let Some(&(rarity_score, rarity_index)) = rarity_map.get(&token_id) {
                    filtered_details.insert(
                        "rarity_score".to_owned(),
                        json!(rarity_score * 1000.0)
                    );
                    filtered_details.insert("rarity_index".to_owned(), json!(rarity_index));
                }
                if let Some(name) = token_details_map.get("name") {
                    filtered_details.insert("name".to_owned(), name.clone());
                }

                return Some((token_id, json!(filtered_details)));
            }
        }
    }
    None
}
async fn handle_get_collection_for_address(
    chain_name: String,
    contract_address: String,
    wallet_address: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    // Fetch environment variables
    let path_rarities = env::var("AFTERLIFE_PATH_RARITIES").unwrap();
    let path_metadata = env::var("AFTERLIFE_PATH_METADATA").unwrap();
    match queries::get_entire_collection_for_address(&*client, &chain_name, &contract_address, &wallet_address)
        .await
        .map_err(|e| format!("Failed to get collection: {}", e))
    {
        Ok(balances) => {
            let rarity_path = format!("{}/{}_{}_rarity.json", path_rarities, chain_name, contract_address);
            let rarity_data = read_file(Path::new(&rarity_path)).await;
            let rarity_map = build_rarity_map(rarity_data);

            let mut tokens: HashMap<u64, Value> = HashMap::new();
            for (token_id, balance) in balances {
                let metadata_path = format!("{}/{}/{}/{}.json", path_metadata, chain_name, contract_address, token_id);
                let metadata = read_file(Path::new(&metadata_path)).await;
                if let Some((token_id, mut token_details)) = build_token_details(token_id, metadata, &rarity_map) {
                    token_details["balance"] = json!(balance);
                    tokens.insert(token_id, token_details);
                }
            }

            Ok(warp::reply::with_status(
                warp::reply::json(&json!({ "tokens": tokens })),
                warp::http::StatusCode::OK,
            ))
        }
        Err(err_str) => Err(warp::reject::custom(CustomReject(err_str))),
    }
}

async fn handle_get_entire_collection(
    chain_name: String,
    contract_address: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    // Fetch environment variables
    let path_rarities = env::var("AFTERLIFE_PATH_RARITIES").unwrap();
    let path_metadata = env::var("AFTERLIFE_PATH_METADATA").unwrap();
    match queries::get_entire_collection(&*client, &chain_name, &contract_address).await
        .map_err(|e| format!("Failed to get entire collection: {}", e))
    {
        Ok(token_ids) => {
            let rarity_path = format!("{}/{}_{}_rarity.json", path_rarities, chain_name, contract_address);
            let rarity_data = fs::read_to_string(&rarity_path).unwrap_or_else(|_| String::new());
            let rarity_map = build_rarity_map(Ok(rarity_data));

            let tokens: HashMap<u64, Value> = token_ids.into_iter().filter_map(|token_id| {
                let metadata_path = format!("{}/{}/{}/{}.json", path_metadata, chain_name, contract_address, token_id);
                let metadata = fs::read_to_string(&metadata_path);
                build_token_details(token_id, metadata, &rarity_map)
            }).collect();

            Ok(warp::reply::with_status(
                warp::reply::json(&json!({ "tokens": tokens })),
                warp::http::StatusCode::OK,
            ))
        }
        Err(err_str) => Err(warp::reject::custom(CustomReject(err_str))),
    }
}

async fn handle_get_token_owners(
    chain_name: String,
    contract_address: String,
    token_id: u64,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    match queries::get_token_owners(&*client, &chain_name, &contract_address, token_id).await {
        Ok(owners) => Ok(warp::reply::with_status(
            warp::reply::json(&json!( owners )),
            warp::http::StatusCode::OK,
        )),
        Err(_) => Err(warp::reject::custom(CustomReject("Failed to fetch token owners".to_string()))),
    }
}

async fn handle_get_username_by_wallet(body: HashMap<String, String>) -> Result<impl warp::Reply, Rejection> {
    println!("Handling get username by wallet, body: {:?}", body);
    let wallet_address = match body.get("address") {
        Some(address) => address,
        None => return Err(warp::reject::custom(CustomReject("Address not provided".to_string()))),
    };

    let users_data = load_users_data().await;

    match users_data.get(wallet_address) {
        Some(username) => Ok(warp::reply::with_status(
            warp::reply::json(&json!({ "username": username })),
            warp::http::StatusCode::OK,
        )),
        None => Err(warp::reject::custom(CustomReject(
            "Wallet address not found".to_string(),
        ))),
    }
}