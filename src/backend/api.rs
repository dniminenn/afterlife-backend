use crate::backend;
use crate::backend::queries::{get_all_users_collections, get_user_full_collection};
use crate::common;
use backend::queries;
use common::file_loader::{load_users_data, read_file};
use eth_checksum::checksum;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::{env, fs};
use tokio_postgres::Client;
use warp::reject::{Reject, Rejection};
use warp::{Filter, Reply};
use warp::http::Response;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

#[derive(Debug)]
struct CustomReject(String);

impl Reject for CustomReject {}

#[derive(serde::Serialize)]
struct ErrorResponse {
    message: String,
}

type CollectionsType = HashMap<String, HashMap<String, HashMap<String, HashMap<u64, i64>>>>;
static ALL_USERS_COLLECTIONS_CACHE: Lazy<Mutex<Option<CollectionsType>>> = Lazy::new(|| Mutex::new(None));

pub async fn run_server(client: Arc<Client>) {
    //let client = Arc::new(client);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "OPTIONS"])
        .allow_headers(vec!["Content-Type"]);

    let routes = warp::path!(String / String / "collection" / String)
        .and(warp::get())
        .and(with_db(client.clone()))
        .and_then(handle_get_collection_for_address)
        .or(warp::path!(String / String / "collection")
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handle_get_entire_collection))
        .or(warp::path!(String / String / "owners" / u64)
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handle_get_token_owners))
        .or(warp::path!("get-username")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(handle_get_username_by_wallet))
        .or(warp::path!("fullcollection" / String)
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handle_get_user_full_collection))
        .or(warp::path!("rarity-score" / String)
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handle_get_user_rarity_score))
        .or(warp::path!("leaderboard")
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handler_leaderboard))
        .map(|reply| warp::reply::with_header(reply, "Access-Control-Allow-Origin", "*"))
        .with(cors);

    warp::serve(routes.recover(handle_custom_rejection))
        .run(([127, 0, 0, 1], 3030))
        .await;
}

fn with_db(client: Arc<Client>) -> impl Filter<Extract = (Arc<Client>,), Error = Infallible> + Clone {
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
                    filtered_details
                        .insert("rarity_score".to_owned(), json!(rarity_score * 1000.0));
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
    match queries::get_entire_collection_for_address(
        &*client,
        &chain_name,
        &contract_address,
        &wallet_address,
    )
    .await
    .map_err(|e| format!("Failed to get collection: {}", e))
    {
        Ok(balances) => {
            //println!("Found {} balances for {} on {}", balances.len(), wallet_address, contract_address);
            let rarity_path = format!(
                "{}/{}_{}_rarity.json",
                path_rarities,
                chain_name,
                checksum(contract_address.as_str())
            );
            let rarity_data = read_file(Path::new(&rarity_path)).await;
            let rarity_map = build_rarity_map(rarity_data);

            let mut tokens: HashMap<u64, Value> = HashMap::new();
            for (token_id, balance) in balances {
                let metadata_path = format!(
                    "{}/{}/{}/{}.json",
                    path_metadata,
                    chain_name,
                    checksum(contract_address.as_str()),
                    token_id
                );
                let metadata = read_file(Path::new(&metadata_path)).await;
                if let Some((token_id, mut token_details)) =
                    build_token_details(token_id, metadata, &rarity_map)
                {
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
    match queries::get_entire_collection(&*client, &chain_name, &contract_address)
        .await
        .map_err(|e| format!("Failed to get entire collection: {}", e))
    {
        Ok(token_ids) => {
            let rarity_path = format!(
                "{}/{}_{}_rarity.json",
                path_rarities,
                chain_name,
                checksum(contract_address.as_str())
            );
            let rarity_data = fs::read_to_string(&rarity_path).unwrap_or_else(|_| String::new());
            let rarity_map = build_rarity_map(Ok(rarity_data));

            let tokens: HashMap<u64, Value> = token_ids
                .into_iter()
                .filter_map(|token_id| {
                    let metadata_path = format!(
                        "{}/{}/{}/{}.json",
                        path_metadata,
                        chain_name,
                        checksum(contract_address.as_str()),
                        token_id
                    );
                    let metadata = fs::read_to_string(&metadata_path);
                    build_token_details(token_id, metadata, &rarity_map)
                })
                .collect();

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
            warp::reply::json(&json!(owners)),
            warp::http::StatusCode::OK,
        )),
        Err(_) => Err(warp::reject::custom(CustomReject(
            "Failed to fetch token owners".to_string(),
        ))),
    }
}

async fn handle_get_username_by_wallet(
    body: HashMap<String, String>,
) -> Result<impl warp::Reply, Rejection> {
    //println!("Handling get username by wallet, body: {:?}", body);
    let wallet_address = match body.get("address") {
        Some(address) => address,
        None => {
            return Err(warp::reject::custom(CustomReject(
                "Address not provided".to_string(),
            )))
        }
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

async fn handle_get_user_full_collection(
    user_address: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    println!("Handling get user full collection, user_address: {}", user_address);
    match get_user_full_collection(&*client, &user_address).await {
        Ok(collection) => Ok(warp::reply::json(&collection).into_response()),
        Err(_) => Err(warp::reject::custom(CustomReject(
            "Failed to fetch user's full collection".to_string(),
        ))),
    }
}

async fn handle_get_user_rarity_score(
    user_address: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    let user_collection = match get_user_full_collection(&*client, &user_address).await {
        Ok(collection) => collection,
        Err(_) => {
            return Err(warp::reject::custom(CustomReject(
                "Failed to fetch user's full collection".to_string(),
            )))
        }
    };

    let path_rarities = env::var("AFTERLIFE_PATH_RARITIES").unwrap();
    let mut total_rarity_score: f64 = 0.0;

    for (chain, contracts) in user_collection {
        for (contract_address, tokens) in contracts {
            let rarity_path = format!(
                "{}/{}_{}_rarity.json",
                path_rarities,
                chain,
                checksum(contract_address.as_str())
            );

            let rarity_data = read_file(Path::new(&rarity_path)).await;
            let rarity_map = build_rarity_map(rarity_data);

            for (token_id, balance) in tokens {
                if let Some((rarity_score, _)) = rarity_map.get(&token_id) {
                    // Multiply the rarity score by the balance as the user might have multiple of the same token.
                    total_rarity_score += rarity_score * balance as f64;
                }
            }
        }
    }
    // Multiply by 1000 and round to nearest integer
    total_rarity_score = (total_rarity_score * 1000.0).round();
    Ok(warp::reply::json(&json!({ "total_rarity_score": total_rarity_score })).into_response())
}

async fn handler_leaderboard(
    client: Arc<Client>,
) -> Result<impl Reply, Rejection> {
    let all_users_collections = get_or_update_all_users_collections(&*client).await?;

    let path_rarities = env::var("AFTERLIFE_PATH_RARITIES").expect("Expected AFTERLIFE_PATH_RARITIES to be set");

    let mut leaderboard: Vec<(String, f64)> = Vec::new();

    for (user_address, user_collection) in all_users_collections {
        let mut total_rarity_score: f64 = 0.0;

        for (chain, contracts) in user_collection {
            for (contract_address, tokens) in contracts {
                let rarity_path = format!(
                    "{}/{}_{}_rarity.json",
                    path_rarities,
                    chain,
                    checksum(contract_address.as_str())
                );

                let rarity_data = read_file(Path::new(&rarity_path)).await;
                let rarity_map = build_rarity_map(rarity_data);

                for (token_id, balance) in tokens {
                    if let Some((rarity_score, _)) = rarity_map.get(&token_id) {
                        total_rarity_score += rarity_score * balance as f64;
                    }
                }
            }
        }

        total_rarity_score = (total_rarity_score * 1000.0).round();
        leaderboard.push((user_address, total_rarity_score));
    }

    // Sort the leaderboard by rarity score in descending order
    //leaderboard.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let mut json_leaderboard = serde_json::Map::new();
    for (address, score) in leaderboard {
        // Insert the address and score into the map, converting the score to an integer.
        json_leaderboard.insert(address, serde_json::Value::Number(serde_json::Number::from(score as i64)));
    }

    // Serialize the map into a JSON object
    let json_response = serde_json::Value::Object(json_leaderboard);

    Ok(warp::reply::json(&json_response).into_response())
}

pub async fn get_or_update_all_users_collections(client: &Client) -> Result<CollectionsType, Rejection> {
    let mut cache = ALL_USERS_COLLECTIONS_CACHE.lock().await;
    if let Some(cached) = cache.as_ref() {
        Ok(cached.clone()) // Return cached value if present
    } else {
        // If not cached, get from the function and cache it
        match get_all_users_collections(client).await {
            Ok(collections) => {
                *cache = Some(collections.clone());
                Ok(collections)
            }
            Err(_) => Err(warp::reject::custom(CustomReject(
                "Failed to fetch collections for all users".to_string(),
            ))),
        }
    }
}