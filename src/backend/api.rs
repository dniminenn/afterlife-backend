use crate::backend;
use crate::backend::queries::{get_all_users_collections, get_user_full_collection, get_contract_name_from_chain_and_address};
use crate::backend::usernames::{get_all_addresses_for_username, get_username_or_checksummed_address};
use crate::common;
use backend::queries;
use common::file_loader::{read_file};
use eth_checksum::checksum;
use once_cell::sync::Lazy;
use serde_json::{json, Map, Number, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::{env, fs};
use futures::future::try_join_all;
use tokio::sync::Mutex;
use tokio::task;
use tokio_postgres::Client;
use warp::reject::{Reject, Rejection};
use warp::http::header::{HeaderValue, CACHE_CONTROL};
use warp::{Filter, Reply, http::Response};
use warp::reply::with_header;

#[derive(Debug)]
struct CustomReject(String);

impl Reject for CustomReject {}

#[derive(serde::Serialize)]
struct ErrorResponse {
    message: String,
}

type CollectionsType = HashMap<String, HashMap<String, HashMap<String, HashMap<u64, i64>>>>;
type LeaderboardType = HashMap<String, f64>;
static ALL_USERS_LEADERBOARD_CACHE: Lazy<Mutex<Option<LeaderboardType>>> = Lazy::new(|| Mutex::new(None));

// define const of excluded users or addresses for the leaderboard
const EXCLUDED_USERS: [&str; 4] = ["Danetron3030", "AfterlifeTreasury", "0x3cc35873a61D925Ac46984f8C4F85d8fa6A892eF", "AfterlifeCoinBank"];

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
        .or(warp::path!("user" / "level" / String)
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handle_get_user_details))
        .or(warp::path!("leaderboard")
            .and(warp::get())
            .and(with_db(client.clone()))
            .and_then(handler_leaderboard))
        .with(cors)
        .with(warp::reply::with::header("Cache-Control", "public, max-age=60"));

    warp::serve(routes.recover(handle_custom_rejection))
        .run(([127, 0, 0, 1], 3030))
        .await;
}

fn with_db(
    client: Arc<Client>,
) -> impl Filter<Extract = (Arc<Client>,), Error = Infallible> + Clone {
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
    let wallet_address = body
        .get("address")
        .ok_or_else(|| warp::reject::custom(CustomReject("Address not provided".to_string())))?;

    match get_username_or_checksummed_address(wallet_address).await {
        Ok(Some(result)) => Ok(warp::reply::with_status(
            warp::reply::json(&json!({ "username": result })),
            warp::http::StatusCode::OK,
        )),
        Ok(None) => Err(warp::reject::custom(CustomReject(
            "Wallet address not found".to_string(),
        ))),
        Err(error_message) => Err(warp::reject::custom(CustomReject(error_message))),
    }
}

async fn handle_get_user_full_collection(
    user_address: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    println!(
        "Handling get user full collection, user_address: {}",
        user_address
    );
    match get_user_full_collection(&*client, &user_address).await {
        Ok(collection) => Ok(warp::reply::json(&collection).into_response()),
        Err(_) => Err(warp::reject::custom(CustomReject(
            "Failed to fetch user's full collection".to_string(),
        ))),
    }
}

async fn handle_get_user_details(
    username: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    let user_addresses = get_all_addresses_for_username(&username).await;
    let mut total_rarity_score: f64 = 0.0;
    let mut collection_scores = HashMap::new();
    let mut all_nfts: HashMap<String, HashMap<String, Vec<_>>> = HashMap::new();
    let mut top_nfts = Vec::new();

    let path_rarities = env::var("AFTERLIFE_PATH_RARITIES")
        .map_err(|_| warp::reject::custom(CustomReject("Environment variable AFTERLIFE_PATH_RARITIES not set".to_string())))?;
    let path_metadata = env::var("AFTERLIFE_PATH_METADATA").unwrap();

    for user_address in &user_addresses {
        let user_collection = get_user_full_collection(&*client, user_address).await
            .map_err(|_| warp::reject::custom(CustomReject("Failed to fetch user's full collection".to_string())))?;

        for (chain, contracts) in user_collection {
            for (contract_address, tokens) in contracts {
                let contract_name = get_contract_name_from_chain_and_address(&*client, &chain, &contract_address).await
                    .map_err(|_| warp::reject::custom(CustomReject("Failed to fetch contract name".to_string())))?;
                let rarity_path = format!("{}/{}_{}_rarity.json", path_rarities, chain, checksum(contract_address.as_str()));
                let rarity_data = read_file(Path::new(&rarity_path)).await;
                let rarity_map = build_rarity_map(rarity_data);
                let collection_name = format!("{}_{}", chain, contract_name);

                for (token_id, balance) in tokens {
                    if let Some((rarity_score, _)) = rarity_map.get(&token_id) {
                        let metadata_path = format!("{}/{}/{}/{}.json", path_metadata, chain, checksum(contract_address.as_str()), token_id);
                        let metadata = read_file(Path::new(&metadata_path)).await;
                        let token_details = build_token_details(token_id, metadata, &rarity_map);
                        let mut token_name = "".to_string();
                        if let Some((_, token_details)) = token_details {
                            token_name = token_details["name"].as_str().unwrap_or("").to_string();
                        }
                        let score = rarity_score * balance as f64;
                        total_rarity_score += score;
                        *collection_scores.entry(collection_name.clone()).or_insert(0.0) += score;
                        top_nfts.push((rarity_score.clone(), token_id, contract_address.clone(), chain.clone(), token_name.clone()));
                        let chain_map = all_nfts.entry(chain.clone()).or_default();
                        let contract_tokens = chain_map.entry(contract_address.clone()).or_default();

                        contract_tokens.push(json!({
                        "rarity_score": (rarity_score * 1000.0).round(),
                        "score": (rarity_score * 1000.0 * (balance as f64)).round(),
                        "token_id": token_id,
                        "balance": balance,
                        "token_name": token_name,
                        }));
                    }
                }
            }
        }
    }

    // Sort by score in descending order and take the top 10 NFTs
    top_nfts.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let top_nfts: Vec<_> = top_nfts.clone().into_iter().take(10).collect();

    // Process other data as before
    total_rarity_score = (total_rarity_score * 1000.0).round();
    let addresses: Vec<String> = user_addresses.into_iter().collect();
    let mut collections: Vec<_> = collection_scores.into_iter()
        .map(|(k, v)| (k, (v * 1000.0).round()))
        .collect();
    collections.sort_by(|a, b| a.0.cmp(&b.0));

    // Construct final JSON response including top NFTs
    let response = json!({
        "username": username,
        "addresses": addresses,
        "afterlifepoints": total_rarity_score,
        "level": backend::usernames::points_to_level(total_rarity_score as i32),
        "collection_scores": collections.into_iter().collect::<HashMap<_, _>>(),
        "all_nfts": all_nfts,
        "top_nfts": top_nfts.into_iter().map(|(rarity_score, token_id, contract_address, chain, name)| json!({
            "rarity_score": (rarity_score * 1000.0).round(), // round to nearest integer
            "token_id": token_id,
            "contract_address": contract_address,
            "chain": chain,
            "token_name": name,
        })).collect::<Vec<_>>(),
    });

    Ok(warp::reply::json(&response).into_response())
}

async fn handler_leaderboard(client: Arc<Client>) -> Result<impl Reply, Rejection> {
    // Retrieve the precomputed leaderboard from the cache.
    let leaderboard = get_or_update_all_users_collections(&*client, false).await?;

    // Convert the leaderboard HashMap into a JSON value.
    let mut json_leaderboard = Map::new();
    for (username_or_addr, score) in leaderboard {
        json_leaderboard.insert(
            username_or_addr,
            Value::Number(Number::from_f64(score).expect("Invalid score")),
        );
    }

    let json_response = Value::Object(json_leaderboard);
    Ok(warp::reply::json(&json_response).into_response())
}

pub async fn get_or_update_all_users_collections(
    client: &Client,
    force_update: bool,
) -> Result<LeaderboardType, Rejection> {
    let mut cache = ALL_USERS_LEADERBOARD_CACHE.lock().await;

    // If the cache is not populated or a forced update is needed, compute the leaderboard.
    if cache.is_none() || force_update {
        let path_rarities =
            env::var("AFTERLIFE_PATH_RARITIES").expect("Expected AFTERLIFE_PATH_RARITIES to be set");
        // Fetch new data because either cache is empty or we're forcing an update.
        let all_users_collections = match get_all_users_collections(client).await {
            Ok(collections) => collections,
            Err(_) => return Err(warp::reject::custom(CustomReject(
                "Failed to fetch collections for all users".to_string(),
            ))),
        };

        let mut tasks = Vec::new();

        for (user_address, user_collection) in all_users_collections {
            let path_rarities = path_rarities.clone();
            let user_address = user_address.clone();

            let task = task::spawn(async move {
                let username_or_addr = get_username_or_checksummed_address(&user_address).await
                    .unwrap_or(Some(user_address.clone())).unwrap_or_default();

                // check if user is in const EXCLUDED_USERS
                if EXCLUDED_USERS.contains(&username_or_addr.as_str()) {
                    return Ok::<_, Rejection>((username_or_addr, 0.0));
                }

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

                // Multiply by 1000 and round to nearest integer as per your original logic.
                total_rarity_score = (total_rarity_score * 1000.0).round();

                Ok::<_, Rejection>((username_or_addr, total_rarity_score))
            });

            tasks.push(task);
        }

        let mut leaderboard: LeaderboardType = HashMap::new();
        let results = try_join_all(tasks).await.map_err(|e| {
            warp::reject::custom(CustomReject(format!("Task join error: {}", e)))
        })?;
        for task_result in results {
            match task_result {
                Ok((address, score)) => {
                    if score > 0.0 { leaderboard.insert(address, score); }
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }

        *cache = Some(leaderboard.clone());

    }

    // The cache is either freshly populated or was already available.
    cache.clone().ok_or_else(|| warp::reject::custom(CustomReject(
        "Leaderboard cache is not available".to_string(),
    )))
}
