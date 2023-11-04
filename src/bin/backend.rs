use afterlife_backend::backend::api::{self, get_or_update_all_users_collections};
use afterlife_backend::common::database;
use dotenv::dotenv;
use std::sync::Arc;
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() {
    println!("Starting Afterlife API, Insanity Edition");
    dotenv().ok();
    let api_db_client = database::connect().await.expect("Failed to connect to API database");
    let cache_db_client = database::connect().await.expect("Failed to connect to Cache database");

    let shared_api_db_client = Arc::new(api_db_client);
    let shared_cache_db_client = Arc::new(cache_db_client);

    // Define the period of the cache update task
    let update_period = Duration::from_secs(60); // 60 seconds
    let mut interval = time::interval(update_period);

    tokio::spawn(async move {
        loop {
            interval.tick().await;
            if let Err(e) = get_or_update_all_users_collections(&shared_cache_db_client, true).await {
                eprintln!("Failed to update cache: {:?}", e);
            }
        }
    });

    // The server uses the original shared API client
    api::run_server(shared_api_db_client).await;
}