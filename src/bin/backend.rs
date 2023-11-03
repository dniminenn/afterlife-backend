use afterlife_backend::backend::api::{self, get_or_update_all_users_collections};
use afterlife_backend::common::database;
use dotenv::dotenv;
use std::sync::Arc;
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db_client = database::connect().await.expect("Failed to connect to database");

    let shared_db_client = Arc::new(db_client);

    // Clone the shared client for the update task
    let update_client = shared_db_client.clone();

    // Define the period of the cache update task
    let update_period = Duration::from_secs(30); // 30 seconds
    let mut interval = time::interval(update_period);

    tokio::spawn(async move {
        loop {
            interval.tick().await;
            if let Err(e) = get_or_update_all_users_collections(&update_client, true).await {
                eprintln!("Failed to update cache: {:?}", e);
            }
        }
    });

    // The server uses the original shared client
    api::run_server(shared_db_client).await;
}