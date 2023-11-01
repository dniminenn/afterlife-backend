use afterlife_backend::common::database;
use afterlife_backend::indexer::indexer_config::IndexerConfig;
use afterlife_backend::indexer::queries::{get_earliest_last_processed_block, Event};
use afterlife_backend::indexer::remote_calls::EventFetcher;
use dotenv::dotenv;
use serde_yaml;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let db_client = database::connect()
        .await
        .expect("Failed to connect to database");
    println!("Starting indexer...");

    let config = IndexerConfig::from_env().expect("Failed to load indexer config");

    for chain in &config.chains {
        let earliest_last_processed_block = get_earliest_last_processed_block(chain, &db_client)
            .await
            .expect("Failed to get earliest last processed block");
        let mut event_fetcher = EventFetcher::new(&chain, earliest_last_processed_block as usize);
        let events = event_fetcher.execute().await.expect("Failed to fetch events");
        println!("Fetched {} events", events.len());
    }
}
