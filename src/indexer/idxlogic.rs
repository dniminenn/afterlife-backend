use crate::indexer::indexer_config;
use crate::indexer::remote_calls;
use crate::indexer::queries;
use tokio_postgres::{Client, Error};

// Collect events from the blockchain
/*pub async fn collect_events(chain: &indexer_config::Chain, last_processed_block: usize) -> Result<Vec<queries::Event>, remote_calls::EventFetcherError> {
    let mut event_fetcher = remote_calls::EventFetcher::new(chain, last_processed_block);
    let events = event_fetcher.execute().await?;
    Ok(events)
}*/

// Compare the obtained events with the events in the database, remove duplicates,
// and insert the new events into the database
//pub async fn process_chunk(