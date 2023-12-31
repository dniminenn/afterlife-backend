use afterlife_backend::common::database;
use afterlife_backend::indexer::indexer_config::IndexerConfig;
use afterlife_backend::indexer::queries::{
    contract_and_chain_to_contractid, get_earliest_last_processed_block,
    nuke_and_process_events_for_chain, Event,
};
use afterlife_backend::indexer::remote_calls::EventFetcher;
use dotenv::dotenv;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio;

#[tokio::main]
async fn main() {
    dotenv().ok();
    println!("Starting Afterlife Indexer, Insanity Edition");
    println!("SWED");

    loop {
        let start = Instant::now();

        let mut db_client = match database::connect().await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to connect to database: {}", e);
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        };

        let config = match IndexerConfig::from_env() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("Failed to load indexer config: {}", e);
                continue;
            }
        };

        let mut tasks = Vec::new();
        let mut blocks_for_chains = Vec::new();

        for chain in &config.chains {
            let earliest_last_processed_block =
                get_earliest_last_processed_block(chain, &db_client)
                    .await
                    .expect("Failed to get earliest last processed block");
            blocks_for_chains.push((chain.clone(), earliest_last_processed_block));
        }

        let mut all_events_by_contract: HashMap<i32, Vec<Event>> = HashMap::new();
        let mut all_blocks_by_chain: HashMap<String, (u64, u64)> = HashMap::new();

        for (chain, block) in blocks_for_chains {
            let task = tokio::task::spawn(async move {
                let event_fetcher = EventFetcher::new(&chain, block as usize);
                let result = event_fetcher
                    .execute()
                    .await
                    .expect("Failed to fetch events");
                (chain.clone(), result.0, result.1)
            });

            tasks.push(task);
        }

        // Await all tasks and collect results
        for task in tasks {
            let (chain, events, (from_block, to_block)) = task.await.unwrap();

            all_blocks_by_chain.insert(chain.name.clone(), (from_block as u64, to_block as u64));

            for event in events {
                let contract_id =
                    contract_and_chain_to_contractid(&event.contract, &chain, &db_client)
                        .await
                        .expect("Failed to get contract id");
                all_events_by_contract
                    .entry(contract_id)
                    .or_insert_with(Vec::new)
                    .push(event);
            }
        }

        // Process all events
        for (chain_name, (from_block, to_block)) in all_blocks_by_chain.iter() {
            let chain = config
                .chains
                .iter()
                .find(|c| &c.name == chain_name)
                .unwrap();
            nuke_and_process_events_for_chain(
                chain,
                &all_events_by_contract,
                *from_block,
                *to_block,
                &mut db_client,
            )
            .await
            .expect("Failed to nuke and process events");
        }

        let elapsed = start.elapsed();

        let mut total_contracts = 0;
        for chain in &config.chains {
            total_contracts += chain.contracts.len();
        }
        //println!("Indexed {} contracts on {} chains in {:?}", total_contracts, config.chains.len(), elapsed);
        if elapsed < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_secs(1) - elapsed).await;
        }
    }
}
