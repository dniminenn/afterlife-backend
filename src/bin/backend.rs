/*
    This is the main entry point for the backend server.
    It will start the server and connect to the database.

    This provides the API for the frontend to interact with.
*/

use afterlife_backend::backend::api;
use afterlife_backend::common::database;
use dotenv::dotenv;


#[tokio::main]
async fn main() {
    dotenv().ok();
    // Print env vars
    //for (key, value) in std::env::vars() {
    //    println!("{}: {}", key, value);
    //}
    let db_client = database::connect().await.expect("Failed to connect to database");
    api::run_server(db_client).await;
}
