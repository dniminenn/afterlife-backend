use tokio_postgres::{Client, Config, NoTls};
use std::env;

pub async fn connect() -> Result<Client, Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.user(&*env::var("AFTERLIFE_DATABASE_USER")?);
    config.host(&*env::var("AFTERLIFE_DATABASE_HOST")?);
    config.port(env::var("AFTERLIFE_DATABASE_PORT")?.parse::<u16>()?);
    config.dbname(&*env::var("AFTERLIFE_DATABASE_DBNAME")?);

    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

