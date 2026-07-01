use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    animus_queue_postgres::plugin::run().await
}
