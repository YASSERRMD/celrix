use celrix_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6380").await?;

    println!("Setting key...");
    client.set("hello", "world").await?;
    
    println!("Getting key...");
    let val = client.get("hello").await?;
    println!("Got: {:?}", val);

    println!("Deleting key...");
    let deleted = client.del("hello").await?;
    println!("Deleted: {}", deleted);

    Ok(())
}
