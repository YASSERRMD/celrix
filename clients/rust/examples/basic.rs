use celrix_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Retry connection a few times if server is not ready
    let mut client = None;
    for i in 0..5 {
        match Client::connect("127.0.0.1:6380").await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(e) => {
                println!("Connection failed (attempt {}): {}", i, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
    let mut client = client.expect("Failed to connect to server");

    println!("Ping...");
    client.ping().await?;
    println!("Pong!");

    println!("Setting key...");
    client.set("hello_rust", "world_rust", None).await?;
    
    println!("Getting key...");
    let val = client.get("hello_rust").await?;
    println!("Got: {:?}", val);
    assert_eq!(val.as_deref(), Some("world_rust"));

    println!("Testing Vector operations...");
    // Server default dimension is 1536
    let vector = vec![0.1f32; 1536]; 
    client.vadd("v_rust", &vector).await?;
    println!("VAdd success");

    let results = client.vsearch(&vector, 5).await?;
    println!("VSearch results: {:?}", results);
    assert!(!results.is_empty());
    assert!(results.contains(&"v_rust".to_string()));

    println!("Deleting key...");
    let deleted = client.del("hello_rust").await?;
    println!("Deleted: {}", deleted);

    println!("All tests passed!");
    Ok(())
}
