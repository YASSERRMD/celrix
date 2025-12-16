use celrix_client::{Client, Result};
use hdrhistogram::Histogram;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use rand::SeedableRng;
use tokio::time::sleep;

const TOTAL_CLIENTS: usize = 50;
const HEAVY_KV_CLIENTS: usize = 20;
const VECTOR_CLIENTS: usize = 10;
const LIGHT_KV_CLIENTS: usize = 20; // Sum = 50

const DURATION_SECS: u64 = 30;
const KEY_PREFIX: &str = "stress_key";
const HEAVY_VALUE_SIZE: usize = 8192; // 8KB
const LIGHT_VALUE_SIZE: usize = 64;   // 64B
const VECTOR_DIM: usize = 1536;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting CELRIX Multi-Tier Isolation Stress Test");
    println!("Total Clients: {}", TOTAL_CLIENTS);
    println!("  - Heavy KV (8KB): {} clients", HEAVY_KV_CLIENTS);
    println!("  - Vector (1536d): {} clients", VECTOR_CLIENTS);
    println!("  - Light KV (64B): {} clients (Latency Probe)", LIGHT_KV_CLIENTS);
    println!("Duration: {} seconds", DURATION_SECS);
    println!("--------------------------------------------------");

    let start_time = Instant::now();
    let mut tasks = Vec::with_capacity(TOTAL_CLIENTS);

    // Shared statistics
    let heavy_store_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let heavy_restore_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    
    let light_store_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let light_restore_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

    let vadd_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let vsearch_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    
    let errors = Arc::new(Mutex::new(0u64));
    let ops_total = Arc::new(Mutex::new(0u64));

    for i in 0..TOTAL_CLIENTS {
        let heavy_store_hist = heavy_store_hist.clone();
        let heavy_restore_hist = heavy_restore_hist.clone();
        let light_store_hist = light_store_hist.clone();
        let light_restore_hist = light_restore_hist.clone();
        let vadd_hist = vadd_hist.clone();
        let vsearch_hist = vsearch_hist.clone();
        
        let errors = errors.clone();
        let ops = ops_total.clone();
        let client_id = i;
        
        // Determine Client Role
        // 0..100 = Heavy KV
        // 100..150 = Vector
        // 150..200 = Light KV
        let role = if i < HEAVY_KV_CLIENTS {
            "HEAVY"
        } else if i < HEAVY_KV_CLIENTS + VECTOR_CLIENTS {
            "VECTOR"
        } else {
            "LIGHT"
        };

        tasks.push(tokio::spawn(async move {
            let mut client = match Client::connect("127.0.0.1:6380").await {
                Ok(c) => c,
                Err(e) => {
                    println!("Client {} failed to connect: {}", client_id, e);
                    return;
                }
            };

            let mut rng = rand::rngs::StdRng::seed_from_u64(client_id as u64);
            let chars = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .?,!";
            
            // Pre-allocate buffers based on role
            let mut value_buf = if role == "LIGHT" {
                vec![0u8; LIGHT_VALUE_SIZE]
            } else {
                vec![0u8; HEAVY_VALUE_SIZE]
            };
            let vector = vec![0.1f32; VECTOR_DIM]; // Only needed for VECTOR role

            let end_time = Instant::now() + Duration::from_secs(DURATION_SECS);

            while Instant::now() < end_time {
                match role {
                    "VECTOR" => {
                        let key = format!("vec_{}_{}", client_id, rng.gen::<u32>());
                        
                        let t0 = Instant::now();
                        if let Err(_) = client.vadd(&key, &vector).await {
                             let mut e = errors.lock().unwrap(); *e += 1; continue;
                        }
                        let t1 = Instant::now();
                        { let mut h = vadd_hist.lock().unwrap(); h.record(t1.duration_since(t0).as_micros() as u64).ok(); }

                        let t2 = Instant::now();
                        if let Err(_) = client.vsearch(&vector, 5).await {
                             let mut e = errors.lock().unwrap(); *e += 1;
                        } else {
                             let t3 = Instant::now();
                             { let mut h = vsearch_hist.lock().unwrap(); h.record(t3.duration_since(t2).as_micros() as u64).ok(); }
                        }
                    },
                    "HEAVY" => {
                        for b in &mut value_buf { *b = chars[rng.gen_range(0..chars.len())]; }
                        let value_str = unsafe { std::str::from_utf8_unchecked(&value_buf) };
                        let key = format!("heavy_{}_{}", client_id, rng.gen::<u32>());

                        let t0 = Instant::now();
                        if let Err(_) = client.set(&key, value_str, None).await {
                             let mut e = errors.lock().unwrap(); *e += 1; continue;
                        }
                        let t1 = Instant::now();
                        { let mut h = heavy_store_hist.lock().unwrap(); h.record(t1.duration_since(t0).as_micros() as u64).ok(); }

                        let t2 = Instant::now();
                        if let Err(_) = client.get(&key).await {
                             let mut e = errors.lock().unwrap(); *e += 1;
                        } else {
                             let t3 = Instant::now();
                             { let mut h = heavy_restore_hist.lock().unwrap(); h.record(t3.duration_since(t2).as_micros() as u64).ok(); }
                        }
                    },
                    "LIGHT" => {
                        for b in &mut value_buf { *b = chars[rng.gen_range(0..chars.len())]; }
                        let value_str = unsafe { std::str::from_utf8_unchecked(&value_buf) };
                        let key = format!("light_{}_{}", client_id, rng.gen::<u32>());

                        let t0 = Instant::now();
                        if let Err(_) = client.set(&key, value_str, None).await {
                             let mut e = errors.lock().unwrap(); *e += 1; continue;
                        }
                        let t1 = Instant::now();
                        { let mut h = light_store_hist.lock().unwrap(); h.record(t1.duration_since(t0).as_micros() as u64).ok(); }

                        let t2 = Instant::now();
                        if let Err(_) = client.get(&key).await {
                             let mut e = errors.lock().unwrap(); *e += 1;
                        } else {
                             let t3 = Instant::now();
                             { let mut h = light_restore_hist.lock().unwrap(); h.record(t3.duration_since(t2).as_micros() as u64).ok(); }
                        }
                    },
                    _ => {}
                }
                let mut o = ops.lock().unwrap(); *o += 1;
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_ops = *ops_total.lock().unwrap();
    let error_count = *errors.lock().unwrap();
    
    println!("--------------------------------------------------");
    println!("Test Completed in {:.2?}", elapsed);
    println!("Total Operations: {}", total_ops);
    println!("Total Errors: {}", error_count);
    println!("Throughput: {:.2} ops/sec", total_ops as f64 / elapsed.as_secs_f64());
    println!("\nLatency Report (microseconds):");
    
    let print_hist = |label: &str, h: &Histogram<u64>| {
        println!("{}:", label);
        println!("  Min:    {:>5} us", h.min());
        println!("  P50:    {:>5} us", h.value_at_quantile(0.5));
        println!("  P95:    {:>5} us", h.value_at_quantile(0.95));
        println!("  P99:    {:>5} us", h.value_at_quantile(0.99));
        println!("  Max:    {:>5} us", h.max());
    };

    let light_store_h = light_store_hist.lock().unwrap();
    print_hist("LIGHT KV: SET (64B)", &light_store_h);
    let light_restore_h = light_restore_hist.lock().unwrap();
    print_hist("LIGHT KV: GET (64B)", &light_restore_h);

    let heavy_store_h = heavy_store_hist.lock().unwrap();
    print_hist("HEAVY KV: SET (8KB)", &heavy_store_h);
    let heavy_restore_h = heavy_restore_hist.lock().unwrap();
    print_hist("HEAVY KV: GET (8KB)", &heavy_restore_h);
    
    let vadd_h = vadd_hist.lock().unwrap();
    print_hist("VECTOR: INDEX", &vadd_h);
    let vsearch_h = vsearch_hist.lock().unwrap();
    print_hist("VECTOR: SEARCH", &vsearch_h);

    Ok(())
}
