use request_cache::{create_connection, HttpMethod, RequestBuilder, RequestCacheError};
use std::time::Instant;

/// Example demonstrating connection reuse for better performance
#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    println!("=== Connection Reuse Example ===\n");

    // Create a shared database connection
    println!("Creating shared database connection...");
    let connection = create_connection("shared_cache.db".to_string()).await?;
    println!("✓ Database connection established\n");

    // List of URLs to fetch
    let urls = [
        "https://httpbin.org/json",
        "https://httpbin.org/uuid",
        "https://httpbin.org/base64/aGVsbG8gd29ybGQ%3D",
        "https://httpbin.org/user-agent",
        "https://httpbin.org/headers",
    ];

    // Demonstrate performance with connection reuse
    println!("1. Making multiple requests with shared connection:");
    let start_time = Instant::now();

    for (i, url) in urls.iter().enumerate() {
        println!("  Request {}: {}", i + 1, url);

        let record = RequestBuilder::new()
            .url(url)?
            .method(HttpMethod::GET)
            .timeout(300)?
            .user_agent("connection_reuse_example/1.0")
            .send_with_connection(&connection)
            .await?;

        println!(
            "    ✓ Completed (cached: {:?}, {} bytes)",
            record.cached,
            record.response.len()
        );
    }

    let elapsed = start_time.elapsed();
    println!("  Total time for {} requests: {:?}\n", urls.len(), elapsed);

    // Make the same requests again to demonstrate caching
    println!("2. Making the same requests again (should be faster due to caching):");
    let start_time = Instant::now();

    for (i, url) in urls.iter().enumerate() {
        println!("  Request {}: {}", i + 1, url);

        let record = RequestBuilder::new()
            .url(url)?
            .method(HttpMethod::GET)
            .timeout(300)?
            .user_agent("connection_reuse_example/1.0")
            .send_with_connection(&connection)
            .await?;

        println!(
            "    ✓ Completed (cached: {:?}, {} bytes)",
            record.cached,
            record.response.len()
        );
    }

    let cached_elapsed = start_time.elapsed();
    println!(
        "  Total time for {} cached requests: {:?}",
        urls.len(),
        cached_elapsed
    );

    if cached_elapsed < elapsed {
        println!(
            "  🚀 Cached requests were {:?} faster!",
            elapsed - cached_elapsed
        );
    }

    // Demonstrate different cache timeouts
    println!("\n3. Testing different cache timeouts:");

    // Short cache (10 seconds)
    let short_cache = RequestBuilder::new()
        .url("https://httpbin.org/delay/1")?
        .timeout(10)?
        .send_with_connection(&connection)
        .await?;
    println!("  ✓ Short cache (10s): cached = {:?}", short_cache.cached);

    // Long cache (1 hour)
    let long_cache = RequestBuilder::new()
        .url("https://httpbin.org/ip")?
        .timeout(3600)?
        .send_with_connection(&connection)
        .await?;
    println!("  ✓ Long cache (1h): cached = {:?}", long_cache.cached);

    // Force refresh example
    println!("\n4. Force refresh example:");
    let fresh_request = RequestBuilder::new()
        .url("https://httpbin.org/timestamp")?
        .force_refresh(true)
        .send_with_connection(&connection)
        .await?;
    println!(
        "  ✓ Forced fresh request: cached = {:?}",
        fresh_request.cached
    );

    println!("\n=== Connection reuse example completed! ===");
    println!("💡 Tip: Reusing connections is especially beneficial for applications");
    println!("   making many requests, as it avoids database connection overhead.");

    Ok(())
}
