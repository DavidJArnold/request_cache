use request_cache::{cached_request, RequestCacheError};

/// Basic example demonstrating simple cached HTTP requests
#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    println!("=== Basic Request Cache Example ===\n");

    // Make a simple GET request with caching
    println!("Making first request to GitHub API...");
    let record1 = cached_request(
        "https://api.github.com/users/octocat".to_string(),
        "GET".to_string(),
        300,  // Cache for 5 minutes
        None, // Don't force refresh
        Some("request_cache_example/1.0".to_string()),
        None, // Use default database path
    )
    .await?;

    println!("✓ First request completed");
    println!("  - Response length: {} bytes", record1.response.len());
    println!("  - Was cached: {:?}", record1.cached);
    println!("  - URL: {}", record1.request);
    println!("  - Method: {}", record1.method);

    // Make the same request again - should come from cache
    println!("\nMaking second request (should be cached)...");
    let record2 = cached_request(
        "https://api.github.com/users/octocat".to_string(),
        "GET".to_string(),
        300,
        None,
        Some("request_cache_example/1.0".to_string()),
        None,
    )
    .await?;

    println!("✓ Second request completed");
    println!("  - Response length: {} bytes", record2.response.len());
    println!("  - Was cached: {:?}", record2.cached);

    // Force a fresh request
    println!("\nMaking third request (forced refresh)...");
    let record3 = cached_request(
        "https://api.github.com/users/octocat".to_string(),
        "GET".to_string(),
        300,
        Some(true), // Force refresh
        Some("request_cache_example/1.0".to_string()),
        None,
    )
    .await?;

    println!("✓ Third request completed");
    println!("  - Response length: {} bytes", record3.response.len());
    println!("  - Was cached: {:?}", record3.cached);

    println!("\n=== Example completed successfully! ===");

    Ok(())
}
