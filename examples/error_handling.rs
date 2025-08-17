use request_cache::{cached_request, HttpMethod, RequestBuilder, RequestCacheError};

/// Example demonstrating comprehensive error handling
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Error Handling Example ===\n");

    // 1. Invalid URL errors
    println!("1. Testing invalid URL handling:");

    let invalid_urls = vec![
        "not-a-url",
        "ftp://invalid-protocol.com",
        "://missing-scheme.com",
        "",
    ];

    for url in invalid_urls {
        match cached_request(url.to_string(), "GET".to_string(), 300, None, None, None).await {
            Ok(_) => println!("  ✗ Unexpected success with URL: '{}'", url),
            Err(RequestCacheError::InvalidUrl(e)) => {
                println!("  ✓ Caught invalid URL '{}': {}", url, e);
            }
            Err(e) => println!("  ? Unexpected error type for '{}': {}", url, e),
        }
    }

    // 2. Invalid HTTP method errors
    println!("\n2. Testing invalid HTTP method handling:");

    let invalid_methods = vec!["INVALID", "TRACE", "CONNECT", ""];

    for method in invalid_methods {
        match RequestBuilder::new()
            .url("https://httpbin.org/get")
            .unwrap()
            .method_str(method)
        {
            Ok(_) => println!("  ✗ Unexpected success with method: '{}'", method),
            Err(RequestCacheError::InvalidMethod(m)) => {
                println!(
                    "  ✓ Caught invalid method '{}': Invalid method '{}'",
                    method, m
                );
            }
            Err(e) => println!("  ? Unexpected error type for '{}': {}", method, e),
        }
    }

    // 3. Invalid timeout errors
    println!("\n3. Testing invalid timeout handling:");

    let invalid_timeouts = vec![-1, -100, 0];

    for timeout in invalid_timeouts {
        match RequestBuilder::new()
            .url("https://httpbin.org/get")
            .unwrap()
            .timeout(timeout)
        {
            Ok(_) => println!("  ✗ Unexpected success with timeout: {}", timeout),
            Err(RequestCacheError::InvalidTimeout) => {
                println!(
                    "  ✓ Caught invalid timeout {}: Timeout must be positive",
                    timeout
                );
            }
            Err(e) => println!("  ? Unexpected error type for timeout {}: {}", timeout, e),
        }
    }

    // 4. Network/HTTP errors (simulated)
    println!("\n4. Testing network error handling:");

    // These requests might fail due to network issues
    let problematic_urls = vec![
        "https://this-domain-definitely-does-not-exist-12345.com",
        "https://httpbin.org/status/404",
        "https://httpbin.org/status/500",
    ];

    for url in problematic_urls {
        match RequestBuilder::new()
            .url(url)?
            .timeout(5)? // Short timeout
            .send()
            .await
        {
            Ok(record) => {
                println!(
                    "  ✓ Request to '{}' succeeded: {} bytes",
                    url,
                    record.response.len()
                );
            }
            Err(RequestCacheError::Http(e)) => {
                println!("  ✓ Caught HTTP error for '{}': {}", url, e);
            }
            Err(e) => {
                println!("  ✓ Caught error for '{}': {}", url, e);
            }
        }
    }

    // 5. Database errors (simulated)
    println!("\n5. Testing database error handling:");

    // Try to use an invalid database path
    match cached_request(
        "https://httpbin.org/get".to_string(),
        "GET".to_string(),
        300,
        None,
        None,
        Some("/invalid/path/that/does/not/exist/cache.db".to_string()),
    )
    .await
    {
        Ok(_) => println!("  ✗ Unexpected success with invalid database path"),
        Err(RequestCacheError::Database(e)) => {
            println!("  ✓ Caught database error: {}", e);
        }
        Err(e) => println!("  ? Unexpected error type: {}", e),
    }

    // 6. Demonstration of proper error handling patterns
    println!("\n6. Proper error handling patterns:");

    fn handle_cache_error(error: RequestCacheError) -> String {
        match error {
            RequestCacheError::InvalidUrl(e) => {
                format!("Please check the URL format: {}", e)
            }
            RequestCacheError::InvalidMethod(method) => {
                format!(
                    "HTTP method '{}' is not supported. Use GET, POST, PUT, DELETE, HEAD, or PATCH",
                    method
                )
            }
            RequestCacheError::InvalidTimeout => {
                "Timeout must be a positive number of seconds".to_string()
            }
            RequestCacheError::Http(e) => {
                format!("Network request failed: {}", e)
            }
            RequestCacheError::Database(e) => {
                format!("Cache database error: {}", e)
            }
            RequestCacheError::InvalidDatabasePath(path) => {
                format!("Invalid database path: {}", path)
            }
        }
    }

    // Example of using the error handler
    match RequestBuilder::new()
        .url("invalid-url")
        .and_then(|builder| builder.timeout(-1))
        .and_then(|builder| builder.method_str("INVALID"))
    {
        Ok(_) => println!("  ✗ This should not succeed"),
        Err(e) => println!("  ✓ Handled error gracefully: {}", handle_cache_error(e)),
    }

    // 7. Successful request after error handling
    println!("\n7. Successful request after error examples:");

    match RequestBuilder::new()
        .url("https://httpbin.org/json")?
        .method(HttpMethod::GET)
        .timeout(300)?
        .user_agent("error_handling_example/1.0")
        .send()
        .await
    {
        Ok(record) => {
            println!("  ✓ Final request succeeded!");
            println!("    - Cached: {:?}", record.cached);
            println!("    - Response length: {} bytes", record.response.len());
        }
        Err(e) => println!("  ✗ Final request failed: {}", handle_cache_error(e)),
    }

    println!("\n=== Error handling example completed! ===");
    println!("💡 Key takeaways:");
    println!("   - Always handle errors appropriately for your use case");
    println!("   - Use pattern matching to handle specific error types");
    println!("   - Provide meaningful error messages to users");
    println!("   - Consider retry strategies for network errors");

    Ok(())
}
