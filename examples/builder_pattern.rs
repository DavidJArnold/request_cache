use request_cache::{HttpMethod, RequestBuilder, RequestCacheError};

/// Example demonstrating the builder pattern API
#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    println!("=== Builder Pattern Example ===\n");

    // Simple GET request using builder pattern
    println!("1. Simple GET request with builder pattern:");
    let record = RequestBuilder::new()
        .url("https://httpbin.org/get")?
        .method(HttpMethod::GET)
        .timeout(300)?
        .user_agent("builder_example/1.0")
        .database_path("builder_cache.db")
        .send()
        .await?;

    println!("✓ Request completed");
    println!("  - Was cached: {:?}", record.cached);
    println!("  - Response length: {} bytes", record.response.len());

    // POST request with custom configuration
    println!("\n2. POST request with custom settings:");
    let post_record = RequestBuilder::new()
        .url("https://httpbin.org/post")?
        .method_str("POST")?
        .timeout(600)?
        .user_agent("MyApp/2.0")
        .database_path("builder_cache.db")
        .force_refresh(false)
        .send()
        .await?;

    println!("✓ POST request completed");
    println!("  - Method: {}", post_record.method);
    println!("  - Was cached: {:?}", post_record.cached);

    // Demonstrate different HTTP methods
    let methods = vec![
        HttpMethod::GET,
        HttpMethod::HEAD,
        HttpMethod::PUT,
        HttpMethod::DELETE,
        HttpMethod::PATCH,
    ];

    println!("\n3. Testing different HTTP methods:");
    for method in methods {
        let url = format!("https://httpbin.org/{}", method.to_string().to_lowercase());

        match RequestBuilder::new()
            .url(&url)?
            .method(method.clone())
            .timeout(120)?
            .send()
            .await
        {
            Ok(record) => {
                println!(
                    "✓ {} request to {} succeeded (cached: {:?})",
                    method, url, record.cached
                );
            }
            Err(e) => {
                println!("✗ {} request failed: {}", method, e);
            }
        }
    }

    // Demonstrate error handling
    println!("\n4. Error handling examples:");

    // Invalid URL
    match RequestBuilder::new().url("not-a-url") {
        Ok(_) => println!("Unexpected success with invalid URL"),
        Err(e) => println!("✓ Invalid URL caught: {}", e),
    }

    // Invalid timeout
    match RequestBuilder::new().timeout(-1) {
        Ok(_) => println!("Unexpected success with invalid timeout"),
        Err(e) => println!("✓ Invalid timeout caught: {}", e),
    }

    // Invalid method
    match RequestBuilder::new().method_str("INVALID") {
        Ok(_) => println!("Unexpected success with invalid method"),
        Err(e) => println!("✓ Invalid method caught: {}", e),
    }

    println!("\n=== Builder pattern example completed! ===");

    Ok(())
}
