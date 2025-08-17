use request_cache::{HttpMethod, RequestBuilder, RequestCacheError};

/// Example demonstrating different HTTP methods with caching
#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    println!("=== Different HTTP Methods Example ===\n");

    // Note: This example uses httpbin.org which provides endpoints for testing
    // different HTTP methods. In real applications, you'd use your actual APIs.

    // 1. GET request (most common)
    println!("1. GET request (retrieving data):");
    let get_record = RequestBuilder::new()
        .url("https://httpbin.org/get")?
        .method(HttpMethod::GET)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ GET request completed");
    println!("    - Method: {}", get_record.method);
    println!("    - Cached: {:?}", get_record.cached);
    println!("    - Response length: {} bytes", get_record.response.len());

    // 2. POST request (creating data)
    println!("\n2. POST request (sending data):");
    let post_record = RequestBuilder::new()
        .url("https://httpbin.org/post")?
        .method(HttpMethod::POST)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ POST request completed");
    println!("    - Method: {}", post_record.method);
    println!("    - Cached: {:?}", post_record.cached);

    // 3. PUT request (updating data)
    println!("\n3. PUT request (updating data):");
    let put_record = RequestBuilder::new()
        .url("https://httpbin.org/put")?
        .method(HttpMethod::PUT)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ PUT request completed");
    println!("    - Method: {}", put_record.method);
    println!("    - Cached: {:?}", put_record.cached);

    // 4. PATCH request (partial updates)
    println!("\n4. PATCH request (partial update):");
    let patch_record = RequestBuilder::new()
        .url("https://httpbin.org/patch")?
        .method(HttpMethod::PATCH)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ PATCH request completed");
    println!("    - Method: {}", patch_record.method);
    println!("    - Cached: {:?}", patch_record.cached);

    // 5. DELETE request (removing data)
    println!("\n5. DELETE request (removing data):");
    let delete_record = RequestBuilder::new()
        .url("https://httpbin.org/delete")?
        .method(HttpMethod::DELETE)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ DELETE request completed");
    println!("    - Method: {}", delete_record.method);
    println!("    - Cached: {:?}", delete_record.cached);

    // 6. HEAD request (metadata only)
    println!("\n6. HEAD request (metadata only):");
    let head_record = RequestBuilder::new()
        .url("https://httpbin.org/headers")?
        .method(HttpMethod::HEAD)
        .timeout(300)?
        .user_agent("methods_example/1.0")
        .database_path("methods_cache.db")
        .send()
        .await?;

    println!("  ✓ HEAD request completed");
    println!("    - Method: {}", head_record.method);
    println!("    - Cached: {:?}", head_record.cached);
    println!(
        "    - Response length: {} bytes (should be small for HEAD)",
        head_record.response.len()
    );

    // 7. Demonstrate caching behavior with same URL but different methods
    println!("\n7. Caching behavior with same URL, different methods:");

    let base_url = "https://httpbin.org/anything/test";
    let methods = vec![HttpMethod::GET, HttpMethod::POST, HttpMethod::PUT];

    for method in methods {
        println!("  Making {} request to {}...", method, base_url);

        // First request (should not be cached)
        let record1 = RequestBuilder::new()
            .url(base_url)?
            .method(method.clone())
            .timeout(300)?
            .database_path("methods_cache.db")
            .send()
            .await?;
        println!(
            "    ✓ First {} request: cached = {:?}",
            method, record1.cached
        );

        // Second request (should be cached)
        let record2 = RequestBuilder::new()
            .url(base_url)?
            .method(method.clone())
            .timeout(300)?
            .database_path("methods_cache.db")
            .send()
            .await?;
        println!(
            "    ✓ Second {} request: cached = {:?}",
            method, record2.cached
        );
    }

    // 8. Method string conversion
    println!("\n8. Using method strings instead of enums:");

    let method_strings = vec!["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"];

    for method_str in method_strings {
        match RequestBuilder::new()
            .url("https://httpbin.org/anything")?
            .method_str(method_str)?
            .timeout(60)?
            .database_path("methods_cache.db")
            .send()
            .await
        {
            Ok(record) => {
                println!("  ✓ {} request from string succeeded", method_str);
                println!("    - Parsed method: {}", record.method);
            }
            Err(e) => {
                println!("  ✗ {} request failed: {}", method_str, e);
            }
        }
    }

    println!("\n=== HTTP Methods example completed! ===");
    println!("💡 Key insights:");
    println!("   - Each HTTP method + URL combination is cached separately");
    println!("   - Use GET for retrieving data, POST for creating, PUT for updating");
    println!("   - HEAD requests typically return minimal response bodies");
    println!("   - The library supports all common HTTP methods");
    println!("   - Methods can be specified as enums or strings");

    Ok(())
}
