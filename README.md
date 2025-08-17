# request_cache

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance, async HTTP request caching library for Rust that provides intelligent caching with SQLite persistence, comprehensive error handling, and a modern builder API.

## Features

- 🚀 **High Performance**: Optimized SQLite operations with proper indexing and HTTP client reuse
- 🛡️ **Production Ready**: Comprehensive error handling, input validation, and type safety
- ⚡ **Async First**: Built on `tokio` and `async-sqlite` for non-blocking operations
- 🎯 **Flexible API**: Both traditional function calls and modern builder pattern
- 📦 **SQLite Persistence**: Automatic cache management with configurable expiration
- 🔧 **HTTP Method Support**: GET, POST, PUT, DELETE, HEAD, and PATCH requests
- ⏱️ **Smart Timeouts**: Configurable request timeouts with safety limits
- 🧪 **Well Tested**: Comprehensive test suite with error scenario coverage

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
request_cache = "0.1.0"
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
```

### Basic Usage

```rust
use request_cache::{cached_request, RequestCacheError};

#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    // Make a cached HTTP request
    let record = cached_request(
        "https://api.github.com/users/octocat".to_string(),
        "GET".to_string(),
        300, // Cache for 5 minutes
        None, // Don't force refresh
        Some("MyApp/1.0".to_string()),
        None, // Use default database path
    ).await?;

    println!("Response: {}", record.response);
    println!("Was cached: {:?}", record.cached);
    
    Ok(())
}
```

### Builder Pattern (Recommended)

```rust
use request_cache::{RequestBuilder, HttpMethod, RequestCacheError};

#[tokio::main]
async fn main() -> Result<(), RequestCacheError> {
    let record = RequestBuilder::new()
        .url("https://api.github.com/users/octocat")?
        .method(HttpMethod::GET)
        .timeout(300)?
        .user_agent("MyApp/1.0")
        .database_path("api_cache.db")
        .force_refresh(false)
        .send()
        .await?;

    println!("User data: {}", record.response);
    
    Ok(())
}
```

## API Reference

### RequestBuilder

The recommended way to create cached requests with a fluent, chainable API:

```rust
let record = RequestBuilder::new()
    .url("https://api.example.com/data")?     // Set the request URL
    .method(HttpMethod::POST)                  // HTTP method (GET, POST, etc.)
    .timeout(600)?                            // Cache timeout in seconds
    .user_agent("MyApp/1.0")                  // Custom User-Agent header
    .database_path("cache.db")                // SQLite database file path
    .force_refresh(true)                      // Bypass cache if true
    .send()                                   // Execute the request
    .await?;
```

### HTTP Methods

Supported HTTP methods via the `HttpMethod` enum:

```rust
use request_cache::HttpMethod;

HttpMethod::GET     // Most common for APIs
HttpMethod::POST    // For creating resources
HttpMethod::PUT     // For updating resources
HttpMethod::DELETE  // For removing resources
HttpMethod::HEAD    // For metadata only
HttpMethod::PATCH   // For partial updates
```

### Error Handling

Comprehensive error types for robust applications:

```rust
use request_cache::RequestCacheError;

match cached_request(/* ... */).await {
    Ok(record) => println!("Success: {}", record.response),
    Err(RequestCacheError::InvalidUrl(e)) => eprintln!("Bad URL: {}", e),
    Err(RequestCacheError::Http(e)) => eprintln!("HTTP error: {}", e),
    Err(RequestCacheError::Database(e)) => eprintln!("Database error: {}", e),
    Err(RequestCacheError::InvalidTimeout) => eprintln!("Timeout must be positive"),
    Err(RequestCacheError::InvalidMethod(m)) => eprintln!("Unsupported method: {}", m),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Advanced Usage

### Connection Management

For high-performance applications, reuse database connections:

```rust
use request_cache::{create_connection, RequestBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a shared connection
    let connection = create_connection("shared_cache.db".to_string()).await?;
    
    // Make multiple requests with the same connection
    for url in &["https://api.example.com/users", "https://api.example.com/posts"] {
        let record = RequestBuilder::new()
            .url(url)?
            .method(HttpMethod::GET)
            .timeout(300)?
            .send_with_connection(&connection)
            .await?;
            
        println!("Cached: {:?}, Response length: {}", record.cached, record.response.len());
    }
    
    Ok(())
}
```

### Custom Cache Expiration

```rust
// Short-lived cache (30 seconds) for frequently changing data
let recent_data = RequestBuilder::new()
    .url("https://api.example.com/live-data")?
    .timeout(30)?
    .send()
    .await?;

// Long-lived cache (1 hour) for stable data
let stable_data = RequestBuilder::new()
    .url("https://api.example.com/config")?
    .timeout(3600)?
    .send()
    .await?;
```

### Force Cache Refresh

```rust
// Force a fresh request, bypassing cache
let fresh_data = RequestBuilder::new()
    .url("https://api.example.com/latest")?
    .force_refresh(true)
    .send()
    .await?;
```

## Performance Tips

1. **Reuse Connections**: Use `create_connection()` once and share the connection across requests
2. **Appropriate Timeouts**: Balance between data freshness and performance
3. **Database Location**: Place cache database on fast storage (SSD)
4. **Batch Operations**: Make multiple requests with a shared connection

## Cache Behavior

- **Cache Key**: Combination of URL and HTTP method
- **Expiration**: Based on Unix timestamps with configurable timeout
- **Replacement**: Old entries are automatically replaced when new requests are made
- **Performance**: Indexed database queries for fast cache lookups
- **Persistence**: Cache survives application restarts

## Error Recovery

The library is designed for resilience:

- **No Panics**: All errors are returned as `Result` types
- **Input Validation**: URLs, timeouts, and methods are validated before use
- **Network Timeouts**: Requests timeout automatically to prevent hanging
- **Database Recovery**: SQLite handles concurrent access and corruption recovery

## Examples

See the `examples/` directory for complete working examples:

- `basic_usage.rs` - Simple GET request caching
- `builder_pattern.rs` - Using the builder API
- `error_handling.rs` - Comprehensive error handling
- `connection_reuse.rs` - High-performance connection management
- `different_methods.rs` - POST, PUT, DELETE examples

## Testing

Run the test suite:

```bash
cargo test
```

Run with output to see request details:

```bash
cargo test -- --nocapture
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### v0.1.0
- Initial release with SQLite caching
- Builder pattern API
- Comprehensive error handling
- Performance optimizations
- Full HTTP method support