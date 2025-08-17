//! # request_cache
//!
//! A high-performance, async HTTP request caching library for Rust that provides intelligent
//! caching with SQLite persistence, comprehensive error handling, and a modern builder API.
//!
//! ## Features
//!
//! - 🚀 **High Performance**: Optimized SQLite operations with proper indexing and HTTP client reuse
//! - 🛡️ **Production Ready**: Comprehensive error handling, input validation, and type safety  
//! - ⚡ **Async First**: Built on `tokio` and `async-sqlite` for non-blocking operations
//! - 🎯 **Flexible API**: Both traditional function calls and modern builder pattern
//! - 📦 **SQLite Persistence**: Automatic cache management with configurable expiration
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use request_cache::{RequestBuilder, HttpMethod, RequestCacheError};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), RequestCacheError> {
//!     let record = RequestBuilder::new()
//!         .url("https://api.github.com/users/octocat")?
//!         .method(HttpMethod::GET)
//!         .timeout(300)?
//!         .user_agent("MyApp/1.0")
//!         .send()
//!         .await?;
//!
//!     println!("Response: {}", record.response);
//!     println!("Was cached: {:?}", record.cached);
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## API Overview
//!
//! The library provides two main ways to make cached requests:
//!
//! 1. **Builder Pattern (Recommended)**: Use [`RequestBuilder`] for a fluent, chainable API
//! 2. **Function Calls**: Use [`cached_request`] for simple, direct calls
//!
//! Both approaches return a [`Record`] containing the HTTP response and cache metadata.

use async_sqlite::{rusqlite::params, Client, ClientBuilder};
use reqwest::header::{HeaderMap, USER_AGENT};
use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

/// Comprehensive error type covering all possible request cache failures.
///
/// This error type provides detailed information about what went wrong during
/// cache operations, making it easier to handle different failure scenarios
/// appropriately in your application.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::{cached_request, RequestCacheError};
///
/// #[tokio::main]
/// async fn main() {
///     match cached_request("invalid-url".to_string(), "GET".to_string(), 300, None, None, None).await {
///         Ok(record) => println!("Success: {}", record.response),
///         Err(RequestCacheError::InvalidUrl(e)) => eprintln!("Bad URL: {}", e),
///         Err(RequestCacheError::Http(e)) => eprintln!("HTTP error: {}", e),
///         Err(RequestCacheError::Database(e)) => eprintln!("Database error: {}", e),
///         Err(e) => eprintln!("Other error: {}", e),
///     }
/// }
/// ```
#[derive(Debug)]
pub enum RequestCacheError {
    /// Database operation failed
    Database(async_sqlite::Error),
    /// HTTP request failed
    Http(reqwest::Error),
    /// Invalid URL provided
    InvalidUrl(url::ParseError),
    /// Invalid timeout value
    InvalidTimeout,
    /// Invalid HTTP method
    InvalidMethod(String),
    /// Database path is invalid
    InvalidDatabasePath(String),
}

impl fmt::Display for RequestCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestCacheError::Database(e) => write!(f, "Database error: {}", e),
            RequestCacheError::Http(e) => write!(f, "HTTP error: {}", e),
            RequestCacheError::InvalidUrl(e) => write!(f, "Invalid URL: {}", e),
            RequestCacheError::InvalidTimeout => write!(f, "Timeout must be positive"),
            RequestCacheError::InvalidMethod(m) => write!(f, "Invalid HTTP method: {}", m),
            RequestCacheError::InvalidDatabasePath(p) => write!(f, "Invalid database path: {}", p),
        }
    }
}

impl std::error::Error for RequestCacheError {}

impl From<async_sqlite::Error> for RequestCacheError {
    fn from(err: async_sqlite::Error) -> Self {
        RequestCacheError::Database(err)
    }
}

impl From<reqwest::Error> for RequestCacheError {
    fn from(err: reqwest::Error) -> Self {
        RequestCacheError::Http(err)
    }
}

impl From<url::ParseError> for RequestCacheError {
    fn from(err: url::ParseError) -> Self {
        RequestCacheError::InvalidUrl(err)
    }
}

/// Supported HTTP methods for cached requests.
///
/// This enum provides type-safe representation of HTTP methods, preventing
/// invalid method strings and enabling better compile-time checking.
///
/// # Examples
///
/// ```rust
/// use request_cache::HttpMethod;
///
/// let method = HttpMethod::GET;
/// println!("Method: {}", method); // Prints: "Method: GET"
///
/// let method_from_string = "POST".parse::<HttpMethod>().unwrap();
/// assert_eq!(method_from_string, HttpMethod::POST);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,
    PATCH,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpMethod::GET => write!(f, "GET"),
            HttpMethod::POST => write!(f, "POST"),
            HttpMethod::PUT => write!(f, "PUT"),
            HttpMethod::DELETE => write!(f, "DELETE"),
            HttpMethod::HEAD => write!(f, "HEAD"),
            HttpMethod::PATCH => write!(f, "PATCH"),
        }
    }
}

impl FromStr for HttpMethod {
    type Err = RequestCacheError;

    fn from_str(method: &str) -> Result<Self, Self::Err> {
        match method.to_uppercase().as_str() {
            "GET" => Ok(HttpMethod::GET),
            "POST" => Ok(HttpMethod::POST),
            "PUT" => Ok(HttpMethod::PUT),
            "DELETE" => Ok(HttpMethod::DELETE),
            "HEAD" => Ok(HttpMethod::HEAD),
            "PATCH" => Ok(HttpMethod::PATCH),
            _ => Err(RequestCacheError::InvalidMethod(method.to_string())),
        }
    }
}

/// Represents a cached HTTP response with metadata.
///
/// This structure contains the complete information about an HTTP request/response
/// cycle, including whether the response came from cache or was freshly fetched.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::RequestBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let record = RequestBuilder::new()
///     .url("https://api.example.com/data")?
///     .send()
///     .await?;
///
/// if record.cached == Some(true) {
///     println!("Response served from cache");
/// } else {
///     println!("Fresh response from server");
/// }
///
/// println!("URL: {}", record.request);
/// println!("Method: {}", record.method);
/// println!("Response: {}", record.response);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Record {
    /// The URL that was requested
    pub request: Url,
    /// The HTTP method used for the request
    pub method: HttpMethod,
    /// The HTTP response body as a string
    pub response: String,
    /// Unix timestamp when this cache entry expires
    pub expires: i64,
    /// Whether this response came from cache (Some(true)), was freshly fetched (Some(false)), or unknown (None)
    pub cached: Option<bool>,
}

/// Get an HTTP response using cache if available, or fetch fresh if not cached or expired.
///
/// This is the main entry point for simple cached HTTP requests. It manages its own
/// database connection and handles all caching logic automatically.
///
/// # Arguments
///
/// * `url` - The URL to request (must be a valid HTTP/HTTPS URL)
/// * `method` - HTTP method as a string ("GET", "POST", etc.)
/// * `timeout` - Cache expiration time in seconds (must be positive)
/// * `force_refresh` - If Some(true), bypasses cache and fetches fresh data
/// * `user_agent` - Optional custom User-Agent header
/// * `db_path` - Optional custom database file path (defaults to "request_cache_db")
///
/// # Returns
///
/// Returns a [`Result`] containing a [`Record`] with the response data and cache metadata,
/// or a [`RequestCacheError`] if something went wrong.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::cached_request;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Simple GET request with 5-minute cache
/// let record = cached_request(
///     "https://api.github.com/users/octocat".to_string(),
///     "GET".to_string(),
///     300, // 5 minutes
///     None, // Use cache if available
///     Some("MyApp/1.0".to_string()),
///     None, // Use default database
/// ).await?;
///
/// println!("Response: {}", record.response);
/// println!("From cache: {:?}", record.cached);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The URL is invalid
/// - The HTTP method is not supported
/// - The timeout is negative or zero
/// - Database operations fail
/// - Network requests fail
pub async fn cached_request(
    url: String,
    method: String,
    timeout: i64,
    force_refresh: Option<bool>,
    user_agent: Option<String>,
    db_path: Option<String>,
) -> Result<Record, RequestCacheError> {
    // Validate inputs
    let parsed_url = Url::parse(&url)?;
    let http_method = HttpMethod::from_str(&method)?;

    if timeout <= 0 {
        return Err(RequestCacheError::InvalidTimeout);
    }

    let db_path = db_path.unwrap_or_else(|| String::from("request_cache_db"));
    let connection = create_connection(db_path).await?;

    request(
        &connection,
        parsed_url,
        http_method,
        timeout,
        force_refresh,
        user_agent,
    )
    .await
}

/// Create and initialize a SQLite database connection for caching.
///
/// This function creates a new SQLite database file (if it doesn't exist) and sets up
/// the necessary tables and indexes for optimal cache performance. The database will
/// be created at the specified path.
///
/// # Arguments
///
/// * `path` - File system path where the SQLite database should be created/opened
///
/// # Returns
///
/// Returns a [`Result`] containing an async SQLite [`Client`] ready for cache operations,
/// or a [`RequestCacheError`] if database initialization fails.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::{create_connection, RequestBuilder};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a shared database connection
/// let connection = create_connection("my_cache.db".to_string()).await?;
///
/// // Use the connection for multiple requests
/// let record1 = RequestBuilder::new()
///     .url("https://api.example.com/users")?
///     .send_with_connection(&connection)
///     .await?;
///
/// let record2 = RequestBuilder::new()
///     .url("https://api.example.com/posts")?
///     .send_with_connection(&connection)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Database Schema
///
/// The function creates a table with the following structure:
/// ```sql
/// CREATE TABLE IF NOT EXISTS requests (
///     request TEXT NOT NULL,    -- The request URL
///     method TEXT NOT NULL,     -- HTTP method
///     response TEXT NOT NULL,   -- Response body
///     expires INTEGER NOT NULL  -- Expiration timestamp
/// );
/// CREATE INDEX IF NOT EXISTS idx_request_method_expires
/// ON requests(request, method, expires);
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The database path is invalid (empty or contains null bytes)
/// - Database file cannot be created or opened
/// - Table creation fails
pub async fn create_connection(path: String) -> Result<Client, RequestCacheError> {
    // Validate database path
    if path.is_empty() || path.contains("\0") {
        return Err(RequestCacheError::InvalidDatabasePath(path));
    }

    let client = ClientBuilder::new().path(path).open().await?;

    // Create table with proper indexes for performance
    client
        .conn(move |conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS requests (
                request TEXT NOT NULL,
                method TEXT NOT NULL,
                response TEXT NOT NULL,
                expires INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_request_method_expires 
            ON requests(request, method, expires);",
            )
        })
        .await?;

    Ok(client)
}

/// Make a cached HTTP request using an existing database connection.
///
/// This function is useful when you want to reuse a database connection across
/// multiple requests for better performance. It provides the same caching logic
/// as [`cached_request`] but with an explicit connection.
///
/// # Arguments
///
/// * `connection` - An existing database connection from [`create_connection`]
/// * `url` - The URL to request
/// * `method` - HTTP method to use
/// * `timeout` - Cache expiration time in seconds
/// * `force_refresh` - If Some(true), bypasses cache
/// * `user_agent` - Optional custom User-Agent header
///
/// # Returns
///
/// Returns a [`Result`] containing a [`Record`] with the response and cache metadata.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::{create_connection, request, HttpMethod};
/// use url::Url;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let connection = create_connection("cache.db".to_string()).await?;
/// let url = Url::parse("https://api.example.com/data")?;
///
/// let record = request(
///     &connection,
///     url,
///     HttpMethod::GET,
///     300,
///     Some(false), // Use cache if available
///     Some("MyApp/1.0".to_string()),
/// ).await?;
///
/// println!("Got response: {}", record.response);
/// # Ok(())
/// # }
/// ```
pub async fn request(
    connection: &Client,
    url: Url,
    method: HttpMethod,
    timeout: i64,
    force_refresh: Option<bool>,
    user_agent: Option<String>,
) -> Result<Record, RequestCacheError> {
    if timeout <= 0 {
        return Err(RequestCacheError::InvalidTimeout);
    }

    if force_refresh.unwrap_or(false) {
        return make_request(connection, &url, &method, timeout, user_agent).await;
    }

    // make a request, using cached response if one exists
    match get_record(connection, url.clone(), method.clone()).await? {
        Some(record) => Ok(record),
        None => make_request(connection, &url, &method, timeout, user_agent).await,
    }
}

async fn get_record(
    connection: &Client,
    url: Url,
    method: HttpMethod,
) -> Result<Option<Record>, RequestCacheError> {
    // try to get a record from the DB
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            RequestCacheError::Database(async_sqlite::Error::Rusqlite(
                async_sqlite::rusqlite::Error::SqliteFailure(
                    async_sqlite::rusqlite::ffi::Error::new(
                        async_sqlite::rusqlite::ffi::SQLITE_MISUSE,
                    ),
                    Some("System time error".to_string()),
                ),
            ))
        })?
        .as_secs() as i64;

    let url_str = url.to_string();
    let method_str = method.to_string();
    let query = "SELECT request, method, response, expires FROM requests WHERE request = ?1 AND method = ?2 AND expires > ?3 ORDER BY expires DESC LIMIT 1;";

    let result = connection
        .conn(move |conn| {
            conn.query_row(query, params![url_str, method_str, current_time], |row| {
                let request_str: String = row.get(0)?;
                let method_str: String = row.get(1)?;
                let response: String = row.get(2)?;
                let expires: i64 = row.get(3)?;

                Ok(Record {
                    request: Url::parse(&request_str).map_err(|_| {
                        async_sqlite::rusqlite::Error::InvalidColumnType(
                            0,
                            "request".to_string(),
                            async_sqlite::rusqlite::types::Type::Text,
                        )
                    })?,
                    method: HttpMethod::from_str(&method_str).map_err(|_| {
                        async_sqlite::rusqlite::Error::InvalidColumnType(
                            1,
                            "method".to_string(),
                            async_sqlite::rusqlite::types::Type::Text,
                        )
                    })?,
                    response,
                    expires,
                    cached: Some(true),
                })
            })
        })
        .await;

    match result {
        Ok(record) => Ok(Some(record)),
        Err(async_sqlite::Error::Rusqlite(async_sqlite::rusqlite::Error::QueryReturnedNoRows)) => {
            Ok(None)
        }
        Err(e) => Err(RequestCacheError::Database(e)),
    }
}

async fn insert_record(connection: &Client, record: Record) -> Result<usize, RequestCacheError> {
    // remove other records for this url/method
    let method_str = record.method.to_string();
    let request_str = record.request.to_string();
    let query = "DELETE FROM requests WHERE request = ?1 AND method = ?2;";

    connection
        .conn(move |conn| conn.execute(query, params![request_str, method_str]))
        .await?;

    // then insert the new record
    let method_str = record.method.to_string();
    let request_str = record.request.to_string();
    let query = "INSERT INTO requests VALUES (?1, ?2, ?3, ?4);";

    let result = connection
        .conn(move |conn| {
            conn.execute(
                query,
                params![request_str, method_str, record.response, record.expires],
            )
        })
        .await?;

    Ok(result)
}

// Global HTTP client for reuse across requests
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn get_http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30)) // Default timeout
            .user_agent("request_cache/0.1.0")
            .pool_max_idle_per_host(10) // Improve connection reuse
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .expect("Failed to create HTTP client")
    })
}

async fn make_request(
    connection: &Client,
    url: &Url,
    method: &HttpMethod,
    timeout: i64,
    user_agent: Option<String>,
) -> Result<Record, RequestCacheError> {
    let client = get_http_client();
    let mut headers = HeaderMap::new();

    // Validate and set user agent
    if let Some(user_agent) = user_agent {
        let header_value = user_agent
            .parse()
            .map_err(|_| RequestCacheError::InvalidUrl(url::ParseError::EmptyHost))?;
        headers.insert(USER_AGENT, header_value);
    }

    // Create request with timeout
    let request_timeout = Duration::from_secs(timeout.min(300) as u64); // Cap at 5 minutes
    let request_builder = match method {
        HttpMethod::GET => client.get(url.clone()),
        HttpMethod::POST => client.post(url.clone()),
        HttpMethod::PUT => client.put(url.clone()),
        HttpMethod::DELETE => client.delete(url.clone()),
        HttpMethod::HEAD => client.head(url.clone()),
        HttpMethod::PATCH => client.patch(url.clone()),
    };

    let response = request_builder
        .headers(headers)
        .timeout(request_timeout)
        .send()
        .await?
        .text()
        .await?;

    // Calculate expiry timestamp
    let expiry_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            RequestCacheError::Database(async_sqlite::Error::Rusqlite(
                async_sqlite::rusqlite::Error::SqliteFailure(
                    async_sqlite::rusqlite::ffi::Error::new(
                        async_sqlite::rusqlite::ffi::SQLITE_MISUSE,
                    ),
                    Some("System time error".to_string()),
                ),
            ))
        })?
        .as_secs() as i64
        + timeout;

    let record = Record {
        request: url.clone(),
        method: method.clone(),
        response,
        expires: expiry_timestamp,
        cached: Some(false),
    };

    // Add to the cache
    insert_record(connection, record.clone()).await?;

    Ok(record)
}

/// Builder for creating cached HTTP requests with a fluent, chainable API.
///
/// This is the recommended way to create cached requests as it provides better
/// type safety, clearer code, and more flexible configuration options.
///
/// # Examples
///
/// ```rust,no_run
/// use request_cache::{RequestBuilder, HttpMethod};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Basic GET request
/// let record = RequestBuilder::new()
///     .url("https://api.github.com/users/octocat")?
///     .method(HttpMethod::GET)
///     .timeout(300)?
///     .send()
///     .await?;
///
/// // POST request with custom configuration
/// let record = RequestBuilder::new()
///     .url("https://api.example.com/data")?
///     .method(HttpMethod::POST)
///     .timeout(600)?
///     .user_agent("MyApp/2.0")
///     .database_path("custom_cache.db")
///     .force_refresh(true)
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RequestBuilder {
    url: Option<Url>,
    method: HttpMethod,
    timeout: i64,
    force_refresh: bool,
    user_agent: Option<String>,
    db_path: Option<String>,
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self {
            url: None,
            method: HttpMethod::GET,
            timeout: 300, // 5 minutes default
            force_refresh: false,
            user_agent: None,
            db_path: None,
        }
    }
}

impl RequestBuilder {
    /// Create a new request builder with default settings.
    ///
    /// Default values:
    /// - Method: GET
    /// - Timeout: 300 seconds (5 minutes)
    /// - Force refresh: false
    /// - Database path: "request_cache_db"
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the URL for the request.
    ///
    /// # Arguments
    /// * `url` - A string that can be parsed as a valid HTTP/HTTPS URL
    ///
    /// # Errors
    /// Returns [`RequestCacheError::InvalidUrl`] if the URL cannot be parsed.
    pub fn url<U: AsRef<str>>(mut self, url: U) -> Result<Self, RequestCacheError> {
        self.url = Some(Url::parse(url.as_ref())?);
        Ok(self)
    }

    /// Set the HTTP method using the [`HttpMethod`] enum.
    ///
    /// This is the type-safe way to specify HTTP methods.
    pub fn method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }

    /// Set the HTTP method from a string.
    ///
    /// # Arguments
    /// * `method` - HTTP method as a string ("GET", "POST", etc.)
    ///
    /// # Errors
    /// Returns [`RequestCacheError::InvalidMethod`] if the method is not supported.
    pub fn method_str<M: AsRef<str>>(mut self, method: M) -> Result<Self, RequestCacheError> {
        self.method = HttpMethod::from_str(method.as_ref())?;
        Ok(self)
    }

    /// Set the cache timeout in seconds.
    ///
    /// This determines how long responses will be cached before they expire.
    ///
    /// # Arguments
    /// * `timeout` - Timeout in seconds (must be positive)
    ///
    /// # Errors
    /// Returns [`RequestCacheError::InvalidTimeout`] if timeout is negative or zero.
    pub fn timeout(mut self, timeout: i64) -> Result<Self, RequestCacheError> {
        if timeout <= 0 {
            return Err(RequestCacheError::InvalidTimeout);
        }
        self.timeout = timeout;
        Ok(self)
    }

    /// Force refresh of cached content.
    ///
    /// When set to `true`, the cache will be bypassed and a fresh request
    /// will be made to the server.
    ///
    /// # Arguments
    /// * `force` - Whether to bypass the cache
    pub fn force_refresh(mut self, force: bool) -> Self {
        self.force_refresh = force;
        self
    }

    /// Set a custom User-Agent header for the request.
    ///
    /// # Arguments
    /// * `user_agent` - User-Agent string to include in the request
    pub fn user_agent<U: Into<String>>(mut self, user_agent: U) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Set a custom path for the SQLite database file.
    ///
    /// # Arguments
    /// * `path` - File system path where the cache database should be stored
    pub fn database_path<P: Into<String>>(mut self, path: P) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Execute the request and return the cached or fresh response.
    ///
    /// This creates a new database connection for the request. For better performance
    /// when making multiple requests, consider using [`RequestBuilder::send_with_connection`] instead.
    ///
    /// # Errors
    /// Returns an error if the URL is not set, or if any network/database operation fails.
    pub async fn send(self) -> Result<Record, RequestCacheError> {
        let url = self
            .url
            .ok_or_else(|| RequestCacheError::InvalidUrl(url::ParseError::EmptyHost))?;

        let db_path = self
            .db_path
            .unwrap_or_else(|| String::from("request_cache_db"));
        let connection = create_connection(db_path).await?;

        request(
            &connection,
            url,
            self.method,
            self.timeout,
            Some(self.force_refresh),
            self.user_agent,
        )
        .await
    }

    /// Execute the request using an existing database connection.
    ///
    /// This is more efficient than [`RequestBuilder::send`] when making multiple requests as it
    /// reuses the database connection.
    ///
    /// # Arguments
    /// * `connection` - An existing database connection from [`create_connection`]
    ///
    /// # Errors
    /// Returns an error if the URL is not set, or if any network/database operation fails.
    pub async fn send_with_connection(
        self,
        connection: &Client,
    ) -> Result<Record, RequestCacheError> {
        let url = self
            .url
            .ok_or_else(|| RequestCacheError::InvalidUrl(url::ParseError::EmptyHost))?;

        request(
            connection,
            url,
            self.method,
            self.timeout,
            Some(self.force_refresh),
            self.user_agent,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, thread::sleep, time::Duration};

    use super::*;

    struct TestCleanup {
        path: String,
    }

    impl Drop for TestCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[tokio::test]
    async fn test_create_connection() {
        let result = create_connection("test".to_string()).await;
        assert!(result.is_ok());
        let _ = fs::remove_file("test");
    }

    #[tokio::test]
    async fn test_connection_and_request() {
        let clean = TestCleanup {
            path: "test_1".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await.unwrap();
        let url = Url::parse("https://httpbin.org/json").unwrap();
        let method = HttpMethod::GET;

        // First request should not be cached
        let resp = request(
            &db_client,
            url.clone(),
            method.clone(),
            10000,
            Some(false),
            None,
        )
        .await;
        
        // Handle potential network failures gracefully
        match resp {
            Ok(record) => {
                assert!(record.cached == Some(false));
                
                // Verify database entry was created
                let query = "SELECT COUNT(*) FROM requests";
                let res: i64 = db_client
                    .conn(move |conn| conn.query_row(query, [], |row| row.get(0)))
                    .await
                    .unwrap();
                assert_eq!(res, 1);

                // Second request should be cached
                let resp2_result = request(&db_client, url.clone(), method.clone(), 10000, None, None)
                    .await;
                    
                let resp2 = match resp2_result {
                    Ok(resp) => resp,
                    Err(_) => {
                        println!("Second network request failed, skipping cache assertion");
                        return;
                    }
                };
                assert!(resp2.cached == Some(true));

                // Force refresh should bypass cache
                let resp3_result = request(
                    &db_client,
                    url,
                    method,
                    10000,
                    Some(true),
                    Some("dummy".to_string()),
                )
                .await;
                
                let resp3 = match resp3_result {
                    Ok(resp) => resp,
                    Err(_) => {
                        println!("Force refresh network request failed, skipping assertion");
                        return;
                    }
                };
                assert!(resp3.cached == Some(false));
            }
            Err(_) => {
                // Skip this test if network is unavailable
                println!("Skipping network-dependent test due to connection failure");
            }
        }
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let clean = TestCleanup {
            path: "test_4".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await.unwrap();
        let url = Url::parse("https://httpbin.org/uuid").unwrap();
        let method = HttpMethod::GET;

        // Make first request with short timeout
        let resp1 = request(
            &db_client,
            url.clone(),
            method.clone(),
            1, // 1 second timeout
            Some(false),
            Some("test_agent".to_string()),
        )
        .await;
        
        match resp1 {
            Ok(record) => {
                assert!(record.cached == Some(false));
                
                // Second request should be cached
                let resp2 = request(
                    &db_client,
                    url.clone(),
                    method.clone(),
                    1,
                    Some(false),
                    None,
                )
                .await
                .unwrap();
                assert!(resp2.cached == Some(true));

                // Wait for cache to expire
                sleep(Duration::from_secs(2));

                // Third request should not be cached (expired)
                let resp3 = request(&db_client, url, method, 5, Some(false), None)
                    .await
                    .unwrap();
                assert!(resp3.cached == Some(false));
            }
            Err(_) => {
                println!("Skipping network-dependent test due to connection failure");
            }
        }
    }

    #[tokio::test]
    async fn test_cached_request() {
        let clean = TestCleanup {
            path: "test_5".to_string(),
        };

        let resp = cached_request(
            "https://httpbin.org/get".to_string(),
            "GET".to_string(),
            10000,
            Some(false),
            None,
            Some(clean.path.clone()),
        )
        .await;
        
        match resp {
            Ok(record) => {
                assert!(record.cached == Some(false));

                let query = "SELECT COUNT(*) FROM requests";
                let db_client = create_connection(clean.path.clone()).await.unwrap();
                let res: i64 = db_client
                    .conn(move |conn| conn.query_row(query, [], |row| row.get(0)))
                    .await
                    .unwrap();
                assert_eq!(res, 1);

                // Second request should be cached
                let resp2_result = cached_request(
                    "https://httpbin.org/get".to_string(),
                    "GET".to_string(),
                    10000,
                    None,
                    None,
                    Some(clean.path.clone()),
                )
                .await;
                
                let resp2 = match resp2_result {
                    Ok(resp) => resp,
                    Err(_) => {
                        println!("Network request failed, skipping cache assertion");
                        return;
                    }
                };
                assert!(resp2.cached == Some(true));

                // Force refresh should bypass cache
                let resp3_result = cached_request(
                    "https://httpbin.org/get".to_string(),
                    "GET".to_string(),
                    10000,
                    Some(true),
                    Some("dummy".to_string()),
                    Some(clean.path.clone()),
                )
                .await;
                
                let resp3 = match resp3_result {
                    Ok(resp) => resp,
                    Err(_) => {
                        println!("Network request for force refresh failed, skipping assertion");
                        return;
                    }
                };
                assert!(resp3.cached == Some(false));
            }
            Err(_) => {
                println!("Skipping network-dependent test due to connection failure");
            }
        }
    }

    #[tokio::test]
    async fn test_cached_request_timeout() {
        let clean = TestCleanup {
            path: "test_6".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await.unwrap();

        let resp = cached_request(
            "https://httpbin.org/headers".to_string(),
            "GET".to_string(),
            1, // 1 second timeout
            Some(false),
            Some("dummy".to_string()),
            Some(clean.path.clone()),
        )
        .await;
        
        match resp {
            Ok(record) => {
                assert!(record.cached == Some(false));

                let query = "SELECT COUNT(*) FROM requests";
                let res: i64 = db_client
                    .conn(move |conn| conn.query_row(query, [], |row| row.get(0)))
                    .await
                    .unwrap();
                assert_eq!(res, 1);

                // Second request should be cached
                let resp2 = cached_request(
                    "https://httpbin.org/headers".to_string(),
                    "GET".to_string(),
                    1,
                    Some(false),
                    None,
                    Some(clean.path.clone()),
                )
                .await
                .unwrap();
                assert!(resp2.cached == Some(true));

                // Wait for cache to expire
                sleep(Duration::from_secs(2));

                // Third request should not be cached (expired)
                let resp3 = cached_request(
                    "https://httpbin.org/headers".to_string(),
                    "GET".to_string(),
                    5,
                    Some(false),
                    None,
                    Some(clean.path.clone()),
                )
                .await
                .unwrap();
                assert!(resp3.cached == Some(false));
            }
            Err(_) => {
                println!("Skipping network-dependent test due to connection failure");
            }
        }
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let clean = TestCleanup {
            path: "test_builder".to_string(),
        };

        // Test successful request with builder
        let result = RequestBuilder::new()
            .url("https://httpbin.org/ip")
            .unwrap()
            .method(HttpMethod::GET)
            .timeout(10000)
            .unwrap()
            .force_refresh(false)
            .user_agent("test-agent")
            .database_path(&clean.path)
            .send()
            .await;

        match result {
            Ok(record) => {
                assert_eq!(record.cached, Some(false));
            }
            Err(_) => {
                println!("Skipping network-dependent test due to connection failure");
            }
        }

        // Test error handling (these don't require network)
        let result = RequestBuilder::new().url("invalid-url").unwrap_err();
        assert!(matches!(result, RequestCacheError::InvalidUrl(_)));

        let result = RequestBuilder::new().timeout(-1).unwrap_err();
        assert!(matches!(result, RequestCacheError::InvalidTimeout));
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Test invalid URL
        let result = cached_request(
            "not-a-url".to_string(),
            "GET".to_string(),
            300,
            None,
            None,
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RequestCacheError::InvalidUrl(_)
        ));

        // Test invalid method
        let result = cached_request(
            "http://example.com".to_string(),
            "INVALID".to_string(),
            300,
            None,
            None,
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RequestCacheError::InvalidMethod(_)
        ));

        // Test invalid timeout
        let result = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            -1,
            None,
            None,
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RequestCacheError::InvalidTimeout
        ));
    }
}
