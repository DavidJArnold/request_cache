//! `request_cache` is a small wrapper around the `reqwest` crate to provide asynchronous cached
//! HTTP responses.

use async_sqlite::{rusqlite::params, Client, ClientBuilder, Error};
use reqwest::header::{HeaderMap, USER_AGENT};

#[derive(Debug, Clone)]
pub struct Record {
    pub request: String,
    pub method: String,
    pub response: String,
    pub expires: i64,
    pub cached: Option<bool>,
}

/// Get response from a request, using cache if available
pub async fn cached_request(
    url: String,
    method: String,
    timeout: i64,
    force_refresh: Option<bool>,
    user_agent: Option<String>,
    db_path: Option<String>,
) -> Record {
    let connection = create_connection(db_path.unwrap_or(String::from("request_cache_db"))).await;

    request(&connection, url, method, timeout, force_refresh, user_agent).await
}

/// Return a connection for the database located at /path
pub async fn create_connection(path: String) -> Client {
    let client = ClientBuilder::new().path(path).open().await.unwrap();
    let _ = client.conn(move |conn| conn.execute_batch("CREATE TABLE IF NOT EXISTS requests (request TEXT, method TEXT, response TEXT, expires INTEGER);")).await;
    client
}

/// Cached request using an explicit connection
pub async fn request(
    connection: &Client,
    url: String,
    method: String,
    timeout: i64,
    force_refresh: Option<bool>,
    user_agent: Option<String>,
) -> Record {
    if force_refresh.unwrap_or(false) {
        return make_request(connection, &url, &method, timeout, user_agent).await;
    }
    // make a request, using cached response if one exists
    match get_record(connection, url.clone(), method.clone()).await {
        Some(x) => x,
        _ => make_request(connection, &url, &method, timeout, user_agent).await,
    }
}

async fn get_record(connection: &Client, url: String, method: String) -> Option<Record> {
    // try to get a record from the DB
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let query = "SELECT * FROM requests WHERE request = ?1 AND method = ?2 AND expires > ?3 ORDER BY expires DESC LIMIT 1;";
    connection
        .conn(move |conn| {
            conn.query_row(query, params![url, method, current_time], |row| {
                Ok(Record {
                    method: row.get(0)?,
                    request: row.get(1)?,
                    response: row.get(2)?,
                    expires: row.get(3)?,
                    cached: Some(true),
                })
            })
        })
        .await
        .ok()
}

async fn insert_record(connection: &Client, record: Record) -> Result<usize, Error> {
    // remove other records for this url/method
    let method = record.method.clone();
    let request = record.request.clone();
    let query = "DELETE FROM requests WHERE request = ?1 AND method = ?2;";
    let _ = connection
        .conn(move |conn| conn.execute(query, params![request, method]))
        .await;
    // then insert the new record
    let query = "INSERT INTO requests VALUES (?1, ?2, ?3, ?4);";
    connection
        .conn(move |conn| {
            conn.execute(
                query,
                params![
                    record.request,
                    record.method,
                    record.response,
                    record.expires
                ],
            )
        })
        .await
}

async fn make_request(
    connection: &Client,
    url: &str,
    method: &str,
    timeout: i64,
    user_agent: Option<String>,
) -> Record {
    // make an HTTP request and create a Record
    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    if let Some(user_agent) = user_agent {
        headers.insert(USER_AGENT, user_agent.parse().unwrap());
    }

    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // expires timeout seconds after now
    let expiry_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + timeout;
    let record = Record {
        request: url.to_string(),
        method: method.to_string(),
        response,
        expires: expiry_timestamp,
        cached: Some(false),
    };
    // add to the cache
    insert_record(connection, record.clone()).await.unwrap();

    record
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
        create_connection("test".to_string()).await;
        let _ = fs::remove_file(&"test");
    }

    #[tokio::test]
    async fn test_connection_and_request() {
        let clean = TestCleanup {
            path: "test_1".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await;
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(false),
            None,
        )
        .await;
        assert!(resp.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            None,
            None,
        )
        .await;
        assert!(resp.cached == Some(true));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(true),
            Some("dummy".to_string()),
        )
        .await;
        assert!(resp.cached == Some(false));
    }

    #[tokio::test]
    async fn test_connection_and_request_timeout() {
        let clean = TestCleanup {
            path: "test_4".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await;
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            1,
            Some(false),
            Some("dummy".to_string()),
        );
        assert!(resp.await.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            1,
            Some(false),
            None,
        );
        assert!(resp.await.cached == Some(true));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        sleep(Duration::from_secs(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            5,
            Some(false),
            None,
        );
        assert!(resp.await.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
    }

    #[tokio::test]
    async fn test_cached_request() {
        let clean = TestCleanup {
            path: "test_5".to_string(),
        };
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(false),
            None,
            Some(clean.path.clone()),
        )
        .await;
        assert!(resp.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let db_client = create_connection(clean.path.clone()).await;
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            None,
            None,
            Some(clean.path.clone()),
        )
        .await;
        assert!(resp.cached == Some(true));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(true),
            Some("dummy".to_string()),
            Some(clean.path.clone()),
        )
        .await;
        assert!(resp.cached == Some(false));
    }

    #[tokio::test]
    async fn test_cached_request_timeout() {
        let clean = TestCleanup {
            path: "test_6".to_string(),
        };
        let db_client = create_connection(clean.path.clone()).await;
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            1,
            Some(false),
            Some("dummy".to_string()),
            Some(clean.path.clone()),
        );
        assert!(resp.await.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            1,
            Some(false),
            None,
            Some(clean.path.clone()),
        );
        assert!(resp.await.cached == Some(true));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
        sleep(Duration::from_secs(1));
        let resp = cached_request(
            "http://example.com".to_string(),
            "GET".to_string(),
            5,
            Some(false),
            None,
            Some(clean.path.clone()),
        );
        assert!(resp.await.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client
            .conn(move |conn| conn.query_row(&query, [], |row| Ok(row.get(0))))
            .await
            .unwrap();
        assert!(res == Ok(1));
    }
}
