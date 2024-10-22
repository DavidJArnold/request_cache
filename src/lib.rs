use reqwest::header::{HeaderMap, USER_AGENT};
use async_sqlite::{Error, Client, ClientBuilder};

#[derive(Debug)]
pub struct Record {
    pub request: String,
    pub method: String,
    pub response: String,
    pub expires: i64,
    pub cached: Option<bool>,
}

pub async fn create_connection(path: &str) -> Client {
    // Return a connection for the database located at /path
    let client = ClientBuilder::new()
                .path(path)
                .open()
                .await
                .unwrap();
    let _ = client.conn(move |conn| conn.execute_batch("CREATE TABLE IF NOT EXISTS requests (request TEXT, method TEXT, response TEXT, expires INTEGER);")).await;
    client
}

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
    match get_record(connection, &url, &method).await {
        Some(x) => x,
        _ => make_request(connection, &url, &method, timeout, user_agent).await,
    }
}

async fn get_record(connection: &Client, url: &str, method: &str) -> Option<Record> {
    // try to get a record from the DB
    let query = format!("SELECT * FROM requests WHERE request = '{}' AND method = '{}' AND expires > {} ORDER BY expires DESC LIMIT 1;", url, method, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64);
    connection.conn(move |conn| conn.query_row(&query, [], |row| Ok(Record {
        method: row.get(0)?,
        request: row.get(1)?,
        response: row.get(2)?,
        expires: row.get(3)?,
        cached: Some(true),
    }))).await.ok()
}

async fn insert_record(connection: &Client, record: &Record) -> Result<(), Error> {
    // remove other records for this url/method
    let query = format!(
        "DELETE FROM requests WHERE request = '{}' AND method = '{}';",
        record.request, record.method
    );
    let _ = connection.conn(move |conn| conn.execute_batch(&query)).await;
    // then insert the new record
    let query = format!(
        "INSERT INTO requests VALUES ('{}', '{}', '{}', {});",
        record.request, record.method, record.response, record.expires
    );
    connection.conn(move |conn| conn.execute_batch(&query)).await
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
    insert_record(connection, &record).await.unwrap();

    record
}

#[cfg(test)]
mod tests {
    use std::{fs, thread::sleep, time::Duration};

    use super::*;

    struct TestCleanup<'a> {
        path: &'a str,
    }

    impl<'a> Drop for TestCleanup<'a> {
        fn drop(&mut self) {
            let _ = fs::remove_file(self.path);
        }
    }

    #[tokio::test]
    async fn test_create_connection() {
        create_connection("test").await;
        let _ = fs::remove_file("test");
    }

    #[tokio::test]
    async fn test_cache_request() {
        let clean = TestCleanup { path: &"test_1" };
        let db_client = create_connection(clean.path).await;
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(false),
            None,
        ).await;
        assert!(resp.cached == Some(false));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client.conn(move |conn| conn.query_row(&query, [], |row| { Ok(row.get(0))})).await.unwrap();
        assert!(res == Ok(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            None,
            None,
        ).await;
        assert!(resp.cached == Some(true));
        let query = "SELECT COUNT(*) FROM requests";
        let res = db_client.conn(move |conn| conn.query_row(&query, [], |row| { Ok(row.get(0))})).await.unwrap();
        assert!(res == Ok(1));
        let resp = request(
            &db_client,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(true),
            Some("dummy".to_string()),
        ).await;
        assert!(resp.cached == Some(false));
    }

    #[tokio::test]
    async fn test_cache_request_timeout() {
        let clean = TestCleanup { path: &"test_4" };
        let db_client = create_connection(clean.path).await;
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
        let res = db_client.conn(move |conn| conn.query_row(&query, [], |row| { Ok(row.get(0))})).await.unwrap();
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
        let res = db_client.conn(move |conn| conn.query_row(&query, [], |row| { Ok(row.get(0))})).await.unwrap();
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
        let res = db_client.conn(move |conn| conn.query_row(&query, [], |row| { Ok(row.get(0))})).await.unwrap();
        assert!(res == Ok(1));
    }
}
