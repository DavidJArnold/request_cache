use reqwest::header::{HeaderMap, USER_AGENT};
use sqlite::{Connection, State};

#[derive(Debug)]
pub struct Record {
    pub request: String,
    pub method: String,
    pub response: String,
    pub expires: i64,
    pub cached: Option<bool>,
}

pub fn create_connection(path: &str) -> Connection {
    // Return a connection for the database located at /path
    let conn = Connection::open(path).unwrap();
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS requests (request TEXT, method TEXT, response TEXT, expires INTEGER);");
    conn
}

pub fn request(
    connection: &Connection,
    url: String,
    method: String,
    timeout: i64,
    force_refresh: Option<bool>,
    user_agent: Option<&str>,
) -> Record {
    if force_refresh.unwrap_or(false) {
        return make_request(connection, &url, &method, timeout, user_agent);
    }
    // make a request, using cached response if one exists
    match get_record(connection, &url, &method) {
        Some(x) => x,
        _ => make_request(connection, &url, &method, timeout, user_agent),
    }
}

fn get_record(connection: &Connection, url: &str, method: &str) -> Option<Record> {
    // try to get a record from the DB
    let query = format!("SELECT * FROM requests WHERE request = '{}' AND method = '{}' AND expires > {} ORDER BY expires DESC LIMIT 1;", url, method, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64);
    let mut result = connection.prepare(query).unwrap();
    match result.next() {
        Err(_) => None,
        Ok(State::Done) => None,
        Ok(_) => Some(Record {
            method: result.read::<String, _>("method").unwrap(),
            request: result.read::<String, _>("request").unwrap(),
            response: result.read::<String, _>("response").unwrap(),
            expires: result.read::<i64, _>("expires").unwrap(),
            cached: Some(true),
        }),
    }
}

fn insert_record(connection: &Connection, record: &Record) -> Result<(), sqlite::Error> {
    // remove other records for this url/method
    let query = format!(
        "DELETE FROM requests WHERE request = '{}' AND method = '{}';",
        record.request, record.method
    );
    let _ = connection.execute(query);
    // then insert the new record
    let query = format!(
        "INSERT INTO requests VALUES ('{}', '{}', '{}', {});",
        record.request, record.method, record.response, record.expires
    );
    connection.execute(query)
}

fn make_request(
    connection: &Connection,
    url: &str,
    method: &str,
    timeout: i64,
    user_agent: Option<&str>,
) -> Record {
    // make an HTTP request and create a Record
    let client = reqwest::blocking::Client::new();
    let mut headers = HeaderMap::new();
    if let Some(user_agent) = user_agent {
        headers.insert(USER_AGENT, user_agent.parse().unwrap());
    }

    let response = client
        .get(url)
        .headers(headers)
        .send()
        .unwrap()
        .text()
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
    insert_record(connection, &record).unwrap();

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

    #[test]
    fn test_create_connection() {
        create_connection("test");
        let _ = fs::remove_file("test");
    }

    #[test]
    fn test_cache_request() {
        let clean = TestCleanup { path: &"test_1" };
        let conn = create_connection(clean.path);
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(false),
            None,
        );
        assert!(resp.cached == Some(false));
        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(statement.iter().count() == 1);
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            None,
            None,
        );
        assert!(resp.cached == Some(true));
        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(statement.iter().count() == 1);
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            10000,
            Some(true),
            Some("dummy"),
        );
        assert!(resp.cached == Some(false));
    }

    #[test]
    fn test_cache_request_timeout() {
        let clean = TestCleanup { path: &"test_4" };
        let conn = create_connection(clean.path);
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            5,
            Some(false),
            Some("dummy"),
        );
        assert!(resp.cached == Some(false));
        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(statement.iter().count() == 1);
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            5,
            Some(false),
            None,
        );
        assert!(resp.cached == Some(true));
        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(statement.iter().count() == 1);
        sleep(Duration::from_secs(8));
        let resp = request(
            &conn,
            "http://example.com".to_string(),
            "GET".to_string(),
            5,
            Some(false),
            None,
        );
        assert!(resp.cached == Some(false));
        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(
            statement.iter().count() == 1,
            "{}",
            statement.iter().count()
        );
    }

    #[test]
    fn test_create_table() {
        let _clean = TestCleanup { path: &"test_2" };
        let _ = fs::remove_file("test_2");
        let conn = create_connection("test_2");
        let _ = conn
            .execute(format!(
                "INSERT INTO requests VALUES ('request', 'GET', 'response', {});",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    + 20
            ))
            .unwrap();
        let _ = conn
            .execute(format!(
                "INSERT INTO requests VALUES ('reques2', 'GE2', 'respons2', {});",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    + 86_400
            ))
            .unwrap();

        let query = "SELECT * FROM requests";
        let mut statement = conn.prepare(query).unwrap();
        assert!(statement.iter().count() == 2);
    }
}
