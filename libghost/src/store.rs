use rusqlite::{Connection, params};
use std::sync::Mutex;

pub trait GhostStore: Send + Sync {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: Vec<u8>);
    fn delete(&self, key: &str);
    fn list(&self, prefix: &str) -> Vec<String>;
}

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn new(path: &str) -> Self {
        let conn = Connection::open(path).expect("failed to open sqlite");
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
        ",
        )
        .expect("failed to create table");
        Self {
            conn: Mutex::new(conn),
        }
    }

    pub fn in_memory() -> Self {
        let conn = Connection::open_in_memory().expect("failed to open in-memory sqlite");
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
        ",
        )
        .expect("failed to create table");
        Self {
            conn: Mutex::new(conn),
        }
    }
}

impl GhostStore for SqliteStore {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT value FROM kv WHERE key = ?1", params![key], |row| {
            row.get(0)
        })
        .ok()
    }

    fn set(&self, key: &str, value: Vec<u8>) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .expect("store set failed");
    }

    fn delete(&self, key: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM kv WHERE key = ?1", params![key])
            .expect("store delete failed");
    }

    fn list(&self, prefix: &str) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT key FROM kv WHERE key LIKE ?1 ORDER BY key")
            .unwrap();
        let pattern = format!("{}%", prefix);
        stmt.query_map(params![pattern], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }
}

pub struct MemoryStore {
    map: Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl GhostStore for MemoryStore {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.map.lock().unwrap().get(key).cloned()
    }
    fn set(&self, key: &str, value: Vec<u8>) {
        self.map.lock().unwrap().insert(key.to_string(), value);
    }
    fn delete(&self, key: &str) {
        self.map.lock().unwrap().remove(key);
    }
    fn list(&self, prefix: &str) -> Vec<String> {
        self.map
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}
