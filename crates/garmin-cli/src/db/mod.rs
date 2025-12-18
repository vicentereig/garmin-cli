//! Database module for DuckDB persistence
//!
//! Provides connection management, schema migrations, and query helpers.

pub mod models;
pub mod schema;

use duckdb::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Database connection wrapper with thread-safe access
pub struct Database {
    conn: Arc<Mutex<Connection>>,
    path: Option<String>,
}

impl Database {
    /// Open or create a database at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let conn = Connection::open(&path_str)
            .map_err(|e| crate::error::GarminError::Database(e.to_string()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path: Some(path_str),
        };

        // Run migrations on open
        db.migrate()?;

        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn in_memory() -> crate::Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| crate::error::GarminError::Database(e.to_string()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path: None,
        };

        db.migrate()?;

        Ok(db)
    }

    /// Run schema migrations
    pub fn migrate(&self) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        schema::migrate(&conn)
    }

    /// Execute a query with no return value
    pub fn execute(&self, sql: &str) -> crate::Result<usize> {
        let conn = self.conn.lock().unwrap();
        conn.execute(sql, [])
            .map_err(|e| crate::error::GarminError::Database(e.to_string()))
    }

    /// Execute a query with parameters
    pub fn execute_params<P: duckdb::Params>(&self, sql: &str, params: P) -> crate::Result<usize> {
        let conn = self.conn.lock().unwrap();
        conn.execute(sql, params)
            .map_err(|e| crate::error::GarminError::Database(e.to_string()))
    }

    /// Get a connection for complex operations
    /// Note: Caller must handle locking
    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
    }

    /// Get database path (None for in-memory)
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            path: self.path.clone(),
        }
    }
}

/// Default database path in user's data directory
pub fn default_db_path() -> crate::Result<String> {
    let data_dir = crate::config::data_dir()?;
    Ok(data_dir.join("garmin.duckdb").to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_database() {
        let db = Database::in_memory().expect("Failed to create in-memory db");
        assert!(db.path().is_none());
    }

    #[test]
    #[ignore] // Requires DuckDB extensions that may not be available in CI
    fn test_execute_query() {
        let db = Database::in_memory().expect("Failed to create db");
        let result = db.execute("SELECT 1");
        assert!(result.is_ok());
    }
}
