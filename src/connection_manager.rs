/*
📌 Phase 5.3: Database Connection Manager - SOLVING LOCK CONTENTION
🔗 Shared connection pool to prevent database lock conflicts
✅ Thread-safe singleton pattern for database connections
✅ Connection reuse across multiple clients
✅ Proper connection lifecycle management
*/

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use sled::Db;
use crate::error::{MiniDbError, MiniDbResult, ConnectionType};

// Global connection manager instance
static CONNECTION_MANAGER: OnceLock<DatabaseConnectionManager> = OnceLock::new();

/// Thread-safe database connection manager that prevents lock contention
/// by reusing database connections across multiple clients
pub struct DatabaseConnectionManager {
    connections: Arc<Mutex<HashMap<String, Arc<Db>>>>,
}

impl DatabaseConnectionManager {
    /// Get the global connection manager instance (singleton)
    pub fn global() -> &'static DatabaseConnectionManager {
        CONNECTION_MANAGER.get_or_init(|| {
            DatabaseConnectionManager {
                connections: Arc::new(Mutex::new(HashMap::new())),
            }
        })
    }

    /// Get a shared database connection for the given path
    /// Returns the same connection instance for the same path to prevent locks
    pub fn get_connection(&self, database_path: &str) -> MiniDbResult<Arc<Db>> {
        let mut connections = self.connections.lock()
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Pool, 
                &format!("Failed to acquire connections lock: {}", e),
                None
            ))?;

        // Check if we already have a connection for this path
        if let Some(existing_connection) = connections.get(database_path) {
            return Ok(Arc::clone(existing_connection));
        }

        // Create new connection
        let db = sled::open(database_path)
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Database,
                &format!("Failed to open database: {}", e),
                Some(database_path)
            ))?;
        
        let shared_db = Arc::new(db);
        connections.insert(database_path.to_string(), Arc::clone(&shared_db));

        println!("🔗 DatabaseConnectionManager: Created new connection for '{}'", database_path);
        Ok(shared_db)
    }

    /// Get an in-memory database connection (for testing)
    /// Each call creates a unique temporary database
    pub fn get_temp_connection(&self) -> MiniDbResult<Arc<Db>> {
        let temp_path = format!("temp_db_{}", uuid::Uuid::new_v4());
        let db = sled::open(&temp_path)
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Database,
                &format!("Failed to create temporary database: {}", e),
                Some(&temp_path)
            ))?;
        
        println!("🧪 DatabaseConnectionManager: Created temporary connection at '{}'", temp_path);
        Ok(Arc::new(db))
    }

    /// Get connection statistics for monitoring
    pub fn get_stats(&self) -> MiniDbResult<ConnectionStats> {
        let connections = self.connections.lock()
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Pool,
                &format!("Failed to acquire connections lock: {}", e),
                None
            ))?;

        Ok(ConnectionStats {
            total_connections: connections.len(),
            connection_paths: connections.keys().cloned().collect(),
        })
    }

    /// Close a specific database connection (useful for cleanup)
    pub fn close_connection(&self, database_path: &str) -> MiniDbResult<bool> {
        let mut connections = self.connections.lock()
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Pool,
                &format!("Failed to acquire connections lock: {}", e),
                None
            ))?;

        let removed = connections.remove(database_path).is_some();
        if removed {
            println!("🔒 DatabaseConnectionManager: Closed connection for '{}'", database_path);
        }
        Ok(removed)
    }

    /// Close all connections (useful for shutdown)
    pub fn close_all_connections(&self) -> MiniDbResult<usize> {
        let mut connections = self.connections.lock()
            .map_err(|e| MiniDbError::connection(
                ConnectionType::Pool,
                &format!("Failed to acquire connections lock: {}", e),
                None
            ))?;

        let count = connections.len();
        connections.clear();
        println!("🔒 DatabaseConnectionManager: Closed all {} connections", count);
        Ok(count)
    }
}

/// Statistics about database connections
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub total_connections: usize,
    pub connection_paths: Vec<String>,
}

impl std::fmt::Display for ConnectionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connections: {}, Paths: {:?}", self.total_connections, self.connection_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_connection_reuse() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db").to_string_lossy().to_string();
        
        let manager = DatabaseConnectionManager::global();
        
        // Get first connection
        let conn1 = manager.get_connection(&db_path).unwrap();
        
        // Get second connection - should be the same instance
        let conn2 = manager.get_connection(&db_path).unwrap();
        
        // Verify they're the same Arc instance
        assert!(Arc::ptr_eq(&conn1, &conn2));
    }

    #[test]
    fn test_temp_connections_are_unique() {
        let manager = DatabaseConnectionManager::global();
        
        let temp1 = manager.get_temp_connection().unwrap();
        let temp2 = manager.get_temp_connection().unwrap();
        
        // Temporary connections should be different instances
        assert!(!Arc::ptr_eq(&temp1, &temp2));
    }

    #[test]
    fn test_connection_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("stats_test.db").to_string_lossy().to_string();
        
        let manager = DatabaseConnectionManager::global();
        let initial_stats = manager.get_stats().unwrap();
        let initial_count = initial_stats.total_connections;
        
        // Create a connection
        let _conn = manager.get_connection(&db_path).unwrap();
        
        let stats = manager.get_stats().unwrap();
        assert_eq!(stats.total_connections, initial_count + 1);
        assert!(stats.connection_paths.contains(&db_path));
    }
}