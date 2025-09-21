/*
📌 File: src/client.rs (COMPLETE FIXED VERSION)
🔄 Fixed ALL type mismatches and architectural issues
✅ Compatible with QueryExecutor::new() returning Arc<QueryExecutor>
✅ Compatible with execute_query() returning String
✅ Fixed all ownership and borrowing issues
*/

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Fixed imports
use crate::query::{QueryExecutor, QueryResponse};
use crate::security::{SecureQueryExecutor, PolicyEngine, TriggerSystem, UserSummary};
use crate::connection_manager::DatabaseConnectionManager;
use crate::error::{MiniDbError, MiniDbResult};

// ================================
// 1. Core Client Types
// ================================

#[derive(Debug, Clone)]
pub struct SessionToken {
    pub token: String,
    pub username: String,
    pub roles: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub database_path: String,
    pub max_connections: Option<usize>,
    pub timeout: Option<Duration>,
    pub enable_wal: bool,
    pub cache_size: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            database_path: "database.db".to_string(),
            max_connections: Some(10),
            timeout: Some(Duration::from_secs(30)),
            enable_wal: true,
            cache_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionString {
    pub path: String,
    pub options: HashMap<String, String>,
}

impl ConnectionString {
    pub fn parse(connection_string: &str) -> Result<Self, String> {
        let parts: Vec<&str> = connection_string.split('?').collect();
        let path = parts[0].to_string();
        let mut options = HashMap::new();
        
        if parts.len() > 1 {
            for option_pair in parts[1].split('&') {
                let option_parts: Vec<&str> = option_pair.split('=').collect();
                if option_parts.len() == 2 {
                    options.insert(option_parts[0].to_string(), option_parts[1].to_string());
                }
            }
        }
        
        Ok(Self { path, options })
    }
}

// ✅ FIXED: Complete QueryResult with all required fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub message: String,
    pub data: Option<Vec<HashMap<String, String>>>,
    pub affected_rows: u64,
    pub execution_time_ms: f64,
    pub status_code: StatusCode,
    pub operation_type: OperationType,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StatusCode {
    Success = 200,
    Created = 201,
    NoContent = 204,
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    Conflict = 409,
    InternalError = 500,
    NotImplemented = 501,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperationType {
    Query,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    UserManagement,
    Authentication,
    Transaction,
    Administration,
}

impl QueryResult {
    /// Create a successful query result
    pub fn success(
        message: &str, 
        operation_type: OperationType,
        data: Option<Vec<HashMap<String, String>>>,
        affected_rows: u64,
        execution_time_ms: f64
    ) -> Self {
        Self {
            success: true,
            message: message.to_string(),
            data,
            affected_rows,
            execution_time_ms,
            status_code: StatusCode::Success,
            operation_type,
            metadata: HashMap::new(),
        }
    }

    /// Create a created result (for inserts/creates)
    pub fn created(
        message: &str, 
        operation_type: OperationType,
        affected_rows: u64,
        execution_time_ms: f64
    ) -> Self {
        Self {
            success: true,
            message: message.to_string(),
            data: None,
            affected_rows,
            execution_time_ms,
            status_code: StatusCode::Created,
            operation_type,
            metadata: HashMap::new(),
        }
    }

    /// Create an error result
    pub fn error(
        message: &str,
        operation_type: OperationType,
        status_code: StatusCode,
        execution_time_ms: f64
    ) -> Self {
        Self {
            success: false,
            message: message.to_string(),
            data: None,
            affected_rows: 0,
            execution_time_ms,
            status_code,
            operation_type,
            metadata: HashMap::new(),
        }
    }

    /// Create an unauthorized result
    pub fn unauthorized(message: &str, operation_type: OperationType) -> Self {
        Self::error(message, operation_type, StatusCode::Unauthorized, 0.0)
    }

    /// Create a forbidden result
    pub fn forbidden(message: &str, operation_type: OperationType) -> Self {
        Self::error(message, operation_type, StatusCode::Forbidden, 0.0)
    }

    /// Add metadata to the result
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Add multiple metadata entries
    pub fn with_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata.extend(metadata);
        self
    }
}

// ✅ UNIFIED: Single AuditLogEntry definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: String,
    pub user_id: Option<String>,
    pub username: Option<String>,
    pub action: String,
    pub table: String,
    pub record_id: Option<String>,
    pub old_values: Option<HashMap<String, String>>,
    pub new_values: Option<HashMap<String, String>>,
    pub timestamp: DateTime<Utc>,
    pub success: bool,
    pub error_message: Option<String>,
}

// ================================
// Database Information Structure
// ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub path: String,
    pub description: Option<String>,
    pub created_at: Option<String>,
}

// ================================
// 2. Database Client (User-Level)
// ================================

pub struct DatabaseClient {
    secure_executor: Arc<SecureQueryExecutor>,
    session_token: Arc<Mutex<Option<SessionToken>>>,
    config: ConnectionConfig,
}

impl DatabaseClient {
    /// Create new database client
    pub fn new(config: ConnectionConfig) -> Result<Self, String> {
        // Initialize core components using connection manager to prevent lock contention
        let db = DatabaseConnectionManager::global()
            .get_connection(&config.database_path)
            .map_err(|e| e.to_string())?;

        // ✅ FIXED: QueryExecutor::new() already returns Arc<QueryExecutor>
        let query_executor = QueryExecutor::new(db.clone(), config.cache_size, 60);
        let policy_engine = Arc::new(PolicyEngine::new(db.clone()));
        let trigger_system = Arc::new(TriggerSystem::new(db));

        let secure_executor = Arc::new(SecureQueryExecutor::new(
            query_executor,
            policy_engine,
            trigger_system,
        ));

        Ok(Self {
            secure_executor,
            session_token: Arc::new(Mutex::new(None)),
            config,
        })
    }

    /// Connect using connection string
    pub fn connect(connection_string: &str) -> Result<Self, String> {
        let conn_str = ConnectionString::parse(connection_string)?;
        let mut config = ConnectionConfig::default();
        config.database_path = conn_str.path;
        
        // Parse options
        for (key, value) in conn_str.options {
            match key.as_str() {
                "cache_size" => {
                    config.cache_size = value.parse().unwrap_or(100);
                }
                "timeout" => {
                    let timeout_secs: u64 = value.parse().unwrap_or(30);
                    config.timeout = Some(Duration::from_secs(timeout_secs));
                }
                _ => {}
            }
        }
        
        Self::new(config)
    }

    /// Login with username and password
    pub fn login(&self, username: &str, password: &str) -> Result<SessionToken, String> {
        let session_id = self.secure_executor.login(username, password)?;
        
        // FIXED: Get actual user roles from secure executor
        let user_info = self.secure_executor.get_user_by_username(username)
            .map_err(|e| format!("Failed to get user info: {}", e))?;
        
        let token = SessionToken {
            token: session_id,
            username: username.to_string(),
            roles: user_info.roles, // FIXED: Use actual roles from user info
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(24),
        };

        // Store session token
        {
            let mut session_token = self.session_token.lock().unwrap();
            *session_token = Some(token.clone());
        }

        Ok(token)
    }

    /// Logout current session
    pub fn logout(&self) -> Result<(), String> {
        let session_id = {
            let session_token = self.session_token.lock().unwrap();
            session_token.as_ref().map(|t| t.token.clone())
        };

        if let Some(session_id) = session_id {
            self.secure_executor.logout_with_session(&session_id)?;
        }

        // Clear session token
        {
            let mut session_token = self.session_token.lock().unwrap();
            *session_token = None;
        }

        Ok(())
    }

    /// Check if user is authenticated
    pub fn is_authenticated(&self) -> bool {
        let session_token = self.session_token.lock().unwrap();
        session_token.is_some()
    }

    /// ✅ FIXED: Execute query with proper error handling
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, String> {
        if !self.is_authenticated() {
            return Err("Authentication required. Please login first.".to_string());
        }

        let start_time = std::time::Instant::now();

        // Parse the SQL
        let parsed_query = crate::parser::SQLParser::parse_sql(sql)?;
        
        // ✅ FIXED: execute_secure_query returns String, not QueryResponse
        let result_json = self.secure_executor.execute_secure_query(parsed_query, None)?;
        
        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        // ✅ FIXED: Parse JSON string to QueryResponse
        let query_response: QueryResponse = serde_json::from_str(&result_json)
            .map_err(|e| format!("Failed to parse query response: {}", e))?;
        
        Ok(QueryResult {
            success: query_response.status == 200,
            message: query_response.message,
            data: query_response.results,
            affected_rows: query_response.affected_rows as u64,
            execution_time_ms: execution_time,
            status_code: if query_response.status == 200 { StatusCode::Success } else { StatusCode::InternalError },
            operation_type: OperationType::Query,
            metadata: HashMap::new(),
        })
    }

    /// ✅ FIXED: Execute prepared statement
    pub fn execute_prepared(&self, sql: &str, params: HashMap<String, String>) -> Result<QueryResult, String> {
        if !self.is_authenticated() {
            return Err("Authentication required. Please login first.".to_string());
        }

        // Replace parameters in SQL (simple implementation)
        let mut prepared_sql = sql.to_string();
        for (key, value) in params {
            let placeholder = format!("${}", key);
            // Escape single quotes to prevent SQL injection
            let escaped_value = value.replace("'", "''");
            prepared_sql = prepared_sql.replace(&placeholder, &format!("'{}'", escaped_value));
        }

        self.execute_query(&prepared_sql)
    }

    /// Execute multiple queries in a transaction
    pub fn execute_transaction<F>(&self, queries_fn: F) -> Result<Vec<QueryResult>, String>
    where
        F: FnOnce(&Self) -> Result<Vec<String>, String>,
    {
        if !self.is_authenticated() {
            return Err("Authentication required. Please login first.".to_string());
        }

        // Get queries from closure
        let queries = queries_fn(self)?;
        let mut results = Vec::new();

        // Begin transaction
        let tx_id = uuid::Uuid::new_v4().to_string();
        self.secure_executor.begin_transaction(tx_id.clone())?;

        // Execute all queries
        for sql in queries {
            match self.execute_query(&sql) {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Rollback on error
                    let _ = self.secure_executor.rollback_transaction(tx_id);
                    return Err(format!("Transaction failed: {}", e));
                }
            }
        }

        // Commit transaction
        self.secure_executor.commit_transaction(tx_id)?;
        Ok(results)
    }

    /// FIXED: Get current session information
    pub fn get_session_info(&self) -> Option<SessionToken> {
        let session_token = self.session_token.lock().unwrap();
        session_token.clone()
    }
}

// ================================
// 3. Administrative Client
// ================================

pub struct AdminClient {
    secure_executor: Arc<SecureQueryExecutor>,
    master_key: String,
    authenticated: bool,
    current_database: String,
    database_path: String,
}

impl AdminClient {
    /// Create admin client with master key
    pub fn new(database_path: &str, master_key: &str) -> Result<Self, String> {
        // Initialize core components using connection manager to prevent lock contention
        let db = DatabaseConnectionManager::global()
            .get_connection(database_path)
            .map_err(|e| e.to_string())?;

        // ✅ FIXED: QueryExecutor::new() already returns Arc<QueryExecutor>
        let query_executor = QueryExecutor::new(db.clone(), 100, 60);
        let policy_engine = Arc::new(PolicyEngine::new(db.clone()));
        let trigger_system = Arc::new(TriggerSystem::new(db));

        let secure_executor = Arc::new(SecureQueryExecutor::new(
            query_executor,
            policy_engine,
            trigger_system,
        ));

        Ok(Self {
            secure_executor,
            master_key: master_key.to_string(),
            authenticated: false,
            current_database: "default".to_string(),
            database_path: database_path.to_string(),
        })
    }

    /// Authenticate with master key
    pub fn authenticate(&mut self, provided_key: &str) -> Result<(), String> {
        if provided_key != self.master_key {
            return Err("Invalid master key".to_string());
        }
        
        // Set admin context in SecureQueryExecutor
        self.secure_executor.set_admin_context(provided_key)?;
        
        self.authenticated = true;
        Ok(())
    }

    /// Create new user (AdminClient bypasses security context since it has master key)
    pub fn create_user(&self, username: &str, email: &str, password: &str, roles: Vec<String>) -> Result<String, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        // AdminClient with master key can create users directly via PolicyEngine
        // This bypasses the SecureQueryExecutor's context requirement
        if roles.contains(&"admin".to_string()) {
            self.secure_executor.create_admin_user(username, email, password)
        } else {
            // For regular users, use the admin-privileged method
            self.secure_executor.create_user_as_admin(username, email, password, roles)
        }
    }

    /// Delete user
    pub fn delete_user(&self, user_id: &str) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.delete_user(user_id)
    }

    /// Update user roles
    pub fn update_user_roles(&self, user_id: &str, roles: Vec<String>) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.update_user_roles(user_id, roles)
    }

    /// Get database statistics
    pub fn get_database_stats(&self) -> Result<DatabaseStats, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.get_database_stats()
    }

    /// Get security logs
    pub fn get_security_logs(&self, limit: Option<usize>) -> Result<Vec<SecurityLogEntry>, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.get_security_logs(limit)
    }

    /// Execute admin query without user restrictions
    pub fn execute_admin_query(&self, sql: &str) -> Result<QueryResult, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        let start_time = std::time::Instant::now();

        // Parse the SQL
        let parsed_query = crate::parser::SQLParser::parse_sql(sql)?;
        
        // Execute with admin bypass (bypasses security checks)
        let result_json = self.secure_executor.execute_admin_query(parsed_query, None)?;
        
        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        // Parse JSON response
        let query_response: QueryResponse = serde_json::from_str(&result_json)
            .map_err(|e| format!("Failed to parse query response: {}", e))?;
        
        Ok(QueryResult {
            success: query_response.status == 200,
            message: query_response.message,
            data: query_response.results,
            affected_rows: 0,
            execution_time_ms: execution_time,
            status_code: if query_response.status == 200 { StatusCode::Success } else { StatusCode::InternalError },
            operation_type: OperationType::Administration,
            metadata: HashMap::new(),
        })
    }

    /// FIXED: Get system statistics (alias for get_database_stats for compatibility)
    pub fn get_system_stats(&self) -> Result<DatabaseStats, String> {
        self.get_database_stats()
    }

    /// FIXED: Reset user password
    pub fn reset_user_password(&self, username: &str, new_password: &str) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        // Get user info to get user_id
        let user_info = self.secure_executor.get_user_by_username(username)?;
        
        // In a real implementation, this would use admin privileges to reset password
        // For now, we'll use a placeholder old password since admin can reset any password
        self.secure_executor.change_password(&user_info.id, "admin_reset", new_password)
    }

    /// FIXED: Get audit logs (compatibility wrapper for get_security_logs)
    pub fn get_audit_logs(&self, table_filter: Option<&str>, limit: Option<usize>) -> Result<Vec<SecurityLogEntry>, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        // Get security logs and filter by table if specified
        let logs = self.secure_executor.get_security_logs(limit)?;
        
        if let Some(table) = table_filter {
            Ok(logs.into_iter()
                .filter(|log| log.resource.contains(table))
                .collect())
        } else {
            Ok(logs)
        }
    }

    /// FIXED: Create database backup
    pub fn backup_database(&self, backup_path: &str) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.create_security_backup(backup_path)
    }

    /// FIXED: List all users
    pub fn list_users(&self) -> Result<Vec<UserSummary>, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }

        self.secure_executor.list_users()
    }

    // 🗄️ MULTI-DATABASE MANAGEMENT METHODS
    
    /// Get current database name
    pub fn get_current_database(&self) -> &str {
        &self.current_database
    }
    
    /// Switch to a different database
    pub fn switch_database(&mut self, database_name: &str) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }
        
        // Execute USE DATABASE command
        let query = format!("USE DATABASE {}", database_name);
        self.execute_admin_query(&query)?;
        
        // Update current database
        self.current_database = database_name.to_string();
        
        // Reinitialize connection to new database
        let new_db_path = format!("{}.db", database_name);
        let db = DatabaseConnectionManager::global()
            .get_connection(&new_db_path)
            .map_err(|e| e.to_string())?;
        
        // Create new secure executor for the new database
        let query_executor = QueryExecutor::new(db.clone(), 100, 60);
        let policy_engine = Arc::new(PolicyEngine::new(db.clone()));
        let trigger_system = Arc::new(TriggerSystem::new(db));
        
        self.secure_executor = Arc::new(SecureQueryExecutor::new(
            query_executor,
            policy_engine,
            trigger_system,
        ));
        
        // Re-authenticate with new executor
        self.secure_executor.set_admin_context(&self.master_key)?;
        
        Ok(())
    }
    
    /// List all available databases
    pub fn list_databases(&self) -> Result<Vec<DatabaseInfo>, String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }
        
        let result = self.execute_admin_query("SHOW DATABASES")?;
        
        // Parse the response to extract database information
        if let Some(data) = &result.data {
            let databases: Vec<DatabaseInfo> = data
                .iter()
                .filter_map(|row| {
                    Some(DatabaseInfo {
                        name: row.get("Database").unwrap_or(&"Unknown".to_string()).clone(),
                        path: row.get("Path").unwrap_or(&"Unknown".to_string()).clone(),
                        description: row.get("Description").cloned(),
                        created_at: row.get("Created").cloned(),
                    })
                })
                .collect();
            return Ok(databases);
        }
        
        // Fallback to empty list if parsing fails
        Ok(vec![])
    }
    
    /// Create a new database
    pub fn create_database(&self, name: &str, description: Option<&str>) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }
        
        let query = match description {
            Some(desc) => format!("CREATE DATABASE {} DESCRIPTION '{}'", name, desc),
            None => format!("CREATE DATABASE {}", name),
        };
        
        self.execute_admin_query(&query)?;
        Ok(())
    }
    
    /// Delete a database
    pub fn delete_database(&self, name: &str) -> Result<(), String> {
        if !self.authenticated {
            return Err("Authentication required".to_string());
        }
        
        // Prevent deletion of system database
        if name == "mini_db_system" {
            return Err("Cannot delete system database".to_string());
        }
        
        // Prevent deletion of current database
        if name == self.current_database {
            return Err("Cannot delete current database. Switch to another database first.".to_string());
        }
        
        let query = format!("DROP DATABASE {}", name);
        self.execute_admin_query(&query)?;
        Ok(())
    }
}

// ================================
// 4. Test Client
// ================================

pub struct TestClient {
    query_executor: Arc<QueryExecutor>,
    in_memory: bool,
    test_db_path: Option<String>,
}

impl TestClient {
    /// Create in-memory test client
    pub fn new_in_memory() -> Result<Self, String> {
        let config = sled::Config::new().temporary(true);
        let db = Arc::new(
            config.open()
                .map_err(|e| format!("Failed to create in-memory database: {}", e))?
        );
        
        // ✅ FIXED: QueryExecutor::new already returns Arc<QueryExecutor>
        let query_executor = QueryExecutor::new(db, 50, 30);

        Ok(Self {
            query_executor,
            in_memory: true,
            test_db_path: None,
        })
    }

    /// Create test client with temporary file
    pub fn new_temporary() -> Result<Self, String> {
        // Use connection manager for temporary connections
        let db = DatabaseConnectionManager::global()
            .get_temp_connection()
            .map_err(|e| e.to_string())?;
        
        // ✅ FIXED: QueryExecutor::new already returns Arc<QueryExecutor>
        let query_executor = QueryExecutor::new(db, 50, 30);

        Ok(Self {
            query_executor,
            in_memory: false,
            test_db_path: None, // Connection manager handles path internally
        })
    }

    /// ✅ FIXED: Execute test query without security checks
    pub fn execute_test_query(&self, sql: &str) -> Result<QueryResult, String> {
        let start_time = std::time::Instant::now();
        
        // Parse SQL
        let parsed_query = crate::parser::parse_sql(sql)
            .map_err(|e| format!("SQL parsing error: {}", e))?;

        // ✅ FIXED: execute_query returns String, not QueryResponse
        let result_json = self.query_executor.execute_query(&parsed_query, None)?;
        
        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        // ✅ FIXED: Parse JSON string to get QueryResponse
        let query_response: QueryResponse = serde_json::from_str(&result_json)
            .map_err(|e| format!("Failed to parse query response: {}", e))?;

        Ok(QueryResult {
            success: query_response.status == 200,
            message: query_response.message,
            data: query_response.results,
            affected_rows: 0,
            execution_time_ms: execution_time,
            status_code: if query_response.status == 200 { StatusCode::Success } else { StatusCode::InternalError },
            operation_type: OperationType::Administration,
            metadata: HashMap::new(),
        })
    }

    /// Setup test schema
    pub fn setup_test_schema(&self) -> Result<(), String> {
        let queries = vec![
            "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, email TEXT)",
            "CREATE TABLE posts (id TEXT PRIMARY KEY, user_id TEXT, title TEXT, content TEXT)",
            "CREATE TABLE comments (id TEXT PRIMARY KEY, post_id TEXT, user_id TEXT, content TEXT)",
        ];

        for query in queries {
            self.execute_test_query(query)?;
        }

        Ok(())
    }

    /// Insert test data
    pub fn insert_test_data(&self) -> Result<(), String> {
        let queries = vec![
            "INSERT INTO users (id, name, email) VALUES ('1', 'Alice', 'alice@example.com')",
            "INSERT INTO users (id, name, email) VALUES ('2', 'Bob', 'bob@example.com')",
            "INSERT INTO posts (id, user_id, title, content) VALUES ('1', '1', 'First Post', 'Hello World')",
            "INSERT INTO posts (id, user_id, title, content) VALUES ('2', '2', 'Second Post', 'Testing')",
            "INSERT INTO comments (id, post_id, user_id, content) VALUES ('1', '1', '2', 'Great post!')",
        ];

        for query in queries {
            self.execute_test_query(query)?;
        }

        Ok(())
    }

    /// Cleanup test database
    pub fn cleanup(&self) -> Result<(), String> {
        if let Some(path) = &self.test_db_path {
            if std::path::Path::new(path).exists() {
                std::fs::remove_file(path)
                    .map_err(|e| format!("Failed to cleanup test database: {}", e))?;
            }
        }
        Ok(())
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

pub struct ClientFactory;

impl ClientFactory {
    /// Create production client with default configuration
    pub fn create_production_client(database_path: &str) -> Result<DatabaseClient, String> {
        DatabaseClient::connect(database_path)
    }

    /// Create admin client with environment-based master key
    pub fn create_admin_client(database_path: &str) -> Result<AdminClient, String> {
        let master_key = std::env::var("MINI_DB_MASTER_KEY")
            .unwrap_or_else(|_| "default_master_key".to_string());
        AdminClient::new(database_path, &master_key)
    }

    /// Create test client based on environment
    pub fn create_test_client() -> Result<TestClient, String> {
        if std::env::var("TEST_USE_MEMORY").unwrap_or_default() == "true" {
            TestClient::new_in_memory()
        } else {
            TestClient::new_temporary()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Unhealthy(String),
    Warning(String),
}

pub struct HealthCheck;

impl HealthCheck {
    /// Check if production client is healthy
    pub fn check_database_client(client: &DatabaseClient) -> HealthStatus {
        if !client.is_authenticated() {
            return HealthStatus::Warning("Client not authenticated".to_string());
        }

        match client.execute_query("SELECT 1") {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => HealthStatus::Unhealthy(format!("Query failed: {}", e)),
        }
    }

    /// Check if admin client is healthy  
    pub fn check_admin_client(client: &AdminClient) -> HealthStatus {
        if !client.authenticated {
            return HealthStatus::Unhealthy("Admin not authenticated".to_string());
        }

        match client.execute_admin_query("SELECT 1") {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => HealthStatus::Unhealthy(format!("Admin query failed: {}", e)),
        }
    }

    /// Check if test client is healthy
    pub fn check_test_client(client: &TestClient) -> HealthStatus {
        match client.execute_test_query("SELECT 1") {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => HealthStatus::Unhealthy(format!("Test query failed: {}", e)),
        }
    }
}

pub struct MigrationHelper;

impl MigrationHelper {
    /// Migrate from direct QueryExecutor usage to DatabaseClient
    pub fn migrate_query_executor_usage() -> &'static str {
        r#"
Migration Guide: QueryExecutor → DatabaseClient

OLD CODE:
```rust
let db = Arc::new(sled::open("my_db").unwrap());
let query_executor = QueryExecutor::new(db, 100, 60);
let result = query_executor.execute_query(parsed_query, None)?;
```

NEW CODE:
```rust
let client = DatabaseClient::connect("my_db")?;
client.login("username", "password")?;
let result = client.execute_query("SELECT * FROM users")?;
```
        "#
    }

    /// Get example migration scripts
    pub fn get_migration_examples() -> Vec<MigrationExample> {
        vec![
            MigrationExample {
                name: "Basic Query Migration".to_string(),
                old_code: "// Old QueryExecutor code".to_string(),
                new_code: "// New DatabaseClient code".to_string(),
            },
        ]
    }
}

#[derive(Debug, Clone)]
pub struct MigrationExample {
    pub name: String,
    pub old_code: String,
    pub new_code: String,
}

// ================================
// 5. Supporting Types
// ================================

// Re-export from secure_executor
pub use crate::security::secure_executor::DatabaseStats;
pub use crate::security::policy_engine::SecurityLogEntry;