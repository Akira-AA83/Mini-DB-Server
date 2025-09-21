/*
📌 Phase 5.4: Structured Error Handling - COMPREHENSIVE ERROR SYSTEM
🚨 Unified error types for better debugging and user experience
✅ Structured error categories with actionable messages
✅ Error context and source tracking
✅ Integration with existing codebase
*/

use std::fmt;
use serde::{Deserialize, Serialize};

/// Comprehensive error type for Mini-DB operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MiniDbError {
    // Database-level errors
    Database {
        operation: String,
        message: String,
        path: Option<String>,
    },
    
    // Query-related errors
    Query {
        query_type: QueryType,
        message: String,
        sql: Option<String>,
        line: Option<usize>,
        column: Option<usize>,
    },
    
    // Schema-related errors
    Schema {
        schema_operation: SchemaOperation,
        table: Option<String>,
        column: Option<String>,
        message: String,
    },
    
    // Security and authentication errors
    Security {
        security_type: SecurityErrorType,
        user_id: Option<String>,
        resource: Option<String>,
        message: String,
    },
    
    // Transaction errors
    Transaction {
        transaction_id: Option<String>,
        operation: TransactionOperation,
        message: String,
    },
    
    // Connection and networking errors
    Connection {
        connection_type: ConnectionType,
        endpoint: Option<String>,
        message: String,
    },
    
    // Module system errors
    Module {
        module_name: Option<String>,
        operation: ModuleOperation,
        message: String,
    },
    
    // Configuration errors
    Configuration {
        config_field: String,
        expected: String,
        actual: String,
        message: String,
    },
    
    // Internal system errors
    Internal {
        component: String,
        message: String,
        suggestion: Option<String>,
    },
    
    // Validation errors
    Validation {
        field: String,
        value: String,
        constraint: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOperation {
    Create,
    Alter,
    Drop,
    Validate,
    Migration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityErrorType {
    Authentication,
    Authorization,
    PermissionDenied,
    SessionExpired,
    InvalidCredentials,
    AccessDenied,
    PolicyViolation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionOperation {
    Begin,
    Commit,
    Rollback,
    Isolation,
    Deadlock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionType {
    Database,
    Network,
    Pool,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModuleOperation {
    Load,
    Execute,
    Initialize,
    Compile,
}

impl MiniDbError {
    /// Create a database error
    pub fn database(operation: &str, message: &str, path: Option<&str>) -> Self {
        Self::Database {
            operation: operation.to_string(),
            message: message.to_string(),
            path: path.map(|s| s.to_string()),
        }
    }
    
    /// Create a query error
    pub fn query(query_type: QueryType, message: &str, sql: Option<&str>) -> Self {
        Self::Query {
            query_type,
            message: message.to_string(),
            sql: sql.map(|s| s.to_string()),
            line: None,
            column: None,
        }
    }
    
    /// Create a query error with position
    pub fn query_with_position(
        query_type: QueryType, 
        message: &str, 
        sql: Option<&str>,
        line: usize,
        column: usize
    ) -> Self {
        Self::Query {
            query_type,
            message: message.to_string(),
            sql: sql.map(|s| s.to_string()),
            line: Some(line),
            column: Some(column),
        }
    }
    
    /// Create a schema error
    pub fn schema(
        operation: SchemaOperation, 
        message: &str, 
        table: Option<&str>, 
        column: Option<&str>
    ) -> Self {
        Self::Schema {
            schema_operation: operation,
            table: table.map(|s| s.to_string()),
            column: column.map(|s| s.to_string()),
            message: message.to_string(),
        }
    }
    
    /// Create a security error
    pub fn security(
        security_type: SecurityErrorType, 
        message: &str, 
        user_id: Option<&str>,
        resource: Option<&str>
    ) -> Self {
        Self::Security {
            security_type,
            user_id: user_id.map(|s| s.to_string()),
            resource: resource.map(|s| s.to_string()),
            message: message.to_string(),
        }
    }
    
    /// Create a transaction error
    pub fn transaction(
        operation: TransactionOperation, 
        message: &str, 
        transaction_id: Option<&str>
    ) -> Self {
        Self::Transaction {
            transaction_id: transaction_id.map(|s| s.to_string()),
            operation,
            message: message.to_string(),
        }
    }
    
    /// Create a connection error
    pub fn connection(
        connection_type: ConnectionType, 
        message: &str, 
        endpoint: Option<&str>
    ) -> Self {
        Self::Connection {
            connection_type,
            endpoint: endpoint.map(|s| s.to_string()),
            message: message.to_string(),
        }
    }
    
    /// Create a module error
    pub fn module(
        operation: ModuleOperation, 
        message: &str, 
        module_name: Option<&str>
    ) -> Self {
        Self::Module {
            module_name: module_name.map(|s| s.to_string()),
            operation,
            message: message.to_string(),
        }
    }
    
    /// Create a configuration error
    pub fn configuration(
        field: &str, 
        expected: &str, 
        actual: &str, 
        message: &str
    ) -> Self {
        Self::Configuration {
            config_field: field.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
            message: message.to_string(),
        }
    }
    
    /// Create an internal error
    pub fn internal(component: &str, message: &str, suggestion: Option<&str>) -> Self {
        Self::Internal {
            component: component.to_string(),
            message: message.to_string(),
            suggestion: suggestion.map(|s| s.to_string()),
        }
    }
    
    /// Create a validation error
    pub fn validation(field: &str, value: &str, constraint: &str, message: &str) -> Self {
        Self::Validation {
            field: field.to_string(),
            value: value.to_string(),
            constraint: constraint.to_string(),
            message: message.to_string(),
        }
    }
    
    /// Get error code for programmatic handling
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Database { .. } => "DB_ERROR",
            Self::Query { .. } => "QUERY_ERROR",
            Self::Schema { .. } => "SCHEMA_ERROR",
            Self::Security { .. } => "SECURITY_ERROR",
            Self::Transaction { .. } => "TRANSACTION_ERROR",
            Self::Connection { .. } => "CONNECTION_ERROR",
            Self::Module { .. } => "MODULE_ERROR",
            Self::Configuration { .. } => "CONFIG_ERROR",
            Self::Internal { .. } => "INTERNAL_ERROR",
            Self::Validation { .. } => "VALIDATION_ERROR",
        }
    }
    
    /// Get severity level for logging
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::Database { .. } => ErrorSeverity::High,
            Self::Query { .. } => ErrorSeverity::Medium,
            Self::Schema { .. } => ErrorSeverity::Medium,
            Self::Security { .. } => ErrorSeverity::High,
            Self::Transaction { .. } => ErrorSeverity::Medium,
            Self::Connection { .. } => ErrorSeverity::High,
            Self::Module { .. } => ErrorSeverity::Low,
            Self::Configuration { .. } => ErrorSeverity::High,
            Self::Internal { .. } => ErrorSeverity::Critical,
            Self::Validation { .. } => ErrorSeverity::Low,
        }
    }
    
    /// Check if error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Database { .. } => false,
            Self::Query { .. } => true,
            Self::Schema { .. } => true,
            Self::Security { security_type, .. } => matches!(
                security_type, 
                SecurityErrorType::SessionExpired | SecurityErrorType::InvalidCredentials
            ),
            Self::Transaction { operation, .. } => matches!(
                operation, 
                TransactionOperation::Rollback | TransactionOperation::Deadlock
            ),
            Self::Connection { .. } => true,
            Self::Module { .. } => true,
            Self::Configuration { .. } => false,
            Self::Internal { .. } => false,
            Self::Validation { .. } => true,
        }
    }
    
    /// Get user-friendly error message
    pub fn user_message(&self) -> String {
        match self {
            Self::Database { operation, message, path } => {
                format!("Database operation '{}' failed: {}{}",
                    operation, 
                    message,
                    path.as_ref().map(|p| format!(" (Path: {})", p)).unwrap_or_default()
                )
            },
            Self::Query { query_type, message, line, column, .. } => {
                format!("Query error in {:?}: {}{}",
                    query_type,
                    message,
                    match (line, column) {
                        (Some(l), Some(c)) => format!(" at line {}, column {}", l, c),
                        _ => String::new(),
                    }
                )
            },
            Self::Schema { schema_operation, table, column, message } => {
                format!("Schema {} error: {}{}{}",
                    match schema_operation {
                        SchemaOperation::Create => "creation",
                        SchemaOperation::Alter => "modification",
                        SchemaOperation::Drop => "deletion",
                        SchemaOperation::Validate => "validation",
                        SchemaOperation::Migration => "migration",
                    },
                    message,
                    table.as_ref().map(|t| format!(" (Table: {})", t)).unwrap_or_default(),
                    column.as_ref().map(|c| format!(" (Column: {})", c)).unwrap_or_default()
                )
            },
            Self::Security { security_type, message, resource, .. } => {
                format!("{}: {}{}",
                    match security_type {
                        SecurityErrorType::Authentication => "Authentication failed",
                        SecurityErrorType::Authorization => "Authorization denied",
                        SecurityErrorType::PermissionDenied => "Permission denied",
                        SecurityErrorType::SessionExpired => "Session expired",
                        SecurityErrorType::InvalidCredentials => "Invalid credentials",
                        SecurityErrorType::AccessDenied => "Access denied",
                        SecurityErrorType::PolicyViolation => "Security policy violation",
                    },
                    message,
                    resource.as_ref().map(|r| format!(" (Resource: {})", r)).unwrap_or_default()
                )
            },
            Self::Transaction { operation, message, transaction_id } => {
                format!("Transaction {} failed: {}{}",
                    match operation {
                        TransactionOperation::Begin => "start",
                        TransactionOperation::Commit => "commit",
                        TransactionOperation::Rollback => "rollback",
                        TransactionOperation::Isolation => "isolation",
                        TransactionOperation::Deadlock => "deadlock resolution",
                    },
                    message,
                    transaction_id.as_ref().map(|id| format!(" (ID: {})", id)).unwrap_or_default()
                )
            },
            Self::Connection { connection_type, message, endpoint } => {
                format!("{} connection error: {}{}",
                    match connection_type {
                        ConnectionType::Database => "Database",
                        ConnectionType::Network => "Network",
                        ConnectionType::Pool => "Connection pool",
                        ConnectionType::Timeout => "Timeout",
                    },
                    message,
                    endpoint.as_ref().map(|e| format!(" (Endpoint: {})", e)).unwrap_or_default()
                )
            },
            Self::Module { operation, message, module_name } => {
                format!("Module {} failed: {}{}",
                    match operation {
                        ModuleOperation::Load => "loading",
                        ModuleOperation::Execute => "execution",
                        ModuleOperation::Initialize => "initialization",
                        ModuleOperation::Compile => "compilation",
                    },
                    message,
                    module_name.as_ref().map(|n| format!(" (Module: {})", n)).unwrap_or_default()
                )
            },
            Self::Configuration { config_field, expected, actual, message } => {
                format!("Configuration error in '{}': {} (Expected: {}, Got: {})",
                    config_field, message, expected, actual
                )
            },
            Self::Internal { component, message, suggestion } => {
                format!("Internal error in {}: {}{}",
                    component,
                    message,
                    suggestion.as_ref().map(|s| format!(" (Suggestion: {})", s)).unwrap_or_default()
                )
            },
            Self::Validation { field, value, constraint, message } => {
                format!("Validation failed for field '{}': {} (Value: '{}', Constraint: {})",
                    field, message, value, constraint
                )
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for MiniDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.error_code(), self.user_message())
    }
}

impl std::error::Error for MiniDbError {}

/// Result type alias for Mini-DB operations
pub type MiniDbResult<T> = Result<T, MiniDbError>;

/// Convert from sled::Error to MiniDbError
impl From<sled::Error> for MiniDbError {
    fn from(err: sled::Error) -> Self {
        MiniDbError::database("sled_operation", &err.to_string(), None)
    }
}

/// Convert from serde_json::Error to MiniDbError
impl From<serde_json::Error> for MiniDbError {
    fn from(err: serde_json::Error) -> Self {
        MiniDbError::internal("json_serialization", &err.to_string(), Some("Check data structure compatibility"))
    }
}

/// Convert from std::io::Error to MiniDbError
impl From<std::io::Error> for MiniDbError {
    fn from(err: std::io::Error) -> Self {
        MiniDbError::connection(ConnectionType::Database, &err.to_string(), None)
    }
}

/// Convert from String to MiniDbError (for legacy compatibility)
impl From<String> for MiniDbError {
    fn from(err: String) -> Self {
        MiniDbError::internal("legacy_string_error", &err, Some("Update to use structured error types"))
    }
}

/// Convert from &str to MiniDbError (for legacy compatibility)
impl From<&str> for MiniDbError {
    fn from(err: &str) -> Self {
        MiniDbError::internal("legacy_str_error", err, Some("Update to use structured error types"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let db_error = MiniDbError::database("open", "File not found", Some("/path/to/db"));
        assert_eq!(db_error.error_code(), "DB_ERROR");
        assert_eq!(db_error.severity() as u8, ErrorSeverity::High as u8);
        assert!(!db_error.is_recoverable());
    }

    #[test]
    fn test_query_error_with_position() {
        let query_error = MiniDbError::query_with_position(
            QueryType::Select,
            "Invalid column name",
            Some("SELECT invalid_col FROM users"),
            1,
            15
        );
        
        let message = query_error.user_message();
        assert!(message.contains("line 1, column 15"));
    }

    #[test]
    fn test_security_error() {
        let sec_error = MiniDbError::security(
            SecurityErrorType::PermissionDenied,
            "User lacks admin privileges",
            Some("user123"),
            Some("admin_panel")
        );
        
        assert!(sec_error.user_message().contains("Permission denied"));
        assert!(sec_error.user_message().contains("Resource: admin_panel"));
    }

    #[test]
    fn test_error_conversions() {
        let string_error: MiniDbError = "Test error".into();
        assert_eq!(string_error.error_code(), "INTERNAL_ERROR");
        
        let owned_string_error: MiniDbError = "Test error".to_string().into();
        assert_eq!(owned_string_error.error_code(), "INTERNAL_ERROR");
    }
}