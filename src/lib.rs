/*
📌 File: src/lib.rs (COMPLETE FIXED)
🔄 Fixed incomplete exports and missing client types
✅ All exports properly defined
*/

// ================================
// Core Database Modules
// ================================
pub mod storage;
pub mod schema;
pub mod parser;
pub mod query;
pub mod transaction;
pub mod modules;
pub mod security;
pub mod join_engine;
pub mod connection_manager;
pub mod error;
#[cfg(feature = "websocket")]
pub mod sync;

// ================================
// NEW: Client APIs Module
// ================================
pub mod client;

// ================================
// WASM Module System (Always Enabled)
// ================================
pub mod wasm;

// ================================
// Internal Types (Legacy Support)
// ================================
pub use storage::Storage;
pub use schema::{TableSchema, DataType, Constraint};
pub use parser::ParsedQuery;
pub use query::{QueryExecutor, QueryResponse};
pub use transaction::TransactionManager;
pub use modules::{Module, ModuleManager, ModuleContext};
pub use join_engine::JoinExecutor;
#[cfg(feature = "websocket")]
pub use sync::SyncServer;

// Security types
pub use security::{
    PolicyEngine, TriggerSystem, SecureQueryExecutor, 
    PasswordSecurityStats, RowLevelPolicy, SecurityEvent, SecurityEventType,
    TriggerBuilder, Trigger, UserInfo, UserSummary, SecurityLogEntry
};

// ================================
// NEW: Public Client APIs (Recommended)
// ================================
pub use client::{
    DatabaseClient,    // Production database client
    AdminClient,       // Administrative operations
    TestClient,        // Testing utilities
    ConnectionConfig,  // Connection configuration
    ConnectionString,  // Connection string parser
    SessionToken,      // Authentication token
    QueryResult,       // Query result wrapper
};