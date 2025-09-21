/*
📌 Module System & Event System
✅ WASM-based stored procedures
✅ Event-driven database reactions
✅ Trigger system integration
✅ Custom business logic execution
✅ Security system compatibility
*/

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[cfg(feature = "wasm")]
use wasmtime::*;
#[cfg(feature = "wasm")]
use crate::wasm::WasmEngine;

// ================================
// Event System
// ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseEvent {
    RowInserted { 
        table: String, 
        row: HashMap<String, String>,
        timestamp: chrono::DateTime<chrono::Utc>,
        tx_id: Option<String>,
    },
    RowUpdated { 
        table: String, 
        old_row: HashMap<String, String>,
        new_row: HashMap<String, String>,
        timestamp: chrono::DateTime<chrono::Utc>,
        tx_id: Option<String>,
    },
    RowDeleted { 
        table: String, 
        row: HashMap<String, String>,
        timestamp: chrono::DateTime<chrono::Utc>,
        tx_id: Option<String>,
    },
    TransactionCommitted {
        tx_id: String,
        tables_affected: Vec<String>,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    TransactionRolledBack {
        tx_id: String,
        reason: String,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSubscription {
    pub id: String,
    pub module_name: String,
    pub event_types: Vec<EventType>,
    pub table_filter: Option<String>, // None = all tables
    pub condition: Option<String>,    // SQL-like condition
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Insert,
    Update,
    Delete,
    TransactionCommit,
    TransactionRollback,
}

// ================================
// Module System
// ================================

pub trait Module: Send + Sync {
    /// Called when a row is inserted
    fn on_insert(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String>;
    
    /// Called when a row is updated
    fn on_update(&self, ctx: &ModuleContext, table: &str, old_row: &HashMap<String, String>, new_row: &HashMap<String, String>) -> Result<ModuleResponse, String>;
    
    /// Called when a row is deleted
    fn on_delete(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String>;
    
    /// Custom reducer function (like stored procedures)
    fn reducer(&self, ctx: &ModuleContext, name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value, String>;
    
    /// Called when transaction commits
    fn on_transaction_commit(&self, ctx: &ModuleContext, tx_id: &str, tables: &[String]) -> Result<ModuleResponse, String>;
    
    /// Module initialization
    fn init(&self, ctx: &ModuleContext) -> Result<(), String>;
    
    /// Module name
    fn name(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct ModuleContext {
    pub db: Arc<sled::Db>,
    pub event_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub user_context: Option<HashMap<String, String>>,
    pub sender_address: Option<String>,
    pub transaction_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleResponse {
    pub success: bool,
    pub message: Option<String>,
    pub data: Option<serde_json::Value>,
    pub side_effects: Vec<SideEffect>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SideEffect {
    InsertRow { table: String, values: HashMap<String, String> },
    UpdateRow { table: String, key: String, values: HashMap<String, String> },
    DeleteRow { table: String, key: String },
    SendNotification { channel: String, message: String },
    LogEvent { level: String, message: String },
    CallReducer { module: String, function: String, args: Vec<serde_json::Value> },
    // NEW: Additional side effects for database operations
    DatabaseWrite { table: String, data: HashMap<String, String> },
    DatabaseRead { table: String, conditions: HashMap<String, String> },
}

// ================================
// WASM Module Implementation
// ================================

#[cfg(feature = "wasm")]
pub struct WasmModule {
    name: String,
    engine: Arc<Mutex<WasmEngine>>,
    module_bytes: Vec<u8>,
}

#[cfg(feature = "wasm")]
impl WasmModule {
    pub fn new(name: String, wasm_bytes: Vec<u8>) -> Result<Self, String> {
        let wasm_engine = WasmEngine::new().map_err(|e| e.to_string())?;
        let engine = Arc::new(Mutex::new(wasm_engine));
        engine.lock().unwrap().register_module(&name, &wasm_bytes)?;
        
        Ok(Self {
            name,
            engine,
            module_bytes: wasm_bytes,
        })
    }
}

#[cfg(feature = "wasm")]
impl Module for WasmModule {
    fn on_insert(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        let input = serde_json::json!({
            "event": "insert",
            "table": table,
            "row": row,
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("on_insert", input)
    }

    fn on_update(&self, ctx: &ModuleContext, table: &str, old_row: &HashMap<String, String>, new_row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        let input = serde_json::json!({
            "event": "update",
            "table": table,
            "old_row": old_row,
            "new_row": new_row,
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("on_update", input)
    }

    fn on_delete(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        let input = serde_json::json!({
            "event": "delete",
            "table": table,
            "row": row,
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("on_delete", input)
    }

    fn reducer(&self, ctx: &ModuleContext, name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value, String> {
        let input = serde_json::json!({
            "function": name,
            "args": args,
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("reducer", input)
            .map(|response| response.data.unwrap_or(serde_json::Value::Null))
    }

    fn on_transaction_commit(&self, ctx: &ModuleContext, tx_id: &str, tables: &[String]) -> Result<ModuleResponse, String> {
        let input = serde_json::json!({
            "event": "transaction_commit",
            "tx_id": tx_id,
            "tables": tables,
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("on_transaction_commit", input)
    }

    fn init(&self, ctx: &ModuleContext) -> Result<(), String> {
        let input = serde_json::json!({
            "event": "init",
            "context": {
                "event_id": ctx.event_id,
                "timestamp": ctx.timestamp,
                "user_context": ctx.user_context
            }
        });
        
        self.execute_wasm_function("init", input)?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(feature = "wasm")]
impl WasmModule {
    fn execute_wasm_function(&self, function: &str, input: serde_json::Value) -> Result<ModuleResponse, String> {
        use crate::wasm::{WasmDataPacket, WasmEngine};
        
        // Convert JSON input to WasmDataPacket for optimized communication
        let data_packet = WasmDataPacket {
            operation: function.to_string(),
            table: input.get("table").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            data: input.get("row")
                .or_else(|| input.get("new_row"))
                .and_then(|v| v.as_object())
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect::<std::collections::HashMap<_, _>>()
                })
                .unwrap_or_default(),
            metadata: Some({
                let mut meta = std::collections::HashMap::new();
                if let Some(event) = input.get("event").and_then(|v| v.as_str()) {
                    meta.insert("event".to_string(), event.to_string());
                }
                if let Some(context) = input.get("context") {
                    meta.insert("context".to_string(), context.to_string());
                }
                meta
            }),
        };
        
        // Call WASM function with optimized memory interface
        let engine = self.engine.lock().unwrap();
        let wasm_result = engine.call_function_optimized(&self.name, function, &data_packet)
            .map_err(|e| format!("WASM execution failed: {}", e))?;
        
        // Parse WASM result back to ModuleResponse
        match serde_json::from_str::<serde_json::Value>(&wasm_result) {
            Ok(result_json) => {
                let success = result_json.get("success").and_then(|v| v.as_bool()).unwrap_or(true);
                let message = result_json.get("message").and_then(|v| v.as_str()).map(|s| s.to_string());
                let data = result_json.get("data").cloned();
                
                Ok(ModuleResponse {
                    success,
                    message,
                    data,
                    side_effects: vec![], // TODO: Parse side_effects from WASM result
                })
            },
            Err(_) => {
                // Fallback: treat as simple result
                Ok(ModuleResponse {
                    success: true,
                    message: Some(format!("WASM function '{}' executed (optimized)", function)),
                    data: Some(serde_json::json!({ "wasm_output": wasm_result })),
                    side_effects: vec![],
                })
            }
        }
    }
}

// ================================
// Rust Native Module Example
// ================================

pub struct AuditModule {
    name: String,
}

impl AuditModule {
    pub fn new() -> Self {
        Self {
            name: "audit_module".to_string(),
        }
    }
}

impl Module for AuditModule {
    fn on_insert(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔍 AUDIT: Row inserted in table '{}' at {}", table, ctx.timestamp);
        println!("📝 Data: {:?}", row);
        
        Ok(ModuleResponse {
            success: true,
            message: Some("Audit logged".to_string()),
            data: None,
            side_effects: vec![
                SideEffect::LogEvent {
                    level: "INFO".to_string(),
                    message: format!("INSERT into {} with data: {:?}", table, row),
                }
            ],
        })
    }

    fn on_update(&self, ctx: &ModuleContext, table: &str, old_row: &HashMap<String, String>, new_row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔍 AUDIT: Row updated in table '{}' at {}", table, ctx.timestamp);
        println!("📝 Old: {:?}", old_row);
        println!("📝 New: {:?}", new_row);
        
        Ok(ModuleResponse {
            success: true,
            message: Some("Audit logged".to_string()),
            data: None,
            side_effects: vec![
                SideEffect::LogEvent {
                    level: "INFO".to_string(),
                    message: format!("UPDATE in {} from {:?} to {:?}", table, old_row, new_row),
                }
            ],
        })
    }

    fn on_delete(&self, ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔍 AUDIT: Row deleted from table '{}' at {}", table, ctx.timestamp);
        println!("📝 Data: {:?}", row);
        
        Ok(ModuleResponse {
            success: true,
            message: Some("Audit logged".to_string()),
            data: None,
            side_effects: vec![
                SideEffect::LogEvent {
                    level: "INFO".to_string(),
                    message: format!("DELETE from {} with data: {:?}", table, row),
                }
            ],
        })
    }

    fn reducer(&self, _ctx: &ModuleContext, name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value, String> {
        match name {
            "get_audit_count" => {
                // In a real implementation, this would query audit logs
                Ok(serde_json::json!({ "audit_count": 42 }))
            }
            "get_table_stats" => {
                if let Some(table) = args.get(0).and_then(|v| v.as_str()) {
                    Ok(serde_json::json!({
                        "table": table,
                        "total_operations": 10,
                        "last_activity": chrono::Utc::now()
                    }))
                } else {
                    Err("Missing table parameter".to_string())
                }
            }
            _ => Err(format!("Unknown reducer function: {}", name))
        }
    }

    fn on_transaction_commit(&self, ctx: &ModuleContext, tx_id: &str, tables: &[String]) -> Result<ModuleResponse, String> {
        println!("🔍 AUDIT: Transaction {} committed at {}", tx_id, ctx.timestamp);
        println!("📝 Tables affected: {:?}", tables);
        
        Ok(ModuleResponse {
            success: true,
            message: Some("Transaction audit logged".to_string()),
            data: None,
            side_effects: vec![
                SideEffect::LogEvent {
                    level: "INFO".to_string(),
                    message: format!("TRANSACTION {} committed affecting tables: {:?}", tx_id, tables),
                }
            ],
        })
    }

    fn init(&self, _ctx: &ModuleContext) -> Result<(), String> {
        println!("🚀 Audit Module initialized");
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ================================
// Module Manager
// ================================

// Callback type for WebSocket notifications
pub type NotificationCallback = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

pub struct ModuleManager {
    modules: HashMap<String, Box<dyn Module>>,
    subscriptions: Vec<EventSubscription>,
    event_log: Arc<Mutex<Vec<DatabaseEvent>>>,
    // NEW: Callback for WebSocket broadcasting (database, table, message)
    notification_callback: Option<NotificationCallback>,
}

impl ModuleManager {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
            subscriptions: Vec::new(),
            event_log: Arc::new(Mutex::new(Vec::new())),
            notification_callback: None,
        }
    }
    
    /// Set the WebSocket notification callback
    pub fn set_notification_callback(&mut self, callback: NotificationCallback) {
        self.notification_callback = Some(callback);
    }

    /// Register a module
    pub fn register_module(&mut self, module: Box<dyn Module>) -> Result<(), String> {
        let name = module.name().to_string();
        
        // Initialize the module with a temporary context
        let ctx = ModuleContext {
            db: Arc::new(sled::open("temp").unwrap()), // This should be the real DB
            event_id: Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now(),
            user_context: None,
            sender_address: None,
            transaction_id: None,
        };
        
        module.init(&ctx)?;
        
        // Auto-subscribe modules to relevant events
        self.auto_subscribe_module(&name);
        
        self.modules.insert(name.clone(), module);
        println!("📦 Module '{}' registered successfully", name);
        Ok(())
    }
    
    /// Auto-subscribe modules to events based on their type
    fn auto_subscribe_module(&mut self, module_name: &str) {
        match module_name {
            "audit_module" => {
                // AuditModule subscribes to all events
                let subscription = EventSubscription {
                    id: Uuid::new_v4().to_string(),
                    module_name: module_name.to_string(),
                    event_types: vec![EventType::Insert, EventType::Update, EventType::Delete, EventType::TransactionCommit],
                    table_filter: None, // All tables
                    condition: None,
                    active: true,
                };
                self.subscriptions.push(subscription);
                println!("📡 Auto-subscribed AuditModule to all events");
            }
            "realtime_module" => {
                // RealtimeModule subscribes to INSERT and UPDATE events on all tables
                let subscription = EventSubscription {
                    id: Uuid::new_v4().to_string(),
                    module_name: module_name.to_string(),
                    event_types: vec![EventType::Insert, EventType::Update],
                    table_filter: None, // All tables for real-time notifications
                    condition: None,
                    active: true,
                };
                self.subscriptions.push(subscription);
                println!("📡 Auto-subscribed RealtimeModule to INSERT and UPDATE events on all tables");
            }
            _ => {
                // Default subscription for other modules
                let subscription = EventSubscription {
                    id: Uuid::new_v4().to_string(),
                    module_name: module_name.to_string(),
                    event_types: vec![EventType::Insert, EventType::Update, EventType::Delete],
                    table_filter: None,
                    condition: None,
                    active: true,
                };
                self.subscriptions.push(subscription);
                println!("📡 Auto-subscribed {} to default events", module_name);
            }
        }
    }

    /// Subscribe to events
    pub fn subscribe(&mut self, subscription: EventSubscription) {
        println!("📡 Module '{}' subscribed to events: {:?}", 
                 subscription.module_name, subscription.event_types);
        self.subscriptions.push(subscription);
    }

    /// Trigger an event
    pub fn trigger_event(&self, event: DatabaseEvent, db: Arc<sled::Db>) -> Result<Vec<ModuleResponse>, String> {
        // Log the event
        self.event_log.lock().unwrap().push(event.clone());
        
        let mut responses = Vec::new();
        
        // Find matching subscriptions
        for subscription in &self.subscriptions {
            if !subscription.active {
                continue;
            }
            
            if self.event_matches_subscription(&event, subscription) {
                if let Some(module) = self.modules.get(&subscription.module_name) {
                    let ctx = ModuleContext {
                        db: Arc::clone(&db),
                        event_id: Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now(),
                        user_context: None,
                        sender_address: None,
                        transaction_id: None,
                    };
                    
                    let response = match &event {
                        DatabaseEvent::RowInserted { table, row, .. } => {
                            module.on_insert(&ctx, table, row)
                        }
                        DatabaseEvent::RowUpdated { table, old_row, new_row, .. } => {
                            module.on_update(&ctx, table, old_row, new_row)
                        }
                        DatabaseEvent::RowDeleted { table, row, .. } => {
                            module.on_delete(&ctx, table, row)
                        }
                        DatabaseEvent::TransactionCommitted { tx_id, tables_affected, .. } => {
                            module.on_transaction_commit(&ctx, tx_id, tables_affected)
                        }
                        DatabaseEvent::TransactionRolledBack { .. } => {
                            // Handle rollback if needed
                            Ok(ModuleResponse {
                                success: true,
                                message: Some("Rollback handled".to_string()),
                                data: None,
                                side_effects: vec![],
                            })
                        }
                    };
                    
                    match response {
                        Ok(resp) => {
                            println!("✅ Module '{}' executed successfully: {}", 
                                     subscription.module_name, 
                                     resp.message.as_ref().unwrap_or(&"No message".to_string()));
                            println!("🔧 Module response has {} side effects", resp.side_effects.len());
                            responses.push(resp.clone());
                            self.execute_side_effects(&resp.side_effects, &db)?;
                        }
                        Err(e) => {
                            println!("❌ Module '{}' error: {}", subscription.module_name, e);
                        }
                    }
                }
            }
        }
        
        Ok(responses)
    }

    /// Execute reducer function
    pub fn execute_reducer(&self, module_name: &str, function_name: &str, args: &[serde_json::Value], db: Arc<sled::Db>) -> Result<serde_json::Value, String> {
        if let Some(module) = self.modules.get(module_name) {
            let ctx = ModuleContext {
                db,
                event_id: Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now(),
                user_context: None,
                sender_address: None,
                transaction_id: None,
            };
            
            module.reducer(&ctx, function_name, args)
        } else {
            Err(format!("Module '{}' not found", module_name))
        }
    }

    /// Get event log
    pub fn get_event_log(&self) -> Vec<DatabaseEvent> {
        self.event_log.lock().unwrap().clone()
    }

    /// Clear event log
    pub fn clear_event_log(&self) {
        self.event_log.lock().unwrap().clear();
    }

    fn event_matches_subscription(&self, event: &DatabaseEvent, subscription: &EventSubscription) -> bool {
        // Check event type
        let event_type = match event {
            DatabaseEvent::RowInserted { .. } => EventType::Insert,
            DatabaseEvent::RowUpdated { .. } => EventType::Update,
            DatabaseEvent::RowDeleted { .. } => EventType::Delete,
            DatabaseEvent::TransactionCommitted { .. } => EventType::TransactionCommit,
            DatabaseEvent::TransactionRolledBack { .. } => EventType::TransactionRollback,
        };
        
        if !subscription.event_types.contains(&event_type) {
            return false;
        }
        
        // Check table filter
        if let Some(table_filter) = &subscription.table_filter {
            let event_table = match event {
                DatabaseEvent::RowInserted { table, .. } |
                DatabaseEvent::RowUpdated { table, .. } |
                DatabaseEvent::RowDeleted { table, .. } => Some(table),
                _ => None,
            };
            
            if event_table.map_or(true, |t| t != table_filter) {
                return false;
            }
        }
        
        // TODO: Implement condition checking (SQL-like WHERE clause)
        
        true
    }

    fn execute_side_effects(&self, side_effects: &[SideEffect], db: &Arc<sled::Db>) -> Result<(), String> {
        for effect in side_effects {
            match effect {
                SideEffect::LogEvent { level, message } => {
                    println!("📝 [{}] {}", level, message);
                }
                SideEffect::SendNotification { channel, message } => {
                    println!("📡 EXECUTING SendNotification side effect");
                    println!("   Channel: '{}'", channel);
                    println!("   Message: '{}'", message);
                    
                    // NEW: Call WebSocket broadcasting callback
                    if let Some(callback) = &self.notification_callback {
                        println!("✅ WebSocket callback is available, executing...");
                        // Parse channel to extract database and table (format: "database.table" or just "table")
                        let parts: Vec<&str> = channel.split('.').collect();
                        let (database, table) = if parts.len() >= 2 {
                            (parts[0], parts[1])
                        } else {
                            ("default", channel.as_str()) // Default database if not specified
                        };
                        
                        println!("🎯 Calling callback for database: '{}', table: '{}'", database, table);
                        callback(database, table, message);
                        println!("📤 Successfully broadcasted WebSocket notification for {}.{}", database, table);
                    } else {
                        println!("❌ WebSocket callback is NOT available! Real-time notifications disabled.");
                    }
                }
                SideEffect::InsertRow { table, values } => {
                    println!("➕ Side effect: Insert into '{}': {:?}", table, values);
                    // FIXED: Actually execute the insert
                    let tree = db.open_tree(table).map_err(|e| format!("Failed to open tree for table {}: {}", table, e))?;
                    let key = values.get("id").unwrap_or(&uuid::Uuid::new_v4().to_string()).clone();
                    let value = serde_json::to_string(&values).map_err(|e| format!("Failed to serialize values: {}", e))?;
                    tree.insert(&key, value.as_bytes()).map_err(|e| format!("Failed to insert row: {}", e))?;
                    println!("✅ Successfully inserted row with key: {}", key);
                }
                SideEffect::UpdateRow { table, key, values } => {
                    println!("✏️ Side effect: Update '{}' key '{}': {:?}", table, key, values);
                    // FIXED: Actually execute the update
                    let tree = db.open_tree(table).map_err(|e| format!("Failed to open tree for table {}: {}", table, e))?;
                    let value = serde_json::to_string(&values).map_err(|e| format!("Failed to serialize values: {}", e))?;
                    tree.insert(&key, value.as_bytes()).map_err(|e| format!("Failed to update row: {}", e))?;
                    println!("✅ Successfully updated row with key: {}", key);
                }
                SideEffect::DeleteRow { table, key } => {
                    println!("❌ Side effect: Delete from '{}' key '{}'", table, key);
                    // FIXED: Actually execute the delete
                    let tree = db.open_tree(table).map_err(|e| format!("Failed to open tree for table {}: {}", table, e))?;
                    tree.remove(&key).map_err(|e| format!("Failed to delete row: {}", e))?;
                    println!("✅ Successfully deleted row with key: {}", key);
                }
                SideEffect::CallReducer { module, function, args } => {
                    println!("🔧 Side effect: Call reducer '{}::{}'({:?})", module, function, args);
                    // FIXED: Actually call the reducer
                    if let Some(target_module) = self.modules.get(module) {
                        let ctx = ModuleContext {
                            db: Arc::clone(&db),
                            event_id: uuid::Uuid::new_v4().to_string(),
                            timestamp: chrono::Utc::now(),
                            user_context: None,
                            sender_address: None,
                            transaction_id: None,
                        };
                        match target_module.reducer(&ctx, function, args) {
                            Ok(result) => {
                                println!("✅ Successfully called reducer {}::{} with result: {:?}", module, function, result);
                            }
                            Err(e) => {
                                println!("❌ Reducer '{}' in module '{}' failed: {}", function, module, e);
                            }
                        }
                    } else {
                        println!("❌ Module '{}' not found", module);
                    }
                }
                SideEffect::DatabaseWrite { table, data } => {
                    println!("💾 Side effect: Write to '{}': {:?}", table, data);
                    // FIXED: Actually execute the write
                    let tree = db.open_tree(table).map_err(|e| format!("Failed to open tree for table {}: {}", table, e))?;
                    let key = data.get("id").unwrap_or(&uuid::Uuid::new_v4().to_string()).clone();
                    let value = serde_json::to_string(&data).map_err(|e| format!("Failed to serialize data: {}", e))?;
                    tree.insert(&key, value.as_bytes()).map_err(|e| format!("Failed to write to database: {}", e))?;
                    println!("✅ Successfully wrote to database with key: {}", key);
                }
                SideEffect::DatabaseRead { table, conditions } => {
                    println!("📖 Side effect: Read from '{}' where: {:?}", table, conditions);
                    // FIXED: Actually execute the read
                    let tree = db.open_tree(table).map_err(|e| format!("Failed to open tree for table {}: {}", table, e))?;
                    let mut matched_rows = Vec::new();
                    
                    for entry in tree.iter() {
                        let (key, value) = entry.map_err(|e| format!("Failed to read database entry: {}", e))?;
                        if let Ok(row_data) = serde_json::from_slice::<HashMap<String, String>>(&value) {
                            // Simple condition matching - check if all conditions are met
                            let mut matches = true;
                            for (condition_key, condition_value) in conditions.iter() {
                                if !row_data.get(condition_key).map_or(false, |v| v == condition_value) {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                matched_rows.push((String::from_utf8_lossy(&key).to_string(), row_data));
                            }
                        }
                    }
                    println!("✅ Successfully read {} rows from database", matched_rows.len());
                }
            }
        }
        Ok(())
    }

    pub fn list_modules(&self) -> Vec<String> {
        self.modules.keys().cloned().collect()
    }

    pub fn list_subscriptions(&self) -> &[EventSubscription] {
        &self.subscriptions
    }
}

// ================================
// Realtime Module - Handles real-time notifications
// ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeConfig {
    pub table_configs: HashMap<String, TableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub channel_pattern: String,  // e.g., "chat_system.{table}" or "gaming.{table}"
    pub fields: Vec<String>,      // Fields to include in notification
    pub enabled: bool,
    pub events: Vec<String>,      // ["insert", "update", "delete"]
}

impl RealtimeConfig {
    pub fn load_from_toml(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use std::fs;
        
        let toml_content = fs::read_to_string(file_path)?;
        let config: toml::Value = toml::from_str(&toml_content)?;
        
        let mut table_configs = HashMap::new();
        
        // Parse modules section
        if let Some(modules) = config.get("modules").and_then(|v| v.as_table()) {
            for (_module_name, module_config) in modules {
                if let Some(tables) = module_config.get("tables").and_then(|v| v.as_array()) {
                    for table in tables {
                        if let Some(table_obj) = table.as_table() {
                            if let Some(name) = table_obj.get("name").and_then(|v| v.as_str()) {
                                let events = table_obj.get("events")
                                    .and_then(|v| v.as_array())
                                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                                    .unwrap_or_else(|| vec!["insert".to_string(), "update".to_string(), "delete".to_string()]);
                                
                                let fields = table_obj.get("fields")
                                    .and_then(|v| v.as_array())
                                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                                    .unwrap_or_else(|| vec!["*".to_string()]);
                                
                                let channel_pattern = table_obj.get("channel_pattern")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("realtime.{table}")
                                    .to_string();
                                
                                let realtime_enabled = table_obj.get("realtime_enabled")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true);
                                
                                table_configs.insert(name.to_string(), TableConfig {
                                    fields,
                                    channel_pattern,
                                    enabled: realtime_enabled,
                                    events,
                                });
                                
                                println!("   📋 Configured table '{}' for real-time sync", name);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(RealtimeConfig { table_configs })
    }
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        // Try to load from external config file, otherwise empty
        if let Ok(config) = Self::load_from_toml("module_config.toml") {
            println!("   📋 Loaded table configurations from module_config.toml");
            config
        } else {
            // COMPLETELY EMPTY - no hardcoded tables!
            // All configurations must be loaded externally via files or API calls
            RealtimeConfig { 
                table_configs: HashMap::new() 
            }
        }
    }
}

pub struct RealtimeModule {
    config: RealtimeConfig,
}

impl RealtimeModule {
    /// Creates a new RealtimeModule with EMPTY configuration
    /// Configuration must be loaded externally via add_table_config() or load_config_from_file()
    pub fn new() -> Self {
        Self {
            config: RealtimeConfig::default(), // Empty HashMap
        }
    }
    
    /// Creates a RealtimeModule with a pre-loaded configuration
    pub fn with_config(config: RealtimeConfig) -> Self {
        Self { config }
    }
    
    /// Dynamically add table configuration at runtime
    pub fn add_table_config(&mut self, table_name: String, config: TableConfig) {
        println!("✅ Added runtime configuration for table: {}", table_name);
        self.config.table_configs.insert(table_name, config);
    }
    
    /// Load configuration from external source (file, API, etc.)
    pub fn load_config_from_file(&mut self, _config_path: &str) -> Result<(), String> {
        // TODO: Implement actual file loading
        // This would load TOML/JSON configuration files
        println!("⚠️ load_config_from_file not yet implemented - configurations must be added via add_table_config()");
        Ok(())
    }
    
    /// Check if any tables are configured
    pub fn is_configured(&self) -> bool {
        !self.config.table_configs.is_empty()
    }
    
    /// Get list of configured tables
    pub fn get_configured_tables(&self) -> Vec<String> {
        self.config.table_configs.keys().cloned().collect()
    }
    
    fn generate_notification(&self, table: &str, row: &HashMap<String, String>, event_type: &str) -> Result<ModuleResponse, String> {
        // Check if table is configured for real-time notifications
        if let Some(table_config) = self.config.table_configs.get(table) {
            if !table_config.enabled || !table_config.events.contains(&event_type.to_string()) {
                return Ok(ModuleResponse {
                    success: true,
                    message: Some(format!("Table '{}' not configured for '{}' events", table, event_type)),
                    data: None,
                    side_effects: vec![],
                });
            }
            
            println!("✅ Generating real-time {} notification for {}", event_type, table);
            
            // Build notification data using configured fields
            let mut notification_data = serde_json::Map::new();
            for field in &table_config.fields {
                let value = row.get(field).unwrap_or(&"".to_string()).clone();
                notification_data.insert(field.clone(), serde_json::Value::String(value));
            }
            
            // Generate channel name using pattern with field substitution
            let mut channel = table_config.channel_pattern.replace("{table}", table);
            
            // Replace field placeholders like {session_id}, {player_id}, etc.
            for field in &table_config.fields {
                if let Some(value) = row.get(field) {
                    let placeholder = format!("{{{}}}", field);
                    channel = channel.replace(&placeholder, value);
                }
            }
            
            let notification_json = serde_json::Value::Object(notification_data);
            println!("📡 {} notification data: {}", table, notification_json);
            
            Ok(ModuleResponse {
                success: true,
                message: Some(format!("Real-time {} notification generated for {}", event_type, table)),
                data: None,
                side_effects: vec![
                    SideEffect::SendNotification {
                        channel,
                        message: notification_json.to_string(),
                    }
                ],
            })
        } else {
            // No configuration found - table is not set up for real-time notifications
            println!("ℹ️ Table '{}' not configured for real-time notifications (no hardcoded defaults)", table);
            Ok(ModuleResponse {
                success: true,
                message: Some(format!("Table '{}' {} event ignored - not configured", table, event_type)),
                data: None,
                side_effects: vec![],
            })
        }
    }
}

impl Module for RealtimeModule {
    fn name(&self) -> &str {
        "realtime_module"
    }

    fn on_insert(&self, _ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔥 RealtimeModule::on_insert called for table: {}", table);
        println!("📊 Row data: {:?}", row);
        
        self.generate_notification(table, row, "insert")
    }

    fn on_update(&self, _ctx: &ModuleContext, table: &str, _old_row: &HashMap<String, String>, new_row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔥 RealtimeModule::on_update called for table: {}", table);
        println!("📊 New row data: {:?}", new_row);
        
        self.generate_notification(table, new_row, "update")
    }

    fn on_delete(&self, _ctx: &ModuleContext, table: &str, row: &HashMap<String, String>) -> Result<ModuleResponse, String> {
        println!("🔥 RealtimeModule::on_delete called for table: {}", table);
        println!("📊 Deleted row data: {:?}", row);
        
        self.generate_notification(table, row, "delete")
    }

    fn reducer(&self, _ctx: &ModuleContext, _name: &str, _args: &[serde_json::Value]) -> Result<serde_json::Value, String> {
        Err("No reducers implemented for realtime module".to_string())
    }

    fn on_transaction_commit(&self, _ctx: &ModuleContext, _tx_id: &str, _tables: &[String]) -> Result<ModuleResponse, String> {
        Ok(ModuleResponse {
            success: true,
            message: Some("Transaction commit logged".to_string()),
            data: None,
            side_effects: vec![],
        })
    }

    fn init(&self, _ctx: &ModuleContext) -> Result<(), String> {
        let configured_tables = self.get_configured_tables();
        if configured_tables.is_empty() {
            println!("🔥 Realtime Module initialized - NO TABLE CONFIGURATIONS LOADED");
            println!("   ⚠️  Use add_table_config() or load_config_from_file() to configure tables");
            println!("   ℹ️  No hardcoded table configurations exist");
        } else {
            println!("🔥 Realtime Module initialized - Configured for {} tables: {:?}", 
                     configured_tables.len(), configured_tables);
        }
        Ok(())
    }
}

impl DatabaseEvent {
    pub fn new(event_type: &str, table: &str, data: &HashMap<String, String>) -> Self {
        match event_type {
            "INSERT" => DatabaseEvent::RowInserted {
                table: table.to_string(),
                row: data.clone(),
                timestamp: chrono::Utc::now(),
                tx_id: None,
            },
            "UPDATE" => DatabaseEvent::RowUpdated {
                table: table.to_string(),
                old_row: HashMap::new(),
                new_row: data.clone(),
                timestamp: chrono::Utc::now(),
                tx_id: None,
            },
            "DELETE" => DatabaseEvent::RowDeleted {
                table: table.to_string(),
                row: data.clone(),
                timestamp: chrono::Utc::now(),
                tx_id: None,
            },
            _ => DatabaseEvent::RowInserted {
                table: table.to_string(),
                row: data.clone(),
                timestamp: chrono::Utc::now(),
                tx_id: None,
            },
        }
    }
}

impl ModuleManager {
    pub fn emit_event(&self, event: DatabaseEvent) {
        // Simple implementation - just log the event
        println!("📢 MODULE EVENT: {:?}", event);
    }

    pub fn call_reducer(&self, module_name: &str, function_name: &str, args: &[serde_json::Value], client_id: Option<String>) -> Result<String, String> {
        // Simple implementation for now
        Ok(format!(r#"{{"success": true, "module": "{}", "function": "{}", "client": "{}"}}"#, 
                   module_name, function_name, client_id.unwrap_or("unknown".to_string())))
    }

}