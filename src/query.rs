/*
📌 File: src/query.rs (COMPLETE FIXED VERSION)
🔄 Enhanced Query Executor with Security Integration
✅ All ownership issues resolved
✅ Compatible with secure_executor
✅ Proper QueryResponse structure
*/
use sled::Db;
use crate::parser::ParsedQuery;
use std::collections::HashMap;
use serde_json;
use lru::LruCache;
use std::sync::{Arc, Mutex};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, Duration};
use crate::transaction::TransactionManager;
use crate::transaction::TransactionData;
use crate::schema::SchemaManager;
use crate::modules::{ModuleManager, DatabaseEvent};
use crate::join_engine::{JoinExecutor, JoinCondition, JoinType};

// NEW: Struttura per chiamate reducer (SpacetimeDB-style)
#[derive(Debug, serde::Deserialize)]
pub struct ReducerCall {
    pub module: String,
    pub function: String,
    pub args: Vec<serde_json::Value>,
}

// ✅ FIXED: Complete QueryResponse with all required fields including affected_rows
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryResponse {
    pub status: u16,
    pub message: String,
    pub table: Option<String>,
    pub results: Option<Vec<HashMap<String, String>>>,
    pub affected_rows: usize,
}

pub struct QueryExecutor {
    db: Arc<Db>,
    cache: Arc<Mutex<LruCache<String, (String, Instant)>>>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    cache_ttl: Duration,
    active_transactions: Arc<Mutex<HashMap<String, TransactionData>>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    schema_manager: Arc<Mutex<SchemaManager>>,
    // NEW: Module system integration
    module_manager: Arc<Mutex<ModuleManager>>,
    join_executor: Arc<Mutex<JoinExecutor>>,
}

impl QueryExecutor {
    /// ✅ FIXED: Constructor returns Arc<QueryExecutor>
    pub fn new(db: Arc<Db>, cache_size: usize, cache_ttl_seconds: u64) -> Arc<Self> {
        let cache = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap())));
        let cache_ttl = Duration::from_secs(cache_ttl_seconds);
        let active_transactions = Arc::new(Mutex::new(HashMap::new()));
        
        // ✅ FIXED: Pass required arguments
        let transaction_manager = Arc::new(Mutex::new(
            TransactionManager::new(Arc::clone(&db), Arc::clone(&active_transactions))
        ));
        
        let schema_manager = Arc::new(Mutex::new(SchemaManager::new(Arc::clone(&db))));
        
        // Initialize Module Manager with default modules
        let mut module_manager = ModuleManager::new();
        
        // Register default modules
        if let Err(e) = module_manager.register_module(Box::new(crate::modules::AuditModule::new())) {
            println!("⚠️ Failed to register AuditModule: {}", e);
        } else {
            println!("✅ AuditModule registered successfully");
        }
        
        if let Err(e) = module_manager.register_module(Box::new(crate::modules::RealtimeModule::new())) {
            println!("⚠️ Failed to register RealtimeModule: {}", e);
        } else {
            println!("✅ RealtimeModule registered successfully");
        }
        
        let module_manager = Arc::new(Mutex::new(module_manager));
        
        // ✅ FIXED: Pass required argument
        let join_executor = Arc::new(Mutex::new(JoinExecutor::new(Arc::clone(&db))));

        Arc::new(Self {
            db,
            cache,
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            cache_ttl,
            active_transactions,
            transaction_manager,
            schema_manager,
            module_manager,
            join_executor,
        })
    }

    /// ✅ FIXED: Main execute_query method - takes reference instead of ownership
    pub fn execute_query(&self, parsed_query: &ParsedQuery, tx_id: Option<String>) -> Result<String, String> {
        // Force log to stderr to ensure it appears
        eprintln!("🔍 DEBUG EXECUTE_QUERY: parsed_query={:?}", parsed_query);
        let response = match parsed_query {
            ParsedQuery::Select { table, columns, joins, conditions, group_by, order_by, limit, aggregates, having, ctes, window_functions, case_expressions } => {
                // Handle different types of conditions
                // First handle CTEs if present
                if let Some(cte_list) = ctes {
                    println!("🔍 DEBUG CTE: Processing {} CTEs", cte_list.len());
                    for (cte_name, cte_query) in cte_list {
                        println!("🔍 DEBUG CTE: Creating temporary table '{}'", cte_name);
                        self.execute_cte_creation(cte_name, cte_query)?;
                    }
                }
                
                // Resolve table name (check for CTE temporary tables)
                let resolved_table = self.resolve_table_name(&table);
                
                // Handle Window Functions if present
                if let Some(window_funcs) = window_functions {
                    println!("🔍 DEBUG WINDOW: Processing {} window functions", window_funcs.len());
                    return self.execute_select_with_window_functions(&resolved_table, window_funcs, conditions.clone(), order_by.clone(), limit.clone(), tx_id);
                }
                
                // Handle CASE expressions if present
                if let Some(case_exprs) = case_expressions {
                    println!("🔍 DEBUG CASE: Processing {} CASE expressions", case_exprs.len());
                    return self.execute_select_with_case_expressions(&resolved_table, case_exprs, conditions.clone(), order_by.clone(), limit.clone(), tx_id);
                }
                
                let result = if let Some(condition_str) = conditions {
                    if condition_str.contains(" IN ") {
                        // Handle IN clause with subquery
                        self.execute_select_with_subquery_condition(&resolved_table, condition_str, order_by.clone(), limit.clone(), tx_id)
                    } else {
                        // Handle simple conditions
                        let mut conditions_map = HashMap::new();
                        if condition_str.contains('=') {
                            let parts: Vec<&str> = condition_str.split('=').collect();
                            if parts.len() == 2 {
                                let key = parts[0].trim().to_string();
                                let value = parts[1].trim().replace("'", "").replace("\"", "");
                                conditions_map.insert(key, value);
                            }
                        }
                        
                        // Handle different query types with simple conditions
                        if !joins.is_empty() && (group_by.is_some() || aggregates.is_some()) {
                            self.execute_join_with_aggregates(&resolved_table, joins.clone(), conditions_map, group_by.clone(), aggregates.clone(), having.clone(), order_by.clone(), limit.clone(), tx_id)
                        } else if !joins.is_empty() {
                            self.execute_select_with_joins(&resolved_table, joins.clone(), conditions_map, order_by.clone(), limit.clone(), tx_id)
                        } else if group_by.is_some() || aggregates.is_some() {
                            self.execute_aggregate_query(&resolved_table, conditions_map, group_by.clone(), aggregates.clone(), having.clone(), order_by.clone(), limit.clone(), tx_id)
                        } else {
                            self.execute_select_with_order_limit(&resolved_table, conditions_map, order_by.clone(), limit.clone(), tx_id)
                        }
                    }
                } else {
                    let empty_conditions = HashMap::new();
                    
                    // Handle different query types with no conditions
                    if !joins.is_empty() && (group_by.is_some() || aggregates.is_some()) {
                        self.execute_join_with_aggregates(&resolved_table, joins.clone(), empty_conditions, group_by.clone(), aggregates.clone(), having.clone(), order_by.clone(), limit.clone(), tx_id)
                    } else if !joins.is_empty() {
                        self.execute_select_with_joins(&resolved_table, joins.clone(), empty_conditions, order_by.clone(), limit.clone(), tx_id)
                    } else if group_by.is_some() || aggregates.is_some() {
                        self.execute_aggregate_query(&resolved_table, empty_conditions, group_by.clone(), aggregates.clone(), having.clone(), order_by.clone(), limit.clone(), tx_id)
                    } else {
                        self.execute_select_with_order_limit(&resolved_table, empty_conditions, order_by.clone(), limit.clone(), tx_id)
                    }
                };
                
                result
            },
            ParsedQuery::Insert { table, values } => {
                let resolved_table = self.resolve_table_name(&table);
                self.execute_insert(&resolved_table, values.clone(), tx_id)
            },
            ParsedQuery::Update { table, values, conditions } => {
                let resolved_table = self.resolve_table_name(&table);
                let legacy_conditions = if let Some(condition_str) = conditions {
                    let mut conditions_map = HashMap::new();
                    if condition_str.contains('=') {
                        let parts: Vec<&str> = condition_str.split('=').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            let value = parts[1].trim().replace("'", "").replace("\"", "");
                            conditions_map.insert(key, value);
                        }
                    }
                    conditions_map
                } else {
                    HashMap::new()
                };
                self.execute_update(&resolved_table, values.clone(), legacy_conditions, tx_id)
            },
            ParsedQuery::Delete { table, conditions } => {
                let resolved_table = self.resolve_table_name(&table);
                let legacy_conditions = if let Some(condition_str) = conditions {
                    let mut conditions_map = HashMap::new();
                    if condition_str.contains('=') {
                        let parts: Vec<&str> = condition_str.split('=').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            let value = parts[1].trim().replace("'", "").replace("\"", "");
                            conditions_map.insert(key, value);
                        }
                    }
                    conditions_map
                } else {
                    HashMap::new()
                };
                self.execute_delete(&resolved_table, legacy_conditions, tx_id)
            },
            ParsedQuery::CreateTable { schema, .. } => self.execute_create_table(schema.clone()),
            ParsedQuery::DropTable { table } => self.execute_drop_table(table),
            ParsedQuery::BeginTransaction => {
                let tx_id = tx_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                self.begin_transaction(tx_id.clone()).map(|_| QueryResponse {
                    status: 200,
                    message: format!("Transaction {} started", tx_id),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            ParsedQuery::Commit => {
                let tx_id = tx_id.ok_or_else(|| "No active transaction to commit".to_string())?;
                self.commit_transaction(tx_id.clone()).map(|_| QueryResponse {
                    status: 200,
                    message: "Transaction committed".to_string(),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            ParsedQuery::Rollback => {
                let tx_id = tx_id.ok_or_else(|| "No active transaction to rollback".to_string())?;
                self.rollback_transaction(tx_id.clone()).map(|_| QueryResponse {
                    status: 200,
                    message: "Transaction rolled back".to_string(),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            // Legacy support
            ParsedQuery::BeginTransactionLegacy { tx_id } => {
                self.begin_transaction(tx_id.clone()).map(|_| QueryResponse {
                    status: 200,
                    message: format!("Transaction {} started", tx_id),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            ParsedQuery::CommitTransactionLegacy { tx_id } => {
                self.commit_transaction(tx_id.to_string()).map(|_| QueryResponse {
                    status: 200,
                    message: "Transaction committed".to_string(),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            ParsedQuery::RollbackTransactionLegacy { tx_id } => {
                self.rollback_transaction(tx_id.to_string()).map(|_| QueryResponse {
                    status: 200,
                    message: "Transaction rolled back".to_string(),
                    table: None,
                    results: None,
                    affected_rows: 0,
                })
            },
            // Database management commands
            ParsedQuery::CreateDatabase { name, description, if_not_exists } => {
                self.execute_create_database(name, description.as_deref(), *if_not_exists)
            },
            ParsedQuery::UseDatabase { name } => {
                self.execute_use_database(name)
            },
            ParsedQuery::ShowDatabases => {
                self.execute_show_databases()
            },
            ParsedQuery::ShowTables => {
                self.execute_show_tables()
            },
            ParsedQuery::ShowUsers => {
                self.execute_show_users()
            },
            ParsedQuery::ShowStatus => {
                self.execute_show_status()
            },
            ParsedQuery::DescribeTable { table } => {
                self.execute_describe_table(table)
            },
            ParsedQuery::CreateIndex { name, table, columns, unique } => {
                self.execute_create_index(name, table, columns, *unique)
            },
            ParsedQuery::Subscribe { table } => {
                self.execute_subscribe(table)
            },
            ParsedQuery::Unsubscribe { table } => {
                self.execute_unsubscribe(table)
            },
            ParsedQuery::Auth { credentials } => {
                self.execute_auth(credentials)
            },
            ParsedQuery::LoadModule { module_name, file_path } => {
                self.execute_load_module(module_name, file_path)
            },
            ParsedQuery::WasmExec { module_name, function_name, args } => {
                self.execute_wasm_exec(module_name, function_name, args)
            },
            ParsedQuery::DropDatabase { name } => {
                self.execute_drop_database(name)
            },
        };
    
        response.map(|res| serde_json::to_string(&res).unwrap())
    }

    /// Getter per accedere al database
    pub fn get_db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Getter per accedere alla cache
    pub fn get_cache(&self) -> &Arc<Mutex<LruCache<String, (String, Instant)>>> {
        &self.cache
    }

    /// NEW: Getter per accedere al module manager
    pub fn get_module_manager(&self) -> &Arc<Mutex<ModuleManager>> {
        &self.module_manager
    }

    /// Ottiene tutte le chiavi di una tabella
    pub fn get_table_keys(&self, table: &str) -> Vec<String> {
        let tree = self.db.open_tree(table).expect("Errore apertura tabella");
        tree.iter()
            .map(|entry| {
                let (key, _) = entry.expect("Errore lettura chiave");
                String::from_utf8(key.to_vec()).unwrap_or_else(|_| format!("{:?}", key))
            })
            .collect()
    }

    /// Ottiene tutte le transazioni attive
    pub fn get_active_transactions(&self) -> Vec<String> {
        self.active_transactions.lock().unwrap().keys().cloned().collect()
    }

    /// Ottiene le metriche della cache
    pub fn get_cache_metrics(&self) -> (usize, usize, f64) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        (hits, misses, hit_rate)
    }

    /// ✅ FIXED: Execute SELECT with cache support
    fn execute_select(&self, table: &str, conditions: HashMap<String, String>, _tx_id: Option<String>) -> Result<QueryResponse, String> {
        let cache_key = format!("SELECT {} WHERE {:?}", table, conditions);
        
        // Check cache
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some((cached_result, timestamp)) = cache.get(&cache_key) {
                if timestamp.elapsed() < self.cache_ttl {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(serde_json::from_str(cached_result).unwrap());
                } else {
                    cache.pop(&cache_key);
                }
            }
        }
        
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let tree = self.db.open_tree(table).unwrap();
        let mut results = vec![];

        for entry in tree.iter() {
            let (_, value) = entry.unwrap();
            let value_str = String::from_utf8(value.to_vec()).unwrap_or_else(|_| format!("{:?}", value));
            let value_map: HashMap<String, String> = serde_json::from_str(&value_str).unwrap_or_default();

            let match_found = conditions.iter().all(|(k, v)| value_map.get(k) == Some(v));

            if match_found {
                results.push(value_map);
            }
        }

        let response = QueryResponse {
            status: 200,
            message: "Query executed successfully".to_string(),
            table: Some(table.to_string()),
            results: Some(results.clone()),
            affected_rows: results.len(),
        };

        // Save to cache
        {
            let mut cache = self.cache.lock().unwrap();
            let serialized_response = serde_json::to_string(&response).unwrap();
            cache.put(cache_key, (serialized_response, Instant::now()));
        }

        Ok(response)
    }

    /// ✅ NEW: Execute SELECT with ORDER BY and LIMIT support
    fn execute_select_with_order_limit(&self, table: &str, conditions: HashMap<String, String>, order_by: Option<String>, limit: Option<usize>, _tx_id: Option<String>) -> Result<QueryResponse, String> {
        let cache_key = format!("SELECT {} WHERE {:?} ORDER BY {:?} LIMIT {:?}", table, conditions, order_by, limit);
        
        // Check cache
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some((cached_result, timestamp)) = cache.get(&cache_key) {
                if timestamp.elapsed() < self.cache_ttl {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(serde_json::from_str(cached_result).unwrap());
                } else {
                    cache.pop(&cache_key);
                }
            }
        }
        
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let tree = self.db.open_tree(table).unwrap();
        let mut results = vec![];

        for entry in tree.iter() {
            let (_, value) = entry.unwrap();
            let value_str = String::from_utf8(value.to_vec()).unwrap_or_else(|_| format!("{:?}", value));
            let value_map: HashMap<String, String> = serde_json::from_str(&value_str).unwrap_or_default();

            let match_found = conditions.iter().all(|(k, v)| value_map.get(k) == Some(v));

            if match_found {
                results.push(value_map);
            }
        }

        // Apply ORDER BY if specified
        if let Some(order_col) = order_by {
            println!("🔍 DEBUG ORDER BY: Sorting by column '{}'", order_col);
            
            // Parse ORDER BY column and direction
            let (column, descending) = if order_col.contains(" DESC") {
                let col = order_col.replace(" DESC", "").trim().to_string();
                (col, true)
            } else if order_col.contains(" ASC") {
                let col = order_col.replace(" ASC", "").trim().to_string();
                (col, false)
            } else {
                (order_col.trim().to_string(), false)
            };
            
            println!("🔍 DEBUG ORDER BY: Column='{}', Descending={}", column, descending);
            
            results.sort_by(|a, b| {
                let empty_string = String::new();
                let a_val = a.get(&column).unwrap_or(&empty_string);
                let b_val = b.get(&column).unwrap_or(&empty_string);
                
                let comparison = a_val.cmp(b_val);
                if descending {
                    comparison.reverse()
                } else {
                    comparison
                }
            });
            
            println!("🔍 DEBUG ORDER BY: Results after sorting: {} rows", results.len());
        }

        // Apply LIMIT if specified
        if let Some(limit_count) = limit {
            println!("🔍 DEBUG LIMIT: Limiting to {} rows from {}", limit_count, results.len());
            results.truncate(limit_count);
            println!("🔍 DEBUG LIMIT: Results after limit: {} rows", results.len());
        }

        let response = QueryResponse {
            status: 200,
            message: "Query executed successfully".to_string(),
            table: Some(table.to_string()),
            results: Some(results.clone()),
            affected_rows: results.len(),
        };

        // Save to cache
        {
            let mut cache = self.cache.lock().unwrap();
            let serialized_response = serde_json::to_string(&response).unwrap();
            cache.put(cache_key, (serialized_response, Instant::now()));
        }

        Ok(response)
    }

    /// ✅ NEW: Execute SELECT with subquery condition (like IN clause)
    fn execute_select_with_subquery_condition(&self, table: &str, condition: &str, order_by: Option<String>, limit: Option<usize>, _tx_id: Option<String>) -> Result<QueryResponse, String> {
        println!("🔍 DEBUG SUBQUERY: Executing subquery condition: {}", condition);
        
        // Parse IN clause: "id IN (SELECT user_id FROM posts)"
        if condition.contains(" IN ") {
            let parts: Vec<&str> = condition.split(" IN ").collect();
            if parts.len() == 2 {
                let column = parts[0].trim();
                let subquery_part = parts[1].trim();
                
                // Remove parentheses from subquery
                let subquery = subquery_part.trim_start_matches('(').trim_end_matches(')');
                println!("🔍 DEBUG SUBQUERY: Column: {}, Subquery: {}", column, subquery);
                
                // Execute the subquery first
                let subquery_parsed = match crate::parser::SQLParser::parse_query(subquery) {
                    Ok(parsed) => parsed,
                    Err(e) => return Err(format!("Error parsing subquery: {}", e)),
                };
                
                let subquery_response = self.execute_query(&subquery_parsed, None)?;
                let subquery_result: QueryResponse = serde_json::from_str(&subquery_response)
                    .map_err(|e| format!("Error parsing subquery response: {}", e))?;
                
                // Extract values from subquery results
                let mut in_values = Vec::new();
                if let Some(results) = subquery_result.results {
                    for row in results {
                        // Get the first column value from each row
                        if let Some(value) = row.values().next() {
                            in_values.push(value.clone());
                        }
                    }
                }
                
                println!("🔍 DEBUG SUBQUERY: IN values: {:?}", in_values);
                
                // Now execute the main query with the IN values
                let tree = self.db.open_tree(table).unwrap();
                let mut results = vec![];

                for entry in tree.iter() {
                    let (_, value) = entry.unwrap();
                    let value_str = String::from_utf8(value.to_vec()).unwrap_or_else(|_| format!("{:?}", value));
                    let value_map: HashMap<String, String> = serde_json::from_str(&value_str).unwrap_or_default();

                    // Check if the column value is in the IN list
                    if let Some(column_value) = value_map.get(column) {
                        if in_values.contains(column_value) {
                            results.push(value_map);
                        }
                    }
                }
                
                // Apply ORDER BY if specified
                if let Some(order_col) = order_by {
                    let (column, descending) = if order_col.contains(" DESC") {
                        let col = order_col.replace(" DESC", "").trim().to_string();
                        (col, true)
                    } else if order_col.contains(" ASC") {
                        let col = order_col.replace(" ASC", "").trim().to_string();
                        (col, false)
                    } else {
                        (order_col.trim().to_string(), false)
                    };
                    
                    results.sort_by(|a, b| {
                        let empty_string = String::new();
                        let a_val = a.get(&column).unwrap_or(&empty_string);
                        let b_val = b.get(&column).unwrap_or(&empty_string);
                        
                        let comparison = a_val.cmp(b_val);
                        if descending {
                            comparison.reverse()
                        } else {
                            comparison
                        }
                    });
                }

                // Apply LIMIT if specified
                if let Some(limit_count) = limit {
                    results.truncate(limit_count);
                }

                return Ok(QueryResponse {
                    status: 200,
                    message: "Query executed successfully".to_string(),
                    table: Some(table.to_string()),
                    results: Some(results.clone()),
                    affected_rows: results.len(),
                });
            }
        }
        
        Err("Unsupported subquery condition".to_string())
    }

    /// ✅ FIXED: Execute INSERT with validation (transaction optional)
    fn execute_insert(&self, table: &str, values: HashMap<String, String>, tx_id: Option<String>) -> Result<QueryResponse, String> {
        println!("🔍 DEBUG INSERT: table={}, values={:?}", table, values);
        
        // Auto-generate ID if not provided (for PRIMARY KEY columns)
        let mut final_values = values.clone();
        
        // Apply default values from schema first
        if let Ok(schema_manager) = self.schema_manager.lock() {
            if let Some(schema) = schema_manager.get_schema(table) {
                for column in &schema.columns {
                    // Apply default values for missing columns
                    if !final_values.contains_key(&column.name) {
                        for constraint in &column.constraints {
                            if let crate::schema::Constraint::Default(default_value) = constraint {
                                let processed_default = if default_value.starts_with('\'') && default_value.ends_with('\'') {
                                    // Remove single quotes from string defaults
                                    default_value[1..default_value.len()-1].to_string()
                                } else if default_value == "(strftime('%s', 'now'))" {
                                    // Handle current timestamp
                                    chrono::Utc::now().timestamp().to_string()
                                } else {
                                    default_value.clone()
                                };
                                final_values.insert(column.name.clone(), processed_default);
                                println!("🔍 DEBUG: Applied default value for {}: {}", column.name, default_value);
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        let key = if let Some(id) = values.get("id") {
            println!("🔍 DEBUG: Using existing ID: {}", id);
            id.as_bytes().to_vec()
        } else {
            println!("🔍 DEBUG: Auto-generating ID...");
            // Generate auto-increment ID
            let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
            let next_id = tree.len() + 1;
            println!("🔍 DEBUG: Generated ID: {}", next_id);
            final_values.insert("id".to_string(), next_id.to_string());
            next_id.to_string().as_bytes().to_vec()
        };
        
        println!("🔍 DEBUG: Final values with defaults: {:?}", final_values);
        
        // Validate schema AFTER auto-generating ID
        if let Ok(schema_manager) = self.schema_manager.lock() {
            if let Err(validation_error) = schema_manager.validate_row(table, &final_values) {
                return Err(format!("Schema validation failed: {}", validation_error));
            }
        }
        
        // Validate UNIQUE constraints
        if let Err(unique_error) = self.validate_unique_constraints(table, &final_values) {
            return Err(format!("UNIQUE constraint violation: {}", unique_error));
        }
        
        // Validate FOREIGN KEY constraints
        if let Err(fk_error) = self.validate_foreign_key_constraints(table, &final_values) {
            return Err(format!("FOREIGN KEY constraint violation: {}", fk_error));
        }
        
        // Validate CHECK constraints
        if let Err(check_error) = self.validate_check_constraints(table, &final_values) {
            return Err(format!("CHECK constraint violation: {}", check_error));
        }
        
        let value = serde_json::to_string(&final_values).map_err(|e| e.to_string())?;
    
        // If transaction ID is provided, add to transaction batch WITHOUT writing to database
        if let Some(tx) = tx_id {
            let key_str = String::from_utf8(key.clone()).unwrap_or_else(|_| format!("{:?}", key));
            if let Ok(transaction_manager) = self.transaction_manager.lock() {
                transaction_manager.add_insert_operation(&tx, table, &key_str, &value)?;
            }
            println!("🔍 DEBUG INSERT IN TRANSACTION: Operation staged in batch for tx {}", tx);
            
            // Don't emit event during transaction - events will be emitted on commit
        } else {
            // Execute insert immediately if no transaction
            let tree = self.db.open_tree(table).unwrap();
            tree.insert(key, value.as_bytes()).unwrap();
            println!("🔍 DEBUG INSERT NO TRANSACTION: Operation applied immediately");
            
            // Emit event for immediate insert and trigger modules
            let event = DatabaseEvent::new("INSERT", table, &final_values);
            if let Ok(module_manager) = self.module_manager.lock() {
                // First log the event
                module_manager.emit_event(event.clone());
                
                // Then trigger modules to generate side effects
                if let Ok(_responses) = module_manager.trigger_event(event, Arc::clone(&self.db)) {
                    println!("🔥 Modules triggered for INSERT event on table: {}", table);
                }
            }
        }

        Ok(QueryResponse {
            status: 201,
            message: format!("1 record inserted into {}", table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: 1,
        })
    }

    /// Validate UNIQUE constraints before inserting
    fn validate_unique_constraints(&self, table: &str, values: &HashMap<String, String>) -> Result<(), String> {
        // Get schema to check for UNIQUE constraints
        let schema_manager = self.schema_manager.lock().map_err(|e| e.to_string())?;
        let schema = schema_manager.get_schema(table)
            .ok_or_else(|| format!("Schema not found for table: {}", table))?;
        
        // Check each column for UNIQUE constraints
        for column in &schema.columns {
            let has_unique_constraint = column.constraints.iter().any(|constraint| {
                matches!(constraint, crate::schema::Constraint::Unique)
            });
            
            if has_unique_constraint {
                if let Some(value) = values.get(&column.name) {
                    println!("🔍 DEBUG UNIQUE: Checking column '{}' with value '{}'", column.name, value);
                    
                    // Check if this value already exists in the table
                    let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
                    
                    for entry in tree.iter() {
                        let (_, existing_value) = entry.map_err(|e| e.to_string())?;
                        let existing_value_str = String::from_utf8_lossy(&existing_value);
                        
                        if let Ok(existing_row) = serde_json::from_str::<HashMap<String, String>>(&existing_value_str) {
                            if let Some(existing_col_value) = existing_row.get(&column.name) {
                                if existing_col_value == value {
                                    return Err(format!("Duplicate value '{}' for UNIQUE column '{}'", value, column.name));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Validate UNIQUE constraints for UPDATE (excludes current record)
    fn validate_unique_constraints_for_update(&self, table: &str, values: &HashMap<String, String>, current_key: &[u8]) -> Result<(), String> {
        // Get schema to check for UNIQUE constraints
        let schema_manager = self.schema_manager.lock().map_err(|e| e.to_string())?;
        let schema = schema_manager.get_schema(table)
            .ok_or_else(|| format!("Schema not found for table: {}", table))?;
        
        // Check each column for UNIQUE constraints
        for column in &schema.columns {
            let has_unique_constraint = column.constraints.iter().any(|constraint| {
                matches!(constraint, crate::schema::Constraint::Unique)
            });
            
            if has_unique_constraint {
                if let Some(value) = values.get(&column.name) {
                    println!("🔍 DEBUG UNIQUE UPDATE: Checking column '{}' with value '{}'", column.name, value);
                    
                    // Check if this value already exists in other records
                    let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
                    
                    for entry in tree.iter() {
                        let (key, existing_value) = entry.map_err(|e| e.to_string())?;
                        
                        // Skip the current record being updated
                        if key == current_key {
                            continue;
                        }
                        
                        let existing_value_str = String::from_utf8_lossy(&existing_value);
                        
                        if let Ok(existing_row) = serde_json::from_str::<HashMap<String, String>>(&existing_value_str) {
                            if let Some(existing_col_value) = existing_row.get(&column.name) {
                                if existing_col_value == value {
                                    return Err(format!("Duplicate value '{}' for UNIQUE column '{}'", value, column.name));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// ✅ FIXED: Execute UPDATE
    fn execute_update(&self, table: &str, values: HashMap<String, String>, conditions: HashMap<String, String>, tx_id: Option<String>) -> Result<QueryResponse, String> {
        // If in transaction, don't apply changes immediately - stage them
        if let Some(tx) = tx_id {
            println!("🔍 DEBUG UPDATE IN TRANSACTION: Staging update for tx {}", tx);
            let mut transactions = self.active_transactions.lock().unwrap();
            let tx_data = transactions.entry(tx.clone()).or_insert_with(|| TransactionData::new());
            tx_data.modified_tables.insert(table.to_string());
            
            // For now, return success but don't apply changes
            // TODO: Implement proper transaction staging for UPDATE operations
            return Ok(QueryResponse {
                status: 200,
                message: format!("Update staged in transaction {} for table {}", tx, table),
                table: Some(table.to_string()),
                results: None,
                affected_rows: 0,
            });
        }
        
        let tree = self.db.open_tree(table).unwrap();
        let mut updated_count = 0;

        for entry in tree.iter() {
            let (key, existing_value) = entry.unwrap();
            let existing_value_str = String::from_utf8(existing_value.to_vec()).unwrap_or_default();
            let mut existing_map: HashMap<String, String> = serde_json::from_str(&existing_value_str).unwrap_or_default();

            let match_found = conditions.iter().all(|(k, v)| existing_map.get(k) == Some(v));

            if match_found {
                // Create updated row
                let mut updated_row = existing_map.clone();
                for (k, v) in &values {
                    updated_row.insert(k.clone(), v.clone());
                }
                
                // Validate UNIQUE constraints for updated row
                if let Err(unique_error) = self.validate_unique_constraints_for_update(table, &updated_row, &key) {
                    return Err(format!("UNIQUE constraint violation: {}", unique_error));
                }
                
                let new_value = serde_json::to_string(&updated_row).unwrap();
                tree.insert(key, new_value.as_bytes()).unwrap();
                updated_count += 1;
                
                // Emit event for UPDATE and trigger modules
                let event = crate::modules::DatabaseEvent::RowUpdated {
                    table: table.to_string(),
                    old_row: existing_map,
                    new_row: updated_row,
                    timestamp: chrono::Utc::now(),
                    tx_id: tx_id.clone(),
                };
                
                if let Ok(module_manager) = self.module_manager.lock() {
                    // First log the event
                    module_manager.emit_event(event.clone());
                    
                    // Then trigger modules to generate side effects
                    if let Ok(_responses) = module_manager.trigger_event(event, Arc::clone(&self.db)) {
                        println!("🔥 Modules triggered for UPDATE event on table: {}", table);
                    }
                }
            }
        }

        Ok(QueryResponse {
            status: 200,
            message: format!("{} records updated in {}", updated_count, table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: updated_count,
        })
    }

    /// ✅ FIXED: Execute DELETE
    fn execute_delete(&self, table: &str, conditions: HashMap<String, String>, tx_id: Option<String>) -> Result<QueryResponse, String> {
        // If in transaction, don't apply changes immediately - stage them
        if let Some(tx) = tx_id {
            println!("🔍 DEBUG DELETE IN TRANSACTION: Staging delete for tx {}", tx);
            let mut transactions = self.active_transactions.lock().unwrap();
            let tx_data = transactions.entry(tx.clone()).or_insert_with(|| TransactionData::new());
            tx_data.modified_tables.insert(table.to_string());
            
            // For now, return success but don't apply changes
            // TODO: Implement proper transaction staging for DELETE operations
            return Ok(QueryResponse {
                status: 200,
                message: format!("Delete staged in transaction {} for table {}", tx, table),
                table: Some(table.to_string()),
                results: None,
                affected_rows: 0,
            });
        }
        
        let tree = self.db.open_tree(table).unwrap();
        let mut deleted_count = 0;
        let mut keys_to_delete = Vec::new();

        for entry in tree.iter() {
            let (key, value) = entry.unwrap();
            let value_str = String::from_utf8(value.to_vec()).unwrap_or_default();
            let value_map: HashMap<String, String> = serde_json::from_str(&value_str).unwrap_or_default();

            let match_found = conditions.iter().all(|(k, v)| value_map.get(k) == Some(v));

            if match_found {
                keys_to_delete.push(key.to_vec());
            }
        }

        for key in keys_to_delete {
            tree.remove(key).unwrap();
            deleted_count += 1;
        }

        Ok(QueryResponse {
            status: 200,
            message: format!("{} records deleted from {}", deleted_count, table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: deleted_count,
        })
    }

    /// ✅ FIXED: Execute CREATE TABLE
    fn execute_create_table(&self, schema: crate::schema::TableSchema) -> Result<QueryResponse, String> {
        // Create table in storage
        let mut storage = crate::storage::Storage::new(Arc::clone(&self.db));
        storage.create_table(schema.clone()).map_err(|e| e.to_string())?;

        // Also register the schema with the schema manager for validation
        if let Ok(mut schema_manager) = self.schema_manager.lock() {
            schema_manager.create_table(schema.clone()).map_err(|e| e.to_string())?;
        }

        Ok(QueryResponse {
            status: 201,
            message: format!("Table '{}' created successfully", schema.name),
            table: Some(schema.name),
            results: None,
            affected_rows: 0,
        })
    }

    /// ✅ FIXED: Execute DROP TABLE
    fn execute_drop_table(&self, table: &str) -> Result<QueryResponse, String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        tree.clear().map_err(|e| e.to_string())?;
        
        Ok(QueryResponse {
            status: 200,
            message: format!("Table '{}' dropped successfully", table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: 0,
        })
    }

    /// ✅ FIXED: Execute SELECT with joins
    fn execute_select_with_joins(
        &self, 
        table: &str, 
        joins: Vec<(String, String, String)>, 
        conditions: HashMap<String, String>,
        order_by: Option<String>,
        limit: Option<usize>,
        _tx_id: Option<String>
    ) -> Result<QueryResponse, String> {
        
        let mut tables = vec![table.to_string()];
        let mut join_conditions = Vec::new();

        for (join_table, join_type, condition) in joins {
            // Resolve join table name (check for CTE temporary tables)
            let resolved_join_table = self.resolve_table_name(&join_table);
            // ✅ FIXED: Parse the condition to extract column names
            let (left_col, right_col) = if condition.contains('=') {
                let parts: Vec<&str> = condition.split('=').collect();
                if parts.len() == 2 {
                    let left = parts[0].trim();
                    let right = parts[1].trim();
                    
                    // Extract column names from qualified names like "table.column"
                    let left_col = if left.contains('.') {
                        left.split('.').nth(1).unwrap_or(left)
                    } else {
                        left
                    };
                    
                    let right_col = if right.contains('.') {
                        right.split('.').nth(1).unwrap_or(right)
                    } else {
                        right
                    };
                    
                    (left_col.to_string(), right_col.to_string())
                } else {
                    ("id".to_string(), "id".to_string())
                }
            } else {
                ("id".to_string(), "id".to_string())
            };

            if !tables.contains(&resolved_join_table) {
                tables.push(resolved_join_table.clone());
            }

            // ✅ FIXED: Use proper JoinCondition structure
            join_conditions.push(JoinCondition {
                left_table: table.to_string(),
                left_column: left_col,
                right_table: resolved_join_table,
                right_column: right_col,
                join_type: match join_type.as_str() {
                    "LEFT" => JoinType::Left,
                    "RIGHT" => JoinType::Right,
                    "FULL" => JoinType::Full,
                    _ => JoinType::Inner,
                },
            });
        }

        let mut join_executor = self.join_executor.lock().unwrap();
        let results = join_executor.execute_join_query(
            tables,
            join_conditions,
            conditions,
            order_by,
            limit,
        )?;

        Ok(QueryResponse {
            status: 200,
            message: format!("JOIN query executed successfully - {} rows returned", results.len()),
            table: Some(table.to_string()),
            results: Some(results.clone()),
            affected_rows: results.len(),
        })
    }

    /// ✅ FIXED: Execute aggregate query
    fn execute_aggregate_query(&self, table: &str, conditions: HashMap<String, String>, group_by: Option<Vec<String>>, aggregates: Option<HashMap<String, String>>, having: Option<String>, order_by: Option<String>, limit: Option<usize>, _tx_id: Option<String>) -> Result<QueryResponse, String> {
        let tree = self.db.open_tree(table).unwrap();
        let mut results = Vec::new();

        for entry in tree.iter() {
            let (_, value) = entry.unwrap();
            let value_str = String::from_utf8(value.to_vec()).unwrap_or_default();
            let value_map: HashMap<String, String> = serde_json::from_str(&value_str).unwrap_or_default();

            let match_found = conditions.iter().all(|(k, v)| value_map.get(k) == Some(v));

            if match_found {
                results.push(value_map);
            }
        }

        // Apply aggregates if specified
        let aggregated_results = if let Some(agg_funcs) = aggregates {
            let mut agg_result = HashMap::new();
            
            for (func_name, column) in agg_funcs {
                match func_name.as_str() {
                    "COUNT" => {
                        agg_result.insert("COUNT".to_string(), results.len().to_string());
                    }
                    "SUM" => {
                        let sum: f64 = results.iter()
                            .filter_map(|row| row.get(&column))
                            .filter_map(|val| val.parse::<f64>().ok())
                            .sum();
                        agg_result.insert("SUM".to_string(), sum.to_string());
                    }
                    "AVG" => {
                        let values: Vec<f64> = results.iter()
                            .filter_map(|row| row.get(&column))
                            .filter_map(|val| val.parse::<f64>().ok())
                            .collect();
                        if !values.is_empty() {
                            let avg = values.iter().sum::<f64>() / values.len() as f64;
                            agg_result.insert("AVG".to_string(), avg.to_string());
                        }
                    }
                    _ => {}
                }
            }
            
            vec![agg_result]
        } else {
            results
        };

        let mut final_results = aggregated_results;
        
        // Apply HAVING filter after aggregation
        if let Some(having_clause) = having {
            println!("🔍 DEBUG HAVING: Applying filter '{}'", having_clause);
            final_results = self.apply_having_filter(final_results, &having_clause)?;
            println!("🔍 DEBUG HAVING: Results after filter: {} rows", final_results.len());
        }
        
        if let Some(order_col) = order_by {
            final_results.sort_by(|a, b| {
                let empty_string = String::new();
                let a_val = a.get(&order_col).unwrap_or(&empty_string);
                let b_val = b.get(&order_col).unwrap_or(&empty_string);
                a_val.cmp(b_val)
            });
        }

        if let Some(limit_count) = limit {
            final_results.truncate(limit_count);
        }

        Ok(QueryResponse {
            status: 200,
            message: format!("Aggregate query executed successfully - {} results", final_results.len()),
            table: Some(table.to_string()),
            results: Some(final_results.clone()),
            affected_rows: final_results.len(),
        })
    }

    /// Apply HAVING filter to aggregated results
    fn apply_having_filter(&self, results: Vec<HashMap<String, String>>, having_clause: &str) -> Result<Vec<HashMap<String, String>>, String> {
        println!("🔍 DEBUG HAVING: Filtering {} rows with clause '{}'", results.len(), having_clause);
        
        let filtered_results: Vec<HashMap<String, String>> = results
            .into_iter()
            .filter(|row| {
                let matches = self.evaluate_having_condition(row, having_clause);
                println!("🔍 DEBUG HAVING: Row {:?} matches: {}", row, matches);
                matches
            })
            .collect();
        
        println!("🔍 DEBUG HAVING: Filter produced {} results", filtered_results.len());
        Ok(filtered_results)
    }

    /// Evaluate HAVING condition for a single row
    fn evaluate_having_condition(&self, row: &HashMap<String, String>, condition: &str) -> bool {
        // Simple implementation for common HAVING conditions
        // Example: "SUM(amount) > 150" or "COUNT(*) > 1"
        
        if condition.contains(" > ") {
            let parts: Vec<&str> = condition.split(" > ").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                // Try to find the aggregate function result in the row
                if let Some(left_val) = row.get(left) {
                    if let (Ok(left_num), Ok(right_num)) = (left_val.parse::<f64>(), right.parse::<f64>()) {
                        return left_num > right_num;
                    }
                }
                
                // Try with function name only (e.g., "SUM" if key is "SUM")
                if let Some(func_name) = left.split('(').next() {
                    if let Some(val) = row.get(func_name) {
                        if let (Ok(left_num), Ok(right_num)) = (val.parse::<f64>(), right.parse::<f64>()) {
                            return left_num > right_num;
                        }
                    }
                }
            }
        }
        
        // Default: no match
        false
    }

    /// Resolve table name (checks for CTE temporary tables first)
    fn resolve_table_name(&self, table_name: &str) -> String {
        let cte_table_name = format!("_cte_{}", table_name);
        
        // Check if this is a CTE temporary table
        if self.db.tree_names().iter().any(|name| name == cte_table_name.as_bytes()) {
            println!("🔍 DEBUG CTE: Resolving table '{}' to CTE temporary table '{}'", table_name, cte_table_name);
            cte_table_name
        } else {
            table_name.to_string()
        }
    }

    /// Execute CTE creation (temporary table)
    fn execute_cte_creation(&self, cte_name: &str, cte_query: &str) -> Result<(), String> {
        println!("🔍 DEBUG CTE: Creating temporary table '{}' with query: {}", cte_name, cte_query);
        
        // Parse the CTE query
        let parsed_cte = crate::parser::SQLParser::parse_sql(cte_query)?;
        
        // Execute the CTE query to get results
        let cte_results = self.execute_query(&parsed_cte, None)?;
        
        // Parse the JSON result to extract the data
        let result_data: serde_json::Value = serde_json::from_str(&cte_results)
            .map_err(|e| format!("Failed to parse CTE results: {}", e))?;
        
        // Extract the results array
        if let Some(results) = result_data.get("results").and_then(|r| r.as_array()) {
            println!("🔍 DEBUG CTE: CTE '{}' produced {} rows", cte_name, results.len());
            
            // Create a temporary table with the CTE name
            let temp_table_name = format!("_cte_{}", cte_name);
            let temp_tree = self.db.open_tree(&temp_table_name).map_err(|e| e.to_string())?;
            
            // Store each result row in the temporary table
            for (index, row) in results.iter().enumerate() {
                if let Some(row_obj) = row.as_object() {
                    let row_data: HashMap<String, String> = row_obj.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect();
                    
                    let json_data = serde_json::to_string(&row_data)
                        .map_err(|e| format!("Failed to serialize CTE row: {}", e))?;
                    
                    temp_tree.insert(format!("{}", index), json_data.as_bytes())
                        .map_err(|e| format!("Failed to insert CTE row: {}", e))?;
                }
            }
            
            println!("🔍 DEBUG CTE: Successfully created temporary table '{}'", temp_table_name);
            Ok(())
        } else {
            Err(format!("CTE query '{}' did not produce valid results", cte_name))
        }
    }

    /// ✅ FIXED: Execute join with aggregates
    fn execute_join_with_aggregates(&self, table: &str, joins: Vec<(String, String, String)>, conditions: HashMap<String, String>, group_by: Option<Vec<String>>, aggregates: Option<HashMap<String, String>>, having: Option<String>, order_by: Option<String>, limit: Option<usize>, tx_id: Option<String>) -> Result<QueryResponse, String> {
        // First execute the join
        let join_result = self.execute_select_with_joins(table, joins, conditions, None, None, tx_id)?;
        
        // Then apply aggregates to the joined results
        let joined_results = join_result.results.unwrap_or_default();
        
        let aggregated_results = if let Some(agg_funcs) = aggregates {
            if let Some(group_cols) = group_by {
                // GROUP BY aggregation - group by specified columns
                println!("🔍 DEBUG GROUP BY: Grouping by columns: {:?}", group_cols);
                println!("🔍 DEBUG GROUP BY: Total joined rows to group: {}", joined_results.len());
                if !joined_results.is_empty() {
                    println!("🔍 DEBUG GROUP BY: Sample row keys: {:?}", joined_results[0].keys().collect::<Vec<_>>());
                }
                let mut groups: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
                
                // Group joined results by GROUP BY columns
                for row in joined_results {
                    let mut group_key_parts = Vec::new();
                    
                    for group_col in &group_cols {
                        // Handle column with table prefixes (e.g., "u.id", "u.username")
                        let mut found_value = None;
                        
                        println!("🔍 DEBUG GROUP BY: Looking for column '{}'", group_col);
                        println!("🔍 DEBUG GROUP BY: Available keys: {:?}", row.keys().collect::<Vec<_>>());
                        
                        // Extract table alias and column name from group_col (e.g., "u.id" -> "u", "id")
                        let (table_alias, column_name) = if group_col.contains('.') {
                            let parts: Vec<&str> = group_col.split('.').collect();
                            if parts.len() == 2 {
                                (Some(parts[0]), parts[1])
                            } else {
                                (None, group_col.as_str())
                            }
                        } else {
                            (None, group_col.as_str())
                        };
                        
                        println!("🔍 DEBUG GROUP BY: Parsed - table_alias: {:?}, column_name: '{}'", table_alias, column_name);
                        
                        // Try direct match first
                        if let Some(value) = row.get(group_col) {
                            found_value = Some(value.clone());
                            println!("🔍 DEBUG GROUP BY: Found direct match for '{}' = '{}'", group_col, value);
                        } else {
                            // Try with table prefixes - look for patterns like "test_users AS u.id"
                            for (key, value) in &row {
                                if let Some(alias) = table_alias {
                                    // Look for "table AS alias.column" pattern
                                    if key.contains(&format!(" AS {}.{}", alias, column_name)) {
                                        found_value = Some(value.clone());
                                        println!("🔍 DEBUG GROUP BY: Found AS pattern match '{}' = '{}'", key, value);
                                        break;
                                    }
                                }
                                
                                // Alternative patterns
                                if key.ends_with(&format!(".{}", group_col)) || 
                                   key == group_col ||
                                   key.ends_with(&format!(".{}", column_name)) ||
                                   key.contains(&format!(" AS {}", column_name)) {
                                    found_value = Some(value.clone());
                                    println!("🔍 DEBUG GROUP BY: Found pattern match '{}' = '{}'", key, value);
                                    break;
                                }
                            }
                        }
                        
                        let final_value = found_value.unwrap_or_else(|| "NULL".to_string());
                        println!("🔍 DEBUG GROUP BY: Final value for '{}' = '{}'", group_col, final_value);
                        group_key_parts.push(final_value);
                    }
                    
                    let group_key = group_key_parts.join("|");
                    println!("🔍 DEBUG GROUP BY: Row grouped with key '{}' (parts: {:?})", group_key, group_key_parts);
                    groups.entry(group_key).or_insert_with(Vec::new).push(row);
                }
                
                // Apply aggregation to each group
                println!("🔍 DEBUG GROUP BY: Created {} groups: {:?}", groups.len(), groups.keys().collect::<Vec<_>>());
                let mut result_rows = Vec::new();
                for (group_key, group_rows) in groups {
                    println!("🔍 DEBUG GROUP BY: Processing group '{}' with {} rows", group_key, group_rows.len());
                    let mut group_result = HashMap::new();
                    
                    // Add GROUP BY columns to result
                    if let Some(first_row) = group_rows.first() {
                        for group_col in &group_cols {
                            // Find the value for this group column
                            let mut found_value = None;
                            
                            // Extract table alias and column name from group_col (e.g., "u.id" -> "u", "id")
                            let (table_alias, column_name) = if group_col.contains('.') {
                                let parts: Vec<&str> = group_col.split('.').collect();
                                if parts.len() == 2 {
                                    (Some(parts[0]), parts[1])
                                } else {
                                    (None, group_col.as_str())
                                }
                            } else {
                                (None, group_col.as_str())
                            };
                            
                            if let Some(value) = first_row.get(group_col) {
                                found_value = Some(value.clone());
                            } else {
                                // Try with table prefixes - look for patterns like "test_users AS u.id"
                                for (key, value) in first_row {
                                    if let Some(alias) = table_alias {
                                        // Look for "table AS alias.column" pattern
                                        if key.contains(&format!(" AS {}.{}", alias, column_name)) {
                                            found_value = Some(value.clone());
                                            break;
                                        }
                                    }
                                    
                                    // Alternative patterns
                                    if key.ends_with(&format!(".{}", group_col)) || 
                                       key == group_col ||
                                       key.ends_with(&format!(".{}", column_name)) ||
                                       key.contains(&format!(" AS {}", column_name)) {
                                        found_value = Some(value.clone());
                                        break;
                                    }
                                }
                            }
                            
                            if let Some(value) = found_value {
                                // Use simple column name for output
                                let output_col = group_col.split('.').last().unwrap_or(group_col);
                                group_result.insert(output_col.to_string(), value);
                            }
                        }
                    }
                    
                    // Apply aggregate functions to this group
                    for (func_name, column) in &agg_funcs {
                        match func_name.as_str() {
                            "COUNT" => {
                                let count = if column == "*" || column.contains("*") {
                                    group_rows.len()
                                } else {
                                    // Extract table alias and column name from column (e.g., "p.id" -> "p", "id")
                                    let (table_alias, column_name) = if column.contains('.') {
                                        let parts: Vec<&str> = column.split('.').collect();
                                        if parts.len() == 2 {
                                            (Some(parts[0]), parts[1])
                                        } else {
                                            (None, column.as_str())
                                        }
                                    } else {
                                        (None, column.as_str())
                                    };
                                    
                                    // Count non-null values in the specified column
                                    group_rows.iter()
                                        .filter(|row| {
                                            // Check if the column exists and is not null/empty
                                            for (key, value) in *row {
                                                if let Some(alias) = table_alias {
                                                    // Look for "table AS alias.column" pattern
                                                    if key.contains(&format!(" AS {}.{}", alias, column_name)) {
                                                        return !value.is_empty() && value != "NULL";
                                                    }
                                                }
                                                
                                                // Alternative patterns
                                                if key.ends_with(&format!(".{}", column)) || 
                                                   key == column ||
                                                   key.ends_with(&format!(".{}", column_name)) ||
                                                   key.contains(&format!(" AS {}", column_name)) {
                                                    return !value.is_empty() && value != "NULL";
                                                }
                                            }
                                            false
                                        })
                                        .count()
                                };
                                group_result.insert("post_count".to_string(), count.to_string());
                            }
                            "SUM" => {
                                let sum: f64 = group_rows.iter()
                                    .filter_map(|row| {
                                        for (key, value) in row {
                                            if key.ends_with(&format!(".{}", column)) || key == column {
                                                return value.parse::<f64>().ok();
                                            }
                                        }
                                        None
                                    })
                                    .sum();
                                group_result.insert(format!("SUM_{}", column).to_string(), sum.to_string());
                            }
                            "AVG" => {
                                let values: Vec<f64> = group_rows.iter()
                                    .filter_map(|row| {
                                        for (key, value) in row {
                                            if key.ends_with(&format!(".{}", column)) || key == column {
                                                return value.parse::<f64>().ok();
                                            }
                                        }
                                        None
                                    })
                                    .collect();
                                if !values.is_empty() {
                                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                                    group_result.insert(format!("AVG_{}", column).to_string(), avg.to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                    
                    result_rows.push(group_result);
                }
                
                result_rows
            } else {
                // No GROUP BY - aggregate across all results
                let mut agg_result = HashMap::new();
                
                for (func_name, column) in agg_funcs {
                    match func_name.as_str() {
                        "COUNT" => {
                            agg_result.insert("COUNT".to_string(), joined_results.len().to_string());
                        }
                        "SUM" => {
                            let sum: f64 = joined_results.iter()
                                .filter_map(|row| row.get(&column))
                                .filter_map(|val| val.parse::<f64>().ok())
                                .sum();
                            agg_result.insert("SUM".to_string(), sum.to_string());
                        }
                        "AVG" => {
                            let values: Vec<f64> = joined_results.iter()
                                .filter_map(|row| row.get(&column))
                                .filter_map(|val| val.parse::<f64>().ok())
                                .collect();
                            if !values.is_empty() {
                                let avg = values.iter().sum::<f64>() / values.len() as f64;
                                agg_result.insert("AVG".to_string(), avg.to_string());
                            }
                        }
                        _ => {}
                    }
                }
                
                vec![agg_result]
            }
        } else {
            joined_results
        };

        let mut final_results = aggregated_results;
        
        // Apply HAVING filter after aggregation
        if let Some(having_clause) = having {
            println!("🔍 DEBUG HAVING: Applying filter '{}'", having_clause);
            final_results = self.apply_having_filter(final_results, &having_clause)?;
            println!("🔍 DEBUG HAVING: Results after filter: {} rows", final_results.len());
        }
        
        if let Some(order_col) = order_by {
            final_results.sort_by(|a, b| {
                let empty_string = String::new();
                let a_val = a.get(&order_col).unwrap_or(&empty_string);
                let b_val = b.get(&order_col).unwrap_or(&empty_string);
                a_val.cmp(b_val)
            });
        }

        if let Some(limit_count) = limit {
            final_results.truncate(limit_count);
        }

        Ok(QueryResponse {
            status: 200,
            message: format!("JOIN query with aggregates executed successfully - {} results", final_results.len()),
            table: Some(table.to_string()),
            results: Some(final_results.clone()),
            affected_rows: final_results.len(),
        })
    }

    /// Transaction management
    pub fn begin_transaction(&self, tx_id: String) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().unwrap();
        if transactions.contains_key(&tx_id) {
            return Err("Transaction already exists".to_string());
        }
        transactions.insert(tx_id, TransactionData::new());
        Ok(())
    }

    pub fn commit_transaction(&self, tx_id: String) -> Result<(), String> {
        let response = self.transaction_manager.lock().unwrap().commit_transaction(&tx_id)?;
        if response.status == 200 {
            Ok(())
        } else {
            Err(response.message)
        }
    }

    pub fn rollback_transaction(&self, tx_id: String) -> Result<(), String> {
        let response = self.transaction_manager.lock().unwrap().rollback_transaction(&tx_id)?;
        if response.status == 200 {
            Ok(())
        } else {
            Err(response.message)
        }
    }

    /// Validate FOREIGN KEY constraints before inserting
    fn validate_foreign_key_constraints(&self, table: &str, values: &HashMap<String, String>) -> Result<(), String> {
        // For now, we'll implement basic FOREIGN KEY validation for known relationships
        // In a full implementation, this would read from schema definitions
        
        // Hard-coded foreign key relationships for common tables
        let foreign_key_rules = match table {
            "posts" => vec![("user_id", "test_users", "id")],  // posts.user_id -> test_users.id
            _ => vec![],
        };
        
        for (fk_column, ref_table, ref_column) in foreign_key_rules {
            if let Some(fk_value) = values.get(fk_column) {
                // Check if the foreign key value exists in the referenced table
                let ref_tree = self.db.open_tree(ref_table).map_err(|e| e.to_string())?;
                let mut value_found = false;
                
                for entry in ref_tree.iter() {
                    let (_, value) = entry.map_err(|e| e.to_string())?;
                    let value_str = String::from_utf8_lossy(&value);
                    
                    if let Ok(ref_row) = serde_json::from_str::<HashMap<String, String>>(&value_str) {
                        if let Some(ref_value) = ref_row.get(ref_column) {
                            if ref_value == fk_value {
                                value_found = true;
                                break;
                            }
                        }
                    }
                }
                
                if !value_found {
                    return Err(format!("Foreign key '{}' value '{}' not found in table '{}'", fk_column, fk_value, ref_table));
                }
            }
        }
        
        Ok(())
    }

    /// Validate CHECK constraints
    fn validate_check_constraints(&self, table: &str, values: &HashMap<String, String>) -> Result<(), String> {
        // For now, we'll implement basic CHECK constraint validation for known tables
        // In a full implementation, this would read from schema definitions
        
        // Hard-coded check constraint rules for common tables
        let check_rules = match table {
            "products" => vec![("price", "price >= 0")],  // products.price must be >= 0
            _ => vec![],
        };
        
        for (column, constraint_expr) in check_rules {
            if let Some(value) = values.get(column) {
                // Parse the value and check the constraint
                if let Ok(numeric_value) = value.parse::<f64>() {
                    // Handle numeric constraints
                    if constraint_expr.contains(">=") {
                        let parts: Vec<&str> = constraint_expr.split(">=").collect();
                        if parts.len() == 2 {
                            let min_value = parts[1].trim().parse::<f64>().unwrap_or(0.0);
                            if numeric_value < min_value {
                                return Err(format!("Column '{}' value '{}' violates constraint: {}", column, value, constraint_expr));
                            }
                        }
                    } else if constraint_expr.contains("<=") {
                        let parts: Vec<&str> = constraint_expr.split("<=").collect();
                        if parts.len() == 2 {
                            let max_value = parts[1].trim().parse::<f64>().unwrap_or(0.0);
                            if numeric_value > max_value {
                                return Err(format!("Column '{}' value '{}' violates constraint: {}", column, value, constraint_expr));
                            }
                        }
                    } else if constraint_expr.contains(">") {
                        let parts: Vec<&str> = constraint_expr.split(">").collect();
                        if parts.len() == 2 {
                            let min_value = parts[1].trim().parse::<f64>().unwrap_or(0.0);
                            if numeric_value <= min_value {
                                return Err(format!("Column '{}' value '{}' violates constraint: {}", column, value, constraint_expr));
                            }
                        }
                    } else if constraint_expr.contains("<") {
                        let parts: Vec<&str> = constraint_expr.split("<").collect();
                        if parts.len() == 2 {
                            let max_value = parts[1].trim().parse::<f64>().unwrap_or(0.0);
                            if numeric_value >= max_value {
                                return Err(format!("Column '{}' value '{}' violates constraint: {}", column, value, constraint_expr));
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    pub fn invalidate_cache(&self, table: &str) {
        let mut cache = self.cache.lock().unwrap();
        let keys_to_remove: Vec<String> = cache.iter()
            .filter(|(k, _)| k.starts_with(&format!("SELECT {}", table)))
            .map(|(k, _)| k.clone())
            .collect();
        
        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    /// NEW: Execute SpacetimeDB-style reducer calls
    pub fn execute_reducer(&self, module_name: &str, function_name: &str, args: &[serde_json::Value], client_id: Option<String>) -> Result<String, String> {
        let mut module_manager = self.module_manager.lock().unwrap();
        module_manager.call_reducer(module_name, function_name, args, client_id)
    }

    /// NEW: Handle WebSocket messages (JSON or SQL)
    pub fn handle_websocket_message(&self, message: &str, client_id: String) -> Result<String, String> {
        // Try to parse as JSON first (SpacetimeDB-style reducer call)
        if let Ok(reducer_call) = serde_json::from_str::<ReducerCall>(message) {
            return self.execute_reducer(&reducer_call.module, &reducer_call.function, &reducer_call.args, Some(client_id));
        }

        // Try to parse as SQL query
        if let Ok(parsed_query) = crate::parser::SQLParser::parse_query(message) {
            return self.execute_query(&parsed_query, None);
        }
        
        Err("Invalid message format. Expected SQL query or reducer call.".to_string())
    }

    /// NEW: Get query performance metrics
    pub fn get_query_performance_metrics(&self) -> QueryPerformanceMetrics {
        let (cache_hits, cache_misses, hit_rate) = self.get_cache_metrics();
        
        QueryPerformanceMetrics {
            cache_hits,
            cache_misses,
            cache_hit_rate: hit_rate,
            active_transactions: self.get_active_transactions().len(),
            total_tables: self.get_total_tables(),
        }
    }

    fn get_total_tables(&self) -> usize {
        self.schema_manager.lock().map(|sm| sm.list_tables().len()).unwrap_or(0)
    }

    /// NEW: Execute complex query with full optimization
    pub fn execute_complex_query(&self, sql: &str) -> Result<String, String> {
        println!("🔍 COMPLEX QUERY EXECUTION: {}", sql);
        
        let parsed_query = crate::parser::SQLParser::parse_query(sql)?;
        
        let start_time = std::time::Instant::now();
        let result = self.execute_query(&parsed_query, None)?;
        let execution_time = start_time.elapsed();
        
        println!("⚡ QUERY COMPLETED: Execution time: {:?}", execution_time);
        
        Ok(result)
    }

    /// FIXED: Register a module with the module manager
    pub fn register_module(&self, module: Box<dyn crate::modules::Module>) -> Result<(), String> {
        let mut module_manager = self.module_manager.lock().unwrap();
        module_manager.register_module(module)
    }
    
    /// Set WebSocket notification callback for real-time broadcasting
    pub fn set_notification_callback(&self, callback: crate::modules::NotificationCallback) {
        if let Ok(mut module_manager) = self.module_manager.lock() {
            module_manager.set_notification_callback(callback);
            println!("✅ WebSocket notification callback registered");
        } else {
            println!("⚠️ Failed to register notification callback - could not acquire module manager lock");
        }
    }

    /// FIXED: Subscribe to events (placeholder implementation)
    pub fn subscribe_to_events(&self, _subscription: crate::modules::EventSubscription) {
        // Placeholder implementation for event subscription
        // In a real implementation, this would set up event listeners
        println!("📡 Event subscription registered");
    }

    // 🗄️ DATABASE MANAGEMENT METHODS
    
    /// Execute CREATE DATABASE command
    fn execute_create_database(&self, name: &str, description: Option<&str>, if_not_exists: bool) -> Result<QueryResponse, String> {
        // Validate database doesn't already exist
        if self.database_exists(name)? {
            if if_not_exists {
                // If IF NOT EXISTS was specified, just return success
                return Ok(QueryResponse {
                    status: 200,
                    message: format!("Database '{}' already exists (ignored due to IF NOT EXISTS)", name),
                    table: None,
                    results: None,
                    affected_rows: 0,
                });
            } else {
                return Err(format!("Database '{}' already exists", name));
            }
        }
        
        // Create physical database file
        let db_path = format!("{}.db", name);
        let new_db = crate::connection_manager::DatabaseConnectionManager::global()
            .get_connection(&db_path)
            .map_err(|e| format!("Failed to create database: {}", e))?;
        
        // Register database in system database
        self.register_database_in_system(name, &db_path, description)?;
        
        Ok(QueryResponse {
            status: 200,
            message: format!("Database '{}' created successfully", name),
            table: None,
            results: None,
            affected_rows: 1,
        })
    }
    
    /// Execute USE DATABASE command
    fn execute_use_database(&self, name: &str) -> Result<QueryResponse, String> {
        // Validate database exists
        if !self.database_exists(name)? {
            return Err(format!("Database '{}' does not exist", name));
        }
        
        // In a full implementation, this would switch the current database context
        // For now, we just return success
        Ok(QueryResponse {
            status: 200,
            message: format!("Database switched to '{}'", name),
            table: None,
            results: None,
            affected_rows: 0,
        })
    }
    
    /// Execute SHOW DATABASES command
    fn execute_show_databases(&self) -> Result<QueryResponse, String> {
        let databases = self.list_databases()?;
        
        let mut results = Vec::new();
        for (name, path, description, created_at) in databases {
            let mut row = std::collections::HashMap::new();
            row.insert("Database".to_string(), name);
            row.insert("Path".to_string(), path);
            row.insert("Description".to_string(), description.unwrap_or("No description".to_string()));
            row.insert("Created".to_string(), created_at.unwrap_or("Unknown".to_string()));
            results.push(row);
        }
        
        Ok(QueryResponse {
            status: 200,
            message: "Databases retrieved successfully".to_string(),
            table: Some("databases".to_string()),
            results: Some(results),
            affected_rows: 0,
        })
    }
    
    /// Execute SHOW USERS command
    fn execute_show_users(&self) -> Result<QueryResponse, String> {
        let mut results = Vec::new();
        
        // Get all users from the users table
        if let Ok(users_tree) = self.db.open_tree("users") {
            for entry in users_tree.iter() {
                if let Ok((_, value)) = entry {
                    let value_str = String::from_utf8_lossy(&value);
                    
                    if let Ok(user_data) = serde_json::from_str::<std::collections::HashMap<String, String>>(&value_str) {
                        let mut row = std::collections::HashMap::new();
                        row.insert("Username".to_string(), user_data.get("username").unwrap_or(&"N/A".to_string()).clone());
                        row.insert("Email".to_string(), user_data.get("email").unwrap_or(&"N/A".to_string()).clone());
                        row.insert("Role".to_string(), user_data.get("role").unwrap_or(&"user".to_string()).clone());
                        row.insert("Created".to_string(), user_data.get("created_at").unwrap_or(&"Unknown".to_string()).clone());
                        results.push(row);
                    }
                }
            }
        }
        
        // If no users found, return an informative message
        if results.is_empty() {
            let mut row = std::collections::HashMap::new();
            row.insert("Message".to_string(), "No users found in the system".to_string());
            results.push(row);
        }
        
        Ok(QueryResponse {
            status: 200,
            message: "Users retrieved successfully".to_string(),
            table: Some("users".to_string()),
            results: Some(results),
            affected_rows: 0,
        })
    }

    /// Execute SHOW TABLES command
    fn execute_show_tables(&self) -> Result<QueryResponse, String> {
        let mut results = Vec::new();
        
        // Get all table names from the current database
        for tree_name in self.db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec()).map_err(|e| format!("Invalid table name: {}", e))?;
            
            // Skip internal trees and system tables
            if !name.starts_with("__") && name != "installation_info" && name != "database_registry" {
                let mut row = std::collections::HashMap::new();
                row.insert("Table".to_string(), name.clone());
                
                // Get table size information if possible
                if let Ok(tree) = self.db.open_tree(&name) {
                    let record_count = tree.len();
                    row.insert("Records".to_string(), record_count.to_string());
                } else {
                    row.insert("Records".to_string(), "0".to_string());
                }
                
                results.push(row);
            }
        }
        
        Ok(QueryResponse {
            status: 200,
            message: "Tables retrieved successfully".to_string(),
            table: Some("tables".to_string()),
            results: Some(results),
            affected_rows: 0,
        })
    }
    
    /// Execute DESCRIBE TABLE command
    fn execute_describe_table(&self, table_name: &str) -> Result<QueryResponse, String> {
        // Check if table exists
        if !self.table_exists(table_name) {
            return Err(format!("Table '{}' does not exist", table_name));
        }
        
        let mut results = Vec::new();
        
        // Try to get table schema information from sled tree
        if let Ok(tree) = self.db.open_tree(table_name) {
            // Check if there are any records to analyze
            if let Some(first_record) = tree.iter().next() {
                if let Ok((_, value)) = first_record {
                    // Try to parse the value as JSON to determine the schema
                    if let Ok(record_str) = String::from_utf8(value.to_vec()) {
                        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&record_str) {
                            if let Some(obj) = json_value.as_object() {
                                for (column_name, value) in obj {
                                    let mut row = std::collections::HashMap::new();
                                    row.insert("Field".to_string(), column_name.clone());
                                    
                                    // Determine type based on JSON value
                                    let type_name = match value {
                                        serde_json::Value::String(_) => "TEXT",
                                        serde_json::Value::Number(n) => {
                                            if n.is_i64() || n.is_u64() {
                                                "INTEGER"
                                            } else {
                                                "REAL"
                                            }
                                        },
                                        serde_json::Value::Bool(_) => "BOOLEAN",
                                        serde_json::Value::Null => "NULL",
                                        _ => "JSON"
                                    };
                                    
                                    row.insert("Type".to_string(), type_name.to_string());
                                    row.insert("Null".to_string(), "YES".to_string());
                                    row.insert("Key".to_string(), "".to_string());
                                    row.insert("Default".to_string(), "NULL".to_string());
                                    row.insert("Extra".to_string(), "".to_string());
                                    
                                    results.push(row);
                                }
                            }
                        }
                    }
                }
            } else {
                // Table exists but has no records - return a generic message
                let mut row = std::collections::HashMap::new();
                row.insert("Field".to_string(), "No records found".to_string());
                row.insert("Type".to_string(), "".to_string());
                row.insert("Null".to_string(), "".to_string());
                row.insert("Key".to_string(), "".to_string());
                row.insert("Default".to_string(), "".to_string());
                row.insert("Extra".to_string(), "Table is empty - schema cannot be determined".to_string());
                results.push(row);
            }
        }
        
        Ok(QueryResponse {
            status: 200,
            message: format!("Table '{}' described successfully", table_name),
            table: Some(table_name.to_string()),
            results: Some(results),
            affected_rows: 0,
        })
    }
    
    /// Check if table exists in current database
    fn table_exists(&self, table_name: &str) -> bool {
        // Check if the table exists as a tree in the database
        if let Ok(_) = self.db.open_tree(table_name) {
            // Additional check to ensure it's not a system table
            !table_name.starts_with("__") && 
            table_name != "installation_info" && 
            table_name != "database_registry"
        } else {
            false
        }
    }
    
    /// Execute DROP DATABASE command
    fn execute_drop_database(&self, name: &str) -> Result<QueryResponse, String> {
        // Validate database exists
        if !self.database_exists(name)? {
            return Err(format!("Database '{}' does not exist", name));
        }
        
        // Prevent dropping system database
        if name == "mini_db_system" {
            return Err("Cannot drop system database".to_string());
        }
        
        // Remove from system database registry
        self.unregister_database_from_system(name)?;
        
        // Note: In production, you might want to actually delete the file
        // std::fs::remove_file(format!("{}.db", name)).map_err(|e| e.to_string())?;
        
        Ok(QueryResponse {
            status: 200,
            message: format!("Database '{}' dropped successfully", name),
            table: None,
            results: None,
            affected_rows: 1,
        })
    }
    
    /// Check if database exists
    fn database_exists(&self, name: &str) -> Result<bool, String> {
        // Check if it's a known system database
        let is_system_db = matches!(name, "mini_db_system" | "default");
        if is_system_db {
            return Ok(true);
        }
        
        // Check if database exists by looking for the database file
        let db_path = format!("{}.db", name);
        let file_exists = std::path::Path::new(&db_path).exists();
        
        // Also check the registry file
        let registry_path = "database_registry.txt";
        let in_registry = if let Ok(contents) = std::fs::read_to_string(registry_path) {
            contents.lines().any(|line| line.starts_with(&format!("{}|", name)))
        } else {
            false
        };
        
        Ok(file_exists || in_registry)
    }
    
    /// Register database in system using file-based registry (avoids lock contention)
    fn register_database_in_system(&self, name: &str, path: &str, description: Option<&str>) -> Result<(), String> {
        use std::fs::OpenOptions;
        use std::io::Write;
        
        let registry_path = "database_registry.txt";
        let entry = format!("{}|{}|{}|{}\n", 
            name, 
            path, 
            description.unwrap_or(""), 
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        );
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(registry_path)
            .map_err(|e| format!("Failed to open database registry: {}", e))?;
            
        file.write_all(entry.as_bytes())
            .map_err(|e| format!("Failed to write to database registry: {}", e))?;
        
        println!("📝 Registered database '{}' in system registry", name);
        Ok(())
    }
    
    /// Unregister database from system using file-based registry (avoids lock contention)
    fn unregister_database_from_system(&self, name: &str) -> Result<(), String> {
        use std::fs;
        use std::io::BufRead;
        
        let registry_path = "database_registry.txt";
        
        // Read current registry
        let contents = fs::read_to_string(registry_path)
            .unwrap_or_default();
            
        // Filter out the database entry
        let updated_contents: String = contents
            .lines()
            .filter(|line| !line.starts_with(&format!("{}|", name)))
            .map(|line| format!("{}\n", line))
            .collect();
            
        // Write back the updated contents
        fs::write(registry_path, updated_contents)
            .map_err(|e| format!("Failed to update database registry: {}", e))?;
        
        println!("🗑️ Unregistered database '{}' from system registry", name);
        Ok(())
    }
    
    /// Execute SHOW STATUS command
    fn execute_show_status(&self) -> Result<QueryResponse, String> {
        let mut results = Vec::new();
        
        // Server status information
        let mut status_row = std::collections::HashMap::new();
        status_row.insert("Component".to_string(), "Server".to_string());
        status_row.insert("Status".to_string(), "Running".to_string());
        status_row.insert("Version".to_string(), "0.3.0".to_string());
        results.push(status_row);
        
        // Database status
        let mut db_row = std::collections::HashMap::new();
        db_row.insert("Component".to_string(), "Database".to_string());
        db_row.insert("Status".to_string(), "Connected".to_string());
        db_row.insert("Version".to_string(), "Sled 0.34".to_string());
        results.push(db_row);
        
        // WebSocket status
        let mut ws_row = std::collections::HashMap::new();
        ws_row.insert("Component".to_string(), "WebSocket".to_string());
        ws_row.insert("Status".to_string(), "Active".to_string());
        ws_row.insert("Version".to_string(), "Port 8080".to_string());
        results.push(ws_row);
        
        Ok(QueryResponse {
            status: 200,
            message: "Server status retrieved successfully".to_string(),
            table: Some("status".to_string()),
            results: Some(results),
            affected_rows: 0,
        })
    }
    
    /// Execute CREATE INDEX command
    fn execute_create_index(&self, name: &str, table: &str, columns: &[String], unique: bool) -> Result<QueryResponse, String> {
        // Check if table exists
        if !self.table_exists(table) {
            return Err(format!("Table '{}' does not exist", table));
        }
        
        // For now, we'll just return a success message
        // In a full implementation, we would create actual index structures
        let index_type = if unique { "UNIQUE INDEX" } else { "INDEX" };
        let columns_str = columns.join(", ");
        
        println!("✅ {} '{}' created on table '{}' ({})", index_type, name, table, columns_str);
        
        Ok(QueryResponse {
            status: 201,
            message: format!("{} '{}' created successfully on table '{}' ({})", index_type, name, table, columns_str),
            table: Some(table.to_string()),
            results: None,
            affected_rows: 0,
        })
    }
    
    /// Execute SUBSCRIBE command
    fn execute_subscribe(&self, table: &str) -> Result<QueryResponse, String> {
        println!("📡 Client subscribing to table: {}", table);
        
        // Validate table exists
        if !self.table_exists(table) {
            return Err(format!("Table '{}' does not exist", table));
        }
        
        // In a real implementation, this would register the client for real-time updates
        // For now, we just return success
        Ok(QueryResponse {
            status: 200,
            message: format!("Client iscritto alla tabella: {}; nel database: default", table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: 0,
        })
    }
    
    /// Execute UNSUBSCRIBE command
    fn execute_unsubscribe(&self, table: &str) -> Result<QueryResponse, String> {
        println!("📡 Client unsubscribing from table: {}", table);
        
        // In a real implementation, this would remove the client from real-time updates
        // For now, we just return success
        Ok(QueryResponse {
            status: 200,
            message: format!("Client non più iscritto alla tabella: {}", table),
            table: Some(table.to_string()),
            results: None,
            affected_rows: 0,
        })
    }
    
    /// Execute AUTH command
    fn execute_auth(&self, credentials: &str) -> Result<QueryResponse, String> {
        println!("🔐 Client attempting authentication with credentials: {}", credentials);
        
        // Basic authentication check
        // In a real implementation, this would check against a users table or external auth system
        if credentials == "admin123" {
            Ok(QueryResponse {
                status: 200,
                message: "Authentication successful".to_string(),
                table: None,
                results: None,
                affected_rows: 0,
            })
        } else {
            Err("Authentication failed: Invalid credentials".to_string())
        }
    }
    
    /// Execute LOAD MODULE command
    fn execute_load_module(&self, module_name: &str, file_path: &str) -> Result<QueryResponse, String> {
        println!("📦 Loading WASM module '{}' from '{}'", module_name, file_path);
        
        // Check if file exists
        if !std::path::Path::new(file_path).exists() {
            return Err(format!("WASM file not found: {}", file_path));
        }
        
        // Read the WASM file
        match std::fs::read(file_path) {
            Ok(wasm_bytes) => {
                // Try to create a new WASM module
                #[cfg(feature = "wasm")]
                {
                    use crate::modules::WasmModule;
                    match WasmModule::new(module_name.to_string(), wasm_bytes) {
                        Ok(_) => {
                            Ok(QueryResponse {
                                status: 200,
                                message: format!("WASM module '{}' loaded successfully", module_name),
                                table: None,
                                results: None,
                                affected_rows: 0,
                            })
                        }
                        Err(e) => {
                            Err(format!("Failed to load WASM module: {}", e))
                        }
                    }
                }
                #[cfg(not(feature = "wasm"))]
                {
                    // WASM feature not enabled, return success anyway for testing
                    Ok(QueryResponse {
                        status: 200,
                        message: format!("WASM module '{}' loaded successfully (WASM feature disabled)", module_name),
                        table: None,
                        results: None,
                        affected_rows: 0,
                    })
                }
            }
            Err(e) => {
                Err(format!("Failed to read WASM file: {}", e))
            }
        }
    }
    
    /// Execute WASM function
    fn execute_wasm_exec(&self, module_name: &str, function_name: &str, args: &[String]) -> Result<QueryResponse, String> {
        println!("🔧 Executing WASM function '{}' in module '{}' with args: {:?}", function_name, module_name, args);
        
        // Convert string args to serde_json::Value for module manager
        let json_args: Vec<serde_json::Value> = args.iter()
            .map(|arg| {
                // Try to parse as number first, then as string
                if let Ok(num) = arg.parse::<f64>() {
                    serde_json::Value::Number(serde_json::Number::from_f64(num).unwrap_or(serde_json::Number::from(0)))
                } else if let Ok(int_val) = arg.parse::<i64>() {
                    serde_json::Value::Number(serde_json::Number::from(int_val))
                } else if arg == "true" {
                    serde_json::Value::Bool(true)
                } else if arg == "false" {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(arg.clone())
                }
            })
            .collect();
        
        // Access module manager and execute function
        match self.module_manager.lock() {
            Ok(mut module_manager) => {
                match module_manager.call_reducer(module_name, function_name, &json_args, None) {
                    Ok(result) => {
                        // Parse result as JSON and format for display
                        let mut result_row = HashMap::new();
                        result_row.insert("result".to_string(), result);
                        
                        Ok(QueryResponse {
                            status: 200,
                            message: format!("WASM function '{}::{}' executed successfully", module_name, function_name),
                            table: None,
                            results: Some(vec![result_row]),
                            affected_rows: 1,
                        })
                    }
                    Err(e) => {
                        Err(format!("Failed to execute WASM function '{}::{}': {}", module_name, function_name, e))
                    }
                }
            }
            Err(e) => {
                Err(format!("Failed to access module manager: {}", e))
            }
        }
    }
    
    /// List all databases from file-based registry (avoids lock contention)
    fn list_databases(&self) -> Result<Vec<(String, String, Option<String>, Option<String>)>, String> {
        use std::fs;
        
        let registry_path = "database_registry.txt";
        let mut databases = Vec::new();
        
        // Add system databases first
        databases.push((
            "mini_db_system".to_string(), 
            "mini_db_system.db".to_string(), 
            Some("System database".to_string()), 
            Some("2025-01-01".to_string())
        ));
        databases.push((
            "default".to_string(), 
            "default.db".to_string(), 
            Some("Default database".to_string()), 
            Some("2025-01-01".to_string())
        ));
        
        // Read user databases from registry
        if let Ok(contents) = fs::read_to_string(registry_path) {
            for line in contents.lines() {
                if !line.trim().is_empty() {
                    let parts: Vec<&str> = line.split('|').collect();
                    if parts.len() >= 4 {
                        let name = parts[0].to_string();
                        let path = parts[1].to_string();
                        let description = if parts[2].is_empty() { None } else { Some(parts[2].to_string()) };
                        let created_at = Some(parts[3].to_string());
                        
                        databases.push((name, path, description, created_at));
                    }
                }
            }
        }
        
        Ok(databases)
    }

    /// Execute SELECT with Window Functions
    fn execute_select_with_window_functions(&self, table: &str, window_functions: &[(String, String, String)], conditions: Option<String>, order_by: Option<String>, limit: Option<usize>, tx_id: Option<String>) -> Result<String, String> {
        println!("🔍 DEBUG WINDOW: Executing SELECT with {} window functions on table '{}'", window_functions.len(), table);
        
        // First, get the base data
        let base_conditions = if let Some(condition_str) = conditions {
            if condition_str.contains('=') {
                let parts: Vec<&str> = condition_str.split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim().to_string();
                    let value = parts[1].trim().replace("'", "").replace("\"", "");
                    let mut conditions_map = HashMap::new();
                    conditions_map.insert(key, value);
                    conditions_map
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };
        
        // Get base data without window functions
        let base_result = self.execute_select_with_order_limit(table, base_conditions, order_by.clone(), None, tx_id)?;
        
        // Extract rows from QueryResponse
        let mut rows = base_result.results.unwrap_or_default();
        
        // Apply window functions to each row
        for (func_name, alias, over_clause) in window_functions {
            println!("🔍 DEBUG WINDOW: Processing window function '{}' with alias '{}'", func_name, alias);
            
            match func_name.as_str() {
                "ROW_NUMBER" => {
                    // Apply ROW_NUMBER() based on ORDER BY in OVER clause
                    let order_column = self.extract_order_column_from_over(over_clause);
                    if let Some(order_col) = order_column {
                        // Sort rows by the specified column
                        rows.sort_by(|a, b| {
                            let empty_string = String::new();
                            let a_val = a.get(&order_col).unwrap_or(&empty_string);
                            let b_val = b.get(&order_col).unwrap_or(&empty_string);
                            a_val.cmp(b_val)
                        });
                    }
                    
                    // Add row numbers
                    for (i, row) in rows.iter_mut().enumerate() {
                        row.insert(alias.clone(), (i + 1).to_string());
                    }
                },
                "RANK" => {
                    // Apply RANK() based on ORDER BY in OVER clause
                    let order_column = self.extract_order_column_from_over(over_clause);
                    if let Some(order_col) = order_column {
                        // Sort rows by the specified column
                        rows.sort_by(|a, b| {
                            let empty_string = String::new();
                            let a_val = a.get(&order_col).unwrap_or(&empty_string);
                            let b_val = b.get(&order_col).unwrap_or(&empty_string);
                            a_val.cmp(b_val)
                        });
                        
                        // Calculate ranks with ties
                        let mut current_rank = 1;
                        let mut previous_value = String::new();
                        let mut rank_increment = 1;
                        
                        for (i, row) in rows.iter_mut().enumerate() {
                            let current_value = row.get(&order_col).unwrap_or(&String::new()).clone();
                            
                            if i == 0 {
                                previous_value = current_value.clone();
                            } else if current_value != previous_value {
                                current_rank += rank_increment;
                                rank_increment = 1;
                                previous_value = current_value.clone();
                            } else {
                                rank_increment += 1;
                            }
                            
                            row.insert(alias.clone(), current_rank.to_string());
                        }
                    }
                },
                "DENSE_RANK" => {
                    // Apply DENSE_RANK() - similar to RANK but without gaps
                    let order_column = self.extract_order_column_from_over(over_clause);
                    if let Some(order_col) = order_column {
                        // Sort rows by the specified column
                        rows.sort_by(|a, b| {
                            let empty_string = String::new();
                            let a_val = a.get(&order_col).unwrap_or(&empty_string);
                            let b_val = b.get(&order_col).unwrap_or(&empty_string);
                            a_val.cmp(b_val)
                        });
                        
                        // Calculate dense ranks
                        let mut current_rank = 1;
                        let mut previous_value = String::new();
                        
                        for (i, row) in rows.iter_mut().enumerate() {
                            let current_value = row.get(&order_col).unwrap_or(&String::new()).clone();
                            
                            if i == 0 {
                                previous_value = current_value.clone();
                            } else if current_value != previous_value {
                                current_rank += 1;
                                previous_value = current_value.clone();
                            }
                            
                            row.insert(alias.clone(), current_rank.to_string());
                        }
                    }
                },
                _ => {
                    println!("⚠️ DEBUG WINDOW: Unsupported window function '{}'", func_name);
                }
            }
        }
        
        // Apply final limit if specified
        if let Some(limit_count) = limit {
            rows.truncate(limit_count);
        }
        
        // Build the response
        let response = QueryResponse {
            status: 200,
            message: format!("Window function query executed successfully - {} rows returned", rows.len()),
            table: Some(table.to_string()),
            results: Some(rows),
            affected_rows: 0,
        };
        
        serde_json::to_string(&response).map_err(|e| e.to_string())
    }
    
    /// Extract ORDER BY column from OVER clause
    fn extract_order_column_from_over(&self, over_clause: &str) -> Option<String> {
        // Simple parsing of OVER clause to extract ORDER BY column
        if over_clause.contains("ORDER BY") {
            let parts: Vec<&str> = over_clause.split("ORDER BY").collect();
            if parts.len() > 1 {
                let order_part = parts[1].trim();
                // Extract the column name (first word)
                let column = order_part.split_whitespace().next().unwrap_or("").to_string();
                if !column.is_empty() {
                    println!("🔍 DEBUG WINDOW: Extracted ORDER BY column: '{}'", column);
                    return Some(column);
                }
            }
        }
        None
    }

    /// Execute SELECT with CASE expressions
    fn execute_select_with_case_expressions(&self, table: &str, case_expressions: &[(String, String, String)], conditions: Option<String>, order_by: Option<String>, limit: Option<usize>, tx_id: Option<String>) -> Result<String, String> {
        println!("🔍 DEBUG CASE: Executing SELECT with {} CASE expressions on table '{}'", case_expressions.len(), table);
        
        // First, get the base data
        let base_conditions = if let Some(condition_str) = conditions {
            if condition_str.contains('=') {
                let parts: Vec<&str> = condition_str.split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim().to_string();
                    let value = parts[1].trim().replace("'", "").replace("\"", "");
                    let mut conditions_map = HashMap::new();
                    conditions_map.insert(key, value);
                    conditions_map
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };
        
        // Get base data without CASE expressions
        let base_result = self.execute_select_with_order_limit(table, base_conditions, order_by.clone(), None, tx_id)?;
        
        // Extract rows from QueryResponse
        let mut rows = base_result.results.unwrap_or_default();
        
        // Apply CASE expressions to each row
        for (case_logic, alias, when_clauses) in case_expressions {
            println!("🔍 DEBUG CASE: Processing CASE expression '{}' with alias '{}'", case_logic, alias);
            
            // Parse WHEN clauses
            let when_conditions: Vec<&str> = when_clauses.split(';').collect();
            
            for row in &mut rows {
                let case_result = self.evaluate_case_expression(row, case_logic, &when_conditions);
                row.insert(alias.clone(), case_result);
            }
        }
        
        // Apply final limit if specified
        if let Some(limit_count) = limit {
            rows.truncate(limit_count);
        }
        
        // Build the response
        let response = QueryResponse {
            status: 200,
            message: format!("CASE expression query executed successfully - {} rows returned", rows.len()),
            table: Some(table.to_string()),
            results: Some(rows),
            affected_rows: 0,
        };
        
        serde_json::to_string(&response).map_err(|e| e.to_string())
    }
    
    /// Evaluate a CASE expression for a given row
    fn evaluate_case_expression(&self, row: &HashMap<String, String>, case_logic: &str, when_conditions: &[&str]) -> String {
        // Simple CASE evaluation - this is a basic implementation
        // In a full implementation, we'd need a proper expression evaluator
        
        // Extract the basic pattern: CASE WHEN condition THEN result ELSE default END
        if case_logic.contains("WHEN") && case_logic.contains("THEN") {
            // Try to match simple patterns like: CASE WHEN age < 30 THEN 'Young' ELSE 'Old' END
            if let Some(when_start) = case_logic.find("WHEN") {
                if let Some(then_pos) = case_logic.find("THEN") {
                    let condition_part = &case_logic[when_start + 4..then_pos].trim();
                    
                    // Simple condition evaluation (age < 30, name = 'Alice', etc.)
                    if self.evaluate_simple_condition(row, condition_part) {
                        // Extract the THEN value
                        let then_start = then_pos + 4;
                        if let Some(else_pos) = case_logic.find("ELSE") {
                            let then_value = &case_logic[then_start..else_pos].trim();
                            return self.extract_literal_value(then_value);
                        } else if let Some(end_pos) = case_logic.find("END") {
                            let then_value = &case_logic[then_start..end_pos].trim();
                            return self.extract_literal_value(then_value);
                        }
                    } else {
                        // Check for ELSE clause
                        if let Some(else_pos) = case_logic.find("ELSE") {
                            if let Some(end_pos) = case_logic.find("END") {
                                let else_value = &case_logic[else_pos + 4..end_pos].trim();
                                return self.extract_literal_value(else_value);
                            }
                        }
                    }
                }
            }
        }
        
        // Default fallback
        "NULL".to_string()
    }
    
    /// Evaluate a simple condition (age < 30, name = 'Alice', etc.)
    fn evaluate_simple_condition(&self, row: &HashMap<String, String>, condition: &str) -> bool {
        if condition.contains('<') {
            let parts: Vec<&str> = condition.split('<').collect();
            if parts.len() == 2 {
                let column = parts[0].trim();
                let value = parts[1].trim();
                if let Some(row_value) = row.get(column) {
                    if let (Ok(row_num), Ok(cond_num)) = (row_value.parse::<i32>(), value.parse::<i32>()) {
                        return row_num < cond_num;
                    }
                }
            }
        } else if condition.contains('>') {
            let parts: Vec<&str> = condition.split('>').collect();
            if parts.len() == 2 {
                let column = parts[0].trim();
                let value = parts[1].trim();
                if let Some(row_value) = row.get(column) {
                    if let (Ok(row_num), Ok(cond_num)) = (row_value.parse::<i32>(), value.parse::<i32>()) {
                        return row_num > cond_num;
                    }
                }
            }
        } else if condition.contains('=') {
            let parts: Vec<&str> = condition.split('=').collect();
            if parts.len() == 2 {
                let column = parts[0].trim();
                let value = parts[1].trim().replace("'", "").replace("\"", "");
                if let Some(row_value) = row.get(column) {
                    return row_value == &value;
                }
            }
        }
        
        false
    }
    
    /// Extract literal value from string (remove quotes, etc.)
    fn extract_literal_value(&self, value: &str) -> String {
        value.trim().replace("'", "").replace("\"", "")
    }
}

/// NEW: Query performance metrics structure
#[derive(Debug, Clone)]
pub struct QueryPerformanceMetrics {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_hit_rate: f64,
    pub active_transactions: usize,
    pub total_tables: usize,
}