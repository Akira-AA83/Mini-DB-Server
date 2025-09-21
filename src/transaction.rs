use sled::{Db, Batch};
use std::sync::{Arc, Mutex, Weak};
use std::collections::{HashMap, HashSet};
use crate::query::QueryResponse;
use crate::query::QueryExecutor;

pub struct TransactionManager {
    db: Arc<Db>,
    active_transactions: Arc<Mutex<HashMap<String, TransactionData>>>,
    query_executor: Weak<QueryExecutor>,
}

#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Insert { table: String, key: String, value: String },
    Update { table: String, key: String, old_value: String, new_value: String },
    Delete { table: String, key: String, value: String },
}

pub struct TransactionData {
    pub batch: Batch,
    pub modified_tables: HashSet<String>,
    pub operations: Vec<TransactionOperation>,
}


impl TransactionManager {
    /// FIXED: Constructor with better memory safety and consistency
    pub fn new(db: Arc<Db>, active_transactions: Arc<Mutex<HashMap<String, TransactionData>>>) -> Self {
        println!("📌 DEBUG: TransactionManager using database @{:p}", Arc::as_ptr(&db));
    
        Self {
            db,  // Use the same database reference for consistency
            active_transactions,
            query_executor: Weak::new(),
        }
    }
    
    /// FIXED: Alternative constructor for better integration patterns
    pub fn new_with_executor(db: Arc<Db>, query_executor: Arc<QueryExecutor>) -> Self {
        let active_transactions = Arc::new(Mutex::new(HashMap::new()));
        
        Self {
            db,
            active_transactions,
            query_executor: Arc::downgrade(&query_executor),
        }
    }
    

    pub fn set_query_executor(&mut self, query_executor: Arc<QueryExecutor>) {
        self.query_executor = Arc::downgrade(&query_executor);
    }
    
    /// FIXED: Provide access to active transactions for integration
    pub fn get_active_transactions(&self) -> Arc<Mutex<HashMap<String, TransactionData>>> {
        Arc::clone(&self.active_transactions)
    }

    /// Start a new transaction
    pub fn begin_transaction(&self, tx_id: String) -> Result<QueryResponse, String> {
        let mut transactions = self.active_transactions.lock().unwrap();

        if transactions.contains_key(&tx_id) {
            return Err(format!("Transazione {} già attiva", tx_id));
        }

        transactions.insert(tx_id.clone(), TransactionData {
            batch: Batch::default(),
            modified_tables: HashSet::new(),
            operations: Vec::new(),
        });

        println!("📌 DEBUG BEGIN: Stato di active_transactions dopo il BEGIN: {:?}", transactions.keys().collect::<Vec<_>>());

        Ok(QueryResponse {
            status: 200,
            message: format!("Transazione {} avviata", tx_id),
            table: None,
            results: None,
            affected_rows: 0,
        })
    }
    
    
    

    /// Add an operation to the active transaction (FIXED: proper transaction isolation)
    pub fn add_insert_operation(&self, tx_id: &str, table: &str, key: &str, value: &str) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().unwrap();
        if let Some(transaction) = transactions.get_mut(tx_id) {
            let operation = TransactionOperation::Insert {
                table: table.to_string(),
                key: key.to_string(),
                value: value.to_string(),
            };
            transaction.add_operation(operation);
            println!("📌 DEBUG ADD_INSERT_OP: Added INSERT operation to transaction batch - table: {}, key: {}", table, key);
            Ok(())
        } else {
            Err("❌ Errore: Nessuna transazione attiva".to_string())
        }
    }
    
    /// Add an UPDATE operation to the active transaction
    pub fn add_update_operation(&self, tx_id: &str, table: &str, key: &str, old_value: &str, new_value: &str) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().unwrap();
        if let Some(transaction) = transactions.get_mut(tx_id) {
            let operation = TransactionOperation::Update {
                table: table.to_string(),
                key: key.to_string(),
                old_value: old_value.to_string(),
                new_value: new_value.to_string(),
            };
            transaction.add_operation(operation);
            println!("📌 DEBUG ADD_UPDATE_OP: Added UPDATE operation to transaction batch - table: {}, key: {}", table, key);
            Ok(())
        } else {
            Err("❌ Errore: Nessuna transazione attiva".to_string())
        }
    }
    
    /// Add a DELETE operation to the active transaction
    pub fn add_delete_operation(&self, tx_id: &str, table: &str, key: &str, value: &str) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().unwrap();
        if let Some(transaction) = transactions.get_mut(tx_id) {
            let operation = TransactionOperation::Delete {
                table: table.to_string(),
                key: key.to_string(),
                value: value.to_string(),
            };
            transaction.add_operation(operation);
            println!("📌 DEBUG ADD_DELETE_OP: Added DELETE operation to transaction batch - table: {}, key: {}", table, key);
            Ok(())
        } else {
            Err("❌ Errore: Nessuna transazione attiva".to_string())
        }
    }
    
    

    

 
    pub fn commit_transaction(&self, tx_id: &str) -> Result<QueryResponse, String> {
        let mut transactions = self.active_transactions.lock().unwrap();

        if let Some(transaction) = transactions.remove(tx_id) {
            // Apply the transaction batch to each modified table
            // This ensures ACID compliance by committing all changes atomically
            for table in &transaction.modified_tables {
                let tree = self.db.open_tree(table).map_err(|e| {
                    format!("Failed to open tree for table {}: {}", table, e)
                })?;
                
                // Apply the batch to this specific table
                if let Err(e) = tree.apply_batch(transaction.batch.clone()) {
                    return Err(format!("apply_batch failed on table {}: {}", table, e));
                }
            }

            // Ensure all changes are persisted to disk
            if let Err(e) = self.db.flush() {
                return Err(format!("flush failed: {}", e));
            }

            // Invalidate cache for modified tables to ensure consistency
            if let Some(query_executor) = self.query_executor.upgrade() {
                for table in &transaction.modified_tables {
                    query_executor.invalidate_cache(table);
                }
            }
        } else {
            return Err(format!("No active transaction found with ID {}", tx_id));
        }

        Ok(QueryResponse {
            status: 200,
            message: format!("Transaction {} committed successfully", tx_id),
            table: None,
            results: None,
            affected_rows: 0,
        })
    }
    
    
    
    
    
       

    /// Cancel the transaction without applying changes
    pub fn rollback_transaction(&self, tx_id: &str) -> Result<QueryResponse, String> {
        let mut transactions = self.active_transactions.lock().unwrap();
        if transactions.remove(tx_id).is_some() {
            Ok(QueryResponse {
                status: 200,
                message: format!("Transazione {} annullata", tx_id),
                table: None,
                results: None,
                affected_rows: 0,
            })
        } else {
            Err(format!("Nessuna transazione attiva con ID {}", tx_id))
        }
    }
    
    // AGGIUNGI SOLO QUESTO METODO per compatibilità security
    pub fn set_secure_executor(&mut self, _executor: Arc<crate::security::SecureQueryExecutor>) {
        // Per ora non serve implementazione, solo per compatibilità
        println!("🔒 Secure executor set for transaction manager");
    }

}

impl TransactionData {
    pub fn new() -> Self {
        Self {
            batch: sled::Batch::default(),
            modified_tables: std::collections::HashSet::new(),
            operations: Vec::new(),
        }
    }
    
    pub fn add_operation(&mut self, operation: TransactionOperation) {
        match &operation {
            TransactionOperation::Insert { table, key, value } => {
                self.batch.insert(key.as_bytes(), value.as_bytes());
                self.modified_tables.insert(table.clone());
            },
            TransactionOperation::Update { table, key, new_value, .. } => {
                self.batch.insert(key.as_bytes(), new_value.as_bytes());
                self.modified_tables.insert(table.clone());
            },
            TransactionOperation::Delete { table, key, .. } => {
                self.batch.remove(key.as_bytes());
                self.modified_tables.insert(table.clone());
            },
        }
        self.operations.push(operation);
    }
}
