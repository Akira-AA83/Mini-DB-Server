/*
Enhanced Storage System with Foreign Key Support
✅ Foreign key constraint validation
✅ Cascade operations
✅ Enhanced schema integration
✅ Index support
*/

use sled::{Db, Batch};
use std::collections::HashMap;
use std::sync::Arc;
use serde_json;
use crate::schema::{SchemaManager, TableSchema, CascadeAction};

pub struct Storage {
    db: Arc<Db>,
    index: Arc<Db>,
    schema_manager: SchemaManager,
}

impl Storage {
    pub fn new(db: Arc<Db>) -> Self {
        // Generate unique index path
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let index_path = format!("index_db_{}", id);
        let index = Arc::new(sled::open(&index_path).expect("Failed to open index"));
        let schema_manager = SchemaManager::new(Arc::clone(&db));
        
        Self { 
            db, 
            index, 
            schema_manager 
        }
    }

    /// Creates a new table with enhanced schema
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), String> {
        self.schema_manager.create_table(schema)
    }

    /// Gets table schema
    pub fn get_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.schema_manager.get_schema(table_name)
    }

    /// Lists all tables
    pub fn list_tables(&self) -> Vec<String> {
        self.schema_manager.list_tables()
    }

    /// Drops a table with foreign key checking
    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        self.schema_manager.drop_table(table_name)?;
        
        // Clean up data and indexes
        let tree = self.db.open_tree(table_name).map_err(|e| e.to_string())?;
        tree.clear().map_err(|e| e.to_string())?;
        
        if let Ok(index_tree) = self.index.open_tree(table_name) {
            index_tree.clear().map_err(|e| e.to_string())?;
        }
        
        Ok(())
    }

    /// Enhanced insert with foreign key validation
    pub fn insert(&self, table: &str, key: &str, values: HashMap<String, String>) -> Result<(), String> {
        // Validate with enhanced schema manager (includes FK validation)
        self.schema_manager.validate_row(table, &values)?;
        
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        let value = serde_json::to_string(&values).map_err(|e| e.to_string())?;

        // Insert record
        tree.insert(key.as_bytes(), value.as_bytes()).map_err(|e| e.to_string())?;

        // Update indexes
        self.update_indexes(table, key, &values)?;

        Ok(())
    }

    /// Updates indexes for a record
    fn update_indexes(&self, table: &str, key: &str, values: &HashMap<String, String>) -> Result<(), String> {
        // Update ID index if exists
        if let Some(id) = values.get("id") {
            let index_tree = self.index.open_tree(table).map_err(|e| e.to_string())?;
            index_tree.insert(id.as_bytes(), key.as_bytes()).map_err(|e| e.to_string())?;
        }

        // Update other indexes based on schema
        if let Some(schema) = self.schema_manager.get_schema(table) {
            for index in &schema.indexes {
                self.update_custom_index(index, key, values)?;
            }
        }

        Ok(())
    }

    /// Updates a custom index
    fn update_custom_index(&self, index: &crate::schema::Index, key: &str, values: &HashMap<String, String>) -> Result<(), String> {
        let index_tree = self.index.open_tree(&format!("idx_{}_{}", index.table, index.name)).map_err(|e| e.to_string())?;
        
        // Build index key from column values
        let mut index_key_parts = Vec::new();
        for column in &index.columns {
            if let Some(value) = values.get(column) {
                index_key_parts.push(value.clone());
            } else {
                index_key_parts.push("NULL".to_string());
            }
        }
        
        let index_key = index_key_parts.join(":");
        
        if index.unique {
            // Check for uniqueness
            if index_tree.contains_key(&index_key).map_err(|e| e.to_string())? {
                return Err(format!("Unique constraint violation on index '{}'", index.name));
            }
        }
        
        index_tree.insert(index_key.as_bytes(), key.as_bytes()).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Select by ID using index
    pub fn select_by_id(&self, table: &str, id: &str) -> Option<HashMap<String, String>> {
        let index_tree = self.index.open_tree(table).ok()?;
        let key = index_tree.get(id.as_bytes()).ok().flatten()?;
        let key_str = String::from_utf8(key.to_vec()).ok()?;
        self.select(table, &key_str)
    }

    /// Select record by key
    pub fn select(&self, table: &str, key: &str) -> Option<HashMap<String, String>> {
        let tree = self.db.open_tree(table).ok()?;
        let value = tree.get(key.as_bytes()).ok().flatten()?;
        let value_str = String::from_utf8(value.to_vec()).ok()?;
        serde_json::from_str(&value_str).ok()
    }

    /// Enhanced update with foreign key validation
    pub fn update(&self, table: &str, key: &str, new_values: HashMap<String, String>) -> Result<(), String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        
        if let Some(value) = tree.get(key).map_err(|e| e.to_string())? {
            let mut values: HashMap<String, String> = serde_json::from_slice(&value).map_err(|e| e.to_string())?;
            
            // Update values
            for (k, v) in new_values {
                values.insert(k, v);
            }
            
            // Validate updated data (includes FK validation)
            self.schema_manager.validate_row(table, &values)?;
            
            let updated_value = serde_json::to_string(&values).map_err(|e| e.to_string())?;
            tree.insert(key, updated_value.as_bytes()).map_err(|e| e.to_string())?;
            
            // Update indexes
            self.update_indexes(table, key, &values)?;
        }
        Ok(())
    }

    /// Enhanced delete with cascade support
    pub fn delete(&self, table: &str, key: &str) -> Result<(), String> {
        // Get the record before deletion for cascade operations
        let record = self.select(table, key);
        
        if let Some(deleted_record) = record {
            // Check for cascade operations
            let cascade_actions = self.schema_manager.cascade_delete(table, &deleted_record)?;
            
            // Execute cascade actions
            for action in cascade_actions {
                self.execute_cascade_action(action)?;
            }
            
            // Delete the actual record
            let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
            tree.remove(key).map_err(|e| e.to_string())?;
            
            // Remove from indexes
            self.remove_from_indexes(table, key, &deleted_record)?;
        }
        
        Ok(())
    }

    /// Executes a cascade action
    fn execute_cascade_action(&self, action: CascadeAction) -> Result<(), String> {
        match action {
            CascadeAction::Delete { table, conditions } => {
                // Find and delete matching records
                let keys_to_delete = self.find_matching_keys(&table, &conditions)?;
                for key in keys_to_delete {
                    self.delete(&table, &key)?; // Recursive delete for further cascades
                }
            }
            CascadeAction::SetNull { table, columns, conditions } => {
                // Find and update matching records to set FK columns to NULL
                let keys_to_update = self.find_matching_keys(&table, &conditions)?;
                for key in keys_to_update {
                    let mut null_values = HashMap::new();
                    for column in &columns {
                        null_values.insert(column.clone(), "".to_string()); // Empty string represents NULL
                    }
                    self.update(&table, &key, null_values)?;
                }
            }
        }
        Ok(())
    }

    /// Finds keys of records matching conditions
    fn find_matching_keys(&self, table: &str, conditions: &HashMap<String, String>) -> Result<Vec<String>, String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        let mut matching_keys = Vec::new();
        
        for entry in tree.iter() {
            let (key, value) = entry.map_err(|e| e.to_string())?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            let value_str = String::from_utf8_lossy(&value);
            
            if let Ok(record) = serde_json::from_str::<HashMap<String, String>>(&value_str) {
                if conditions.iter().all(|(k, v)| record.get(k) == Some(v)) {
                    matching_keys.push(key_str);
                }
            }
        }
        
        Ok(matching_keys)
    }

    /// Removes record from all indexes
    fn remove_from_indexes(&self, table: &str, key: &str, values: &HashMap<String, String>) -> Result<(), String> {
        // Remove from ID index
        if let Some(id) = values.get("id") {
            if let Ok(index_tree) = self.index.open_tree(table) {
                index_tree.remove(id.as_bytes()).map_err(|e| e.to_string())?;
            }
        }

        // Remove from custom indexes
        if let Some(schema) = self.schema_manager.get_schema(table) {
            for index in &schema.indexes {
                self.remove_from_custom_index(index, key, values)?;
            }
        }

        Ok(())
    }

    /// Removes from a custom index
    fn remove_from_custom_index(&self, index: &crate::schema::Index, _key: &str, values: &HashMap<String, String>) -> Result<(), String> {
        let index_tree = self.index.open_tree(&format!("idx_{}_{}", index.table, index.name)).map_err(|e| e.to_string())?;
        
        // Build index key
        let mut index_key_parts = Vec::new();
        for column in &index.columns {
            if let Some(value) = values.get(column) {
                index_key_parts.push(value.clone());
            } else {
                index_key_parts.push("NULL".to_string());
            }
        }
        
        let index_key = index_key_parts.join(":");
        index_tree.remove(index_key.as_bytes()).map_err(|e| e.to_string())?;
        
        Ok(())
    }

    /// Search by prefix
    pub fn search_by_prefix(&self, table: &str, prefix: &str) -> Result<Vec<HashMap<String, String>>, String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        let mut results = Vec::new();

        for item in tree.scan_prefix(prefix) {
            let (_, value) = item.map_err(|e| e.to_string())?;
            let json_str = String::from_utf8(value.to_vec()).map_err(|e| e.to_string())?;
            let values: HashMap<String, String> = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
            results.push(values);
        }
        Ok(results)
    }

    /// Batch operations with foreign key validation
    pub fn batch_operations(&self, table: &str, operations: Vec<(String, Option<HashMap<String, String>>)>) -> Result<(), String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        let mut batch = Batch::default();
        
        // Validate all operations first
        for (_key, values) in &operations {
            if let Some(val) = values {
                self.schema_manager.validate_row(table, val)?;
            }
        }
        
        // Execute batch if all validations pass
        for (key, values) in operations {
            match values {
                Some(val) => {
                    let value_str = serde_json::to_string(&val).map_err(|e| e.to_string())?;
                    batch.insert(key.as_bytes(), value_str.as_bytes());
                },
                None => {
                    batch.remove(key.as_bytes());
                }
            }
        }
        
        tree.apply_batch(batch).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Get all keys for a table (for testing/debugging)
    pub fn get_table_keys(&self, table: &str) -> Result<Vec<String>, String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        let mut keys = Vec::new();
        
        for entry in tree.iter() {
            let (key, _) = entry.map_err(|e| e.to_string())?;
            let key_str = String::from_utf8(key.to_vec()).map_err(|e| e.to_string())?;
            keys.push(key_str);
        }
        
        Ok(keys)
    }

    /// Query by index
    pub fn query_by_index(&self, table: &str, index_name: &str, value: &str) -> Result<Vec<HashMap<String, String>>, String> {
        let index_tree = self.index.open_tree(&format!("idx_{}_{}", table, index_name)).map_err(|e| e.to_string())?;
        let mut results = Vec::new();
        
        // Find matching index entries
        for entry in index_tree.scan_prefix(value) {
            let (_, record_key) = entry.map_err(|e| e.to_string())?;
            let key_str = String::from_utf8(record_key.to_vec()).map_err(|e| e.to_string())?;
            
            if let Some(record) = self.select(table, &key_str) {
                results.push(record);
            }
        }
        
        Ok(results)
    }

    /// Get foreign keys for a table
    pub fn get_foreign_keys(&self, table: &str) -> Option<&Vec<crate::schema::ForeignKey>> {
        self.schema_manager.get_foreign_keys(table)
    }

    /// Alter table schema
    pub fn alter_table(&mut self, table: &str, alteration: crate::schema::TableAlteration) -> Result<(), String> {
        self.schema_manager.alter_table(table, alteration)
    }
}