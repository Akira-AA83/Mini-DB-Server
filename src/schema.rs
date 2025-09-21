/*
📌 Enhanced Schema Management System
✅ Foreign Keys con referential integrity
✅ Constraint validation avanzata
✅ Schema evolution (ALTER TABLE)
✅ Index management
✅ Trigger system foundation
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use sled::Db;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub foreign_keys: Vec<ForeignKey>,
    pub triggers: Vec<Trigger>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
    pub default_value: Option<String>,
    pub is_nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Integer,
    BigInteger,
    Text,
    VarChar(usize),
    Real,
    Double,
    Boolean,
    Timestamp,
    Date,
    UUID,
    JSON,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    Check(String),
    Default(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
    FullText,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ForeignKeyAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub name: String,
    pub table: String,
    pub event: TriggerEvent,
    pub timing: TriggerTiming,
    pub condition: Option<String>,
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

pub struct SchemaManager {
    db: Arc<Db>,
    schemas: HashMap<String, TableSchema>,
    foreign_keys: HashMap<String, Vec<ForeignKey>>, // table -> FK list
}

impl SchemaManager {
    pub fn new(db: Arc<Db>) -> Self {
        let mut manager = Self {
            db,
            schemas: HashMap::new(),
            foreign_keys: HashMap::new(),
        };
        
        manager.load_schemas();
        manager.load_foreign_keys();
        manager
    }

    /// Carica tutti gli schemi dal database
    fn load_schemas(&mut self) {
        if let Ok(schema_tree) = self.db.open_tree("__schemas__") {
            for item in schema_tree.iter() {
                if let Ok((key, value)) = item {
                    let table_name = String::from_utf8_lossy(&key).to_string();
                    if let Ok(schema) = serde_json::from_slice::<TableSchema>(&value) {
                        self.schemas.insert(table_name, schema);
                    }
                }
            }
        }
    }

    /// Carica tutte le foreign keys
    fn load_foreign_keys(&mut self) {
        if let Ok(fk_tree) = self.db.open_tree("__foreign_keys__") {
            for item in fk_tree.iter() {
                if let Ok((key, value)) = item {
                    let table_name = String::from_utf8_lossy(&key).to_string();
                    if let Ok(fks) = serde_json::from_slice::<Vec<ForeignKey>>(&value) {
                        self.foreign_keys.insert(table_name, fks);
                    }
                }
            }
        }
    }

    /// Crea una nuova tabella con schema avanzato
    pub fn create_table(&mut self, mut schema: TableSchema) -> Result<(), String> {
        // Validate schema
        self.validate_schema(&schema)?;
        
        // Set metadata
        schema.created_at = chrono::Utc::now();
        schema.version = 1;
        
        // Save schema
        let schema_tree = self.db.open_tree("__schemas__").map_err(|e| e.to_string())?;
        let serialized = serde_json::to_vec(&schema).map_err(|e| e.to_string())?;
        schema_tree.insert(schema.name.as_bytes(), serialized).map_err(|e| e.to_string())?;
        
        // Save foreign keys separately for quick access
        if !schema.foreign_keys.is_empty() {
            self.save_foreign_keys(&schema.name, &schema.foreign_keys)?;
        }
        
        // Create indexes
        for index in &schema.indexes {
            self.create_index(index.clone())?;
        }
        
        // Create the actual table tree in the database
        let table_tree = self.db.open_tree(&schema.name).map_err(|e| e.to_string())?;
        // Insert a dummy entry and remove it to ensure the tree is created and visible
        table_tree.insert(b"__init__", b"").map_err(|e| e.to_string())?;
        table_tree.remove(b"__init__").map_err(|e| e.to_string())?;
        table_tree.flush().map_err(|e| e.to_string())?;
        
        // Store in memory
        self.schemas.insert(schema.name.clone(), schema.clone());
        
        println!("✅ Enhanced schema created for table: {}", schema.name);
        Ok(())
    }

    /// Salva foreign keys per una tabella
    fn save_foreign_keys(&mut self, table: &str, fks: &[ForeignKey]) -> Result<(), String> {
        let fk_tree = self.db.open_tree("__foreign_keys__").map_err(|e| e.to_string())?;
        let serialized = serde_json::to_vec(fks).map_err(|e| e.to_string())?;
        fk_tree.insert(table.as_bytes(), serialized).map_err(|e| e.to_string())?;
        
        self.foreign_keys.insert(table.to_string(), fks.to_vec());
        Ok(())
    }

    /// Crea un indice
    fn create_index(&self, index: Index) -> Result<(), String> {
        let index_tree = self.db.open_tree(&format!("__index__{}", index.name)).map_err(|e| e.to_string())?;
        let index_data = serde_json::to_vec(&index).map_err(|e| e.to_string())?;
        index_tree.insert("metadata", index_data).map_err(|e| e.to_string())?;
        
        println!("✅ Index created: {} on table {}", index.name, index.table);
        Ok(())
    }

    /// Validazione schema avanzata
    fn validate_schema(&self, schema: &TableSchema) -> Result<(), String> {
        if schema.name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }

        if schema.columns.is_empty() {
            return Err("Table must have at least one column".to_string());
        }

        // Check for duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in &schema.columns {
            if !column_names.insert(column.name.clone()) {
                return Err(format!("Duplicate column name: {}", column.name));
            }
        }

        // Validate primary key
        let pk_count = schema.columns.iter()
            .filter(|col| col.constraints.iter().any(|c| matches!(c, Constraint::PrimaryKey)))
            .count();
        
        if pk_count == 0 {
            return Err("Table must have at least one primary key".to_string());
        }

        // Validate foreign keys
        for fk in &schema.foreign_keys {
            self.validate_foreign_key(fk)?;
        }

        Ok(())
    }

    /// Validazione foreign key
    fn validate_foreign_key(&self, fk: &ForeignKey) -> Result<(), String> {
        // Check referenced table exists
        if !self.schemas.contains_key(&fk.referenced_table) {
            return Err(format!("Referenced table '{}' does not exist", fk.referenced_table));
        }

        let referenced_schema = &self.schemas[&fk.referenced_table];

        // Check column count matches
        if fk.columns.len() != fk.referenced_columns.len() {
            return Err("Foreign key column count must match referenced columns".to_string());
        }

        // Check referenced columns exist and have compatible types
        for (_local_col, ref_col) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
            if !referenced_schema.columns.iter().any(|c| c.name == *ref_col) {
                return Err(format!("Referenced column '{}' does not exist in table '{}'", ref_col, fk.referenced_table));
            }
        }

        Ok(())
    }

    /// Validazione dati con foreign key constraints
    pub fn validate_row(&self, table: &str, row: &HashMap<String, String>) -> Result<(), String> {
        let schema = self.get_schema(table)
            .ok_or_else(|| format!("Schema not found for table: {}", table))?;

        // Basic column validation
        for column in &schema.columns {
            let value = row.get(&column.name);

            // Check NOT NULL (skip for auto-increment PRIMARY KEY columns)
            if !column.is_nullable && (value.is_none() || value.unwrap().is_empty()) {
                // Skip validation for PRIMARY KEY columns that can be auto-generated
                if column.constraints.contains(&crate::schema::Constraint::PrimaryKey) && column.name == "id" {
                    println!("🔍 DEBUG SCHEMA: Skipping NULL check for auto-increment PRIMARY KEY: {}", column.name);
                    continue;
                }
                println!("🔍 DEBUG SCHEMA: NULL validation failed for column: {}", column.name);
                // TEMPORARILY: Show what error this generates
                let error_msg = format!("Column {} cannot be NULL", column.name);
                if column.name == "id" {
                    println!("🔍 DEBUG SCHEMA: This is the ID error! Error: {}", error_msg);
                    println!("🔍 DEBUG SCHEMA: Column details: {:?}", column);
                }
                return Err(error_msg);
            }

            // Type validation
            if let Some(val) = value {
                self.validate_data_type(&column.data_type, val)?;
            }
        }

        // Foreign key validation
        self.validate_foreign_key_constraints(table, row)?;

        Ok(())
    }

    /// Validazione constraint di foreign key
    fn validate_foreign_key_constraints(&self, table: &str, row: &HashMap<String, String>) -> Result<(), String> {
        if let Some(fks) = self.foreign_keys.get(table) {
            for fk in fks {
                self.validate_foreign_key_reference(fk, row)?;
            }
        }
        Ok(())
    }

    /// Valida che un foreign key reference esista
    fn validate_foreign_key_reference(&self, fk: &ForeignKey, row: &HashMap<String, String>) -> Result<(), String> {
        // Build referenced values
        let mut ref_values = HashMap::new();
        for (local_col, ref_col) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
            if let Some(value) = row.get(local_col) {
                ref_values.insert(ref_col.clone(), value.clone());
            }
        }

        // Skip validation if any FK column is NULL (allowed)
        if ref_values.values().any(|v| v.is_empty()) {
            return Ok(());
        }

        // Check if referenced record exists
        if !self.record_exists(&fk.referenced_table, &ref_values)? {
            return Err(format!(
                "Foreign key constraint violated: referenced record not found in table '{}'", 
                fk.referenced_table
            ));
        }

        Ok(())
    }

    /// Verifica se un record esiste
    fn record_exists(&self, table: &str, conditions: &HashMap<String, String>) -> Result<bool, String> {
        let tree = self.db.open_tree(table).map_err(|e| e.to_string())?;
        
        for entry in tree.iter() {
            let (_, value) = entry.map_err(|e| e.to_string())?;
            let value_str = String::from_utf8_lossy(&value);
            
            if let Ok(record) = serde_json::from_str::<HashMap<String, String>>(&value_str) {
                if conditions.iter().all(|(k, v)| record.get(k) == Some(v)) {
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }

    /// Validazione tipo di dato avanzata
    fn validate_data_type(&self, data_type: &DataType, value: &str) -> Result<(), String> {
        match data_type {
            DataType::Integer | DataType::BigInteger => {
                value.parse::<i64>().map_err(|_| format!("'{}' is not a valid integer", value))?;
            }
            DataType::Real | DataType::Double => {
                value.parse::<f64>().map_err(|_| format!("'{}' is not a valid number", value))?;
            }
            DataType::Boolean => {
                let lower = value.to_lowercase();
                if !["true", "false", "1", "0", "yes", "no"].contains(&lower.as_str()) {
                    return Err(format!("'{}' is not a valid boolean", value));
                }
            }
            DataType::VarChar(max_len) => {
                if value.len() > *max_len {
                    return Err(format!("Text too long: {} > {} characters", value.len(), max_len));
                }
            }
            DataType::UUID => {
                uuid::Uuid::parse_str(value).map_err(|_| format!("'{}' is not a valid UUID", value))?;
            }
            DataType::JSON => {
                serde_json::from_str::<serde_json::Value>(value)
                    .map_err(|_| format!("'{}' is not valid JSON", value))?;
            }
            DataType::Text | DataType::Timestamp | DataType::Date | DataType::Binary => {
                // These are always valid as strings for now
            }
        }
        Ok(())
    }

    /// ALTER TABLE support
    pub fn alter_table(&mut self, table: &str, alteration: TableAlteration) -> Result<(), String> {
        let mut schema = self.get_schema(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?
            .clone();

        match alteration {
            TableAlteration::AddColumn(column) => {
                schema.columns.push(column);
            }
            TableAlteration::DropColumn(column_name) => {
                schema.columns.retain(|c| c.name != column_name);
            }
            TableAlteration::AddForeignKey(fk) => {
                self.validate_foreign_key(&fk)?;
                schema.foreign_keys.push(fk);
            }
            TableAlteration::DropForeignKey(fk_name) => {
                schema.foreign_keys.retain(|fk| fk.name != fk_name);
            }
        }

        schema.version += 1;
        self.schemas.insert(table.to_string(), schema.clone());
        
        // Save updated schema
        let schema_tree = self.db.open_tree("__schemas__").map_err(|e| e.to_string())?;
        let serialized = serde_json::to_vec(&schema).map_err(|e| e.to_string())?;
        schema_tree.insert(table.as_bytes(), serialized).map_err(|e| e.to_string())?;

        println!("✅ Table '{}' altered successfully (version {})", table, schema.version);
        Ok(())
    }

    /// Cascade delete support
    pub fn cascade_delete(&self, table: &str, deleted_row: &HashMap<String, String>) -> Result<Vec<CascadeAction>, String> {
        let mut actions = Vec::new();
        
        // Find all tables that reference this table
        for (ref_table, fks) in &self.foreign_keys {
            for fk in fks {
                if fk.referenced_table == table {
                    match fk.on_delete {
                        ForeignKeyAction::Cascade => {
                            // Find records to cascade delete
                            let cascade_conditions = self.build_cascade_conditions(fk, deleted_row);
                            actions.push(CascadeAction::Delete {
                                table: ref_table.clone(),
                                conditions: cascade_conditions,
                            });
                        }
                        ForeignKeyAction::SetNull => {
                            let cascade_conditions = self.build_cascade_conditions(fk, deleted_row);
                            actions.push(CascadeAction::SetNull {
                                table: ref_table.clone(),
                                columns: fk.columns.clone(),
                                conditions: cascade_conditions,
                            });
                        }
                        ForeignKeyAction::Restrict => {
                            // Check if any referencing records exist
                            let cascade_conditions = self.build_cascade_conditions(fk, deleted_row);
                            if self.records_exist(ref_table, &cascade_conditions)? {
                                return Err(format!(
                                    "Cannot delete: foreign key constraint violation in table '{}'", 
                                    ref_table
                                ));
                            }
                        }
                        _ => {
                            // NoAction, SetDefault - implement as needed
                        }
                    }
                }
            }
        }
        
        Ok(actions)
    }

    fn build_cascade_conditions(&self, fk: &ForeignKey, deleted_row: &HashMap<String, String>) -> HashMap<String, String> {
        let mut conditions = HashMap::new();
        for (local_col, ref_col) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
            if let Some(value) = deleted_row.get(ref_col) {
                conditions.insert(local_col.clone(), value.clone());
            }
        }
        conditions
    }

    fn records_exist(&self, table: &str, conditions: &HashMap<String, String>) -> Result<bool, String> {
        self.record_exists(table, conditions)
    }

    // Getters
    pub fn get_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    pub fn get_foreign_keys(&self, table: &str) -> Option<&Vec<ForeignKey>> {
        self.foreign_keys.get(table)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        // Check for referencing foreign keys first
        for (other_table, fks) in &self.foreign_keys {
            for fk in fks {
                if fk.referenced_table == table_name {
                    return Err(format!(
                        "Cannot drop table '{}': referenced by foreign key '{}' in table '{}'",
                        table_name, fk.name, other_table
                    ));
                }
            }
        }

        // Remove schema
        if let Ok(schema_tree) = self.db.open_tree("__schemas__") {
            schema_tree.remove(table_name.as_bytes()).map_err(|e| e.to_string())?;
        }
        
        // Remove foreign keys
        if let Ok(fk_tree) = self.db.open_tree("__foreign_keys__") {
            fk_tree.remove(table_name.as_bytes()).map_err(|e| e.to_string())?;
        }
        
        self.schemas.remove(table_name);
        self.foreign_keys.remove(table_name);
        
        println!("✅ Table '{}' dropped successfully", table_name);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum TableAlteration {
    AddColumn(Column),
    DropColumn(String),
    AddForeignKey(ForeignKey),
    DropForeignKey(String),
}

#[derive(Debug, Clone)]
pub enum CascadeAction {
    Delete {
        table: String,
        conditions: HashMap<String, String>,
    },
    SetNull {
        table: String,
        columns: Vec<String>,
        conditions: HashMap<String, String>,
    },
}

// Helper implementations for TableSchema
impl TableSchema {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            triggers: Vec::new(),
            created_at: chrono::Utc::now(),
            version: 1,
        }
    }

    pub fn add_column(mut self, name: &str, data_type: DataType, constraints: Vec<Constraint>) -> Self {
        let is_nullable = !constraints.iter().any(|c| matches!(c, Constraint::NotNull | Constraint::PrimaryKey));
        
        self.columns.push(Column {
            name: name.to_string(),
            data_type,
            constraints,
            default_value: None,
            is_nullable,
        });
        self
    }

    pub fn add_foreign_key(mut self, name: &str, columns: Vec<String>, referenced_table: &str, referenced_columns: Vec<String>) -> Self {
        self.foreign_keys.push(ForeignKey {
            name: name.to_string(),
            table: self.name.clone(),
            columns,
            referenced_table: referenced_table.to_string(),
            referenced_columns,
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        });
        self
    }

    pub fn add_index(mut self, name: &str, columns: Vec<String>, unique: bool) -> Self {
        self.indexes.push(Index {
            name: name.to_string(),
            table: self.name.clone(),
            columns,
            unique,
            index_type: IndexType::BTree,
        });
        self
    }
}