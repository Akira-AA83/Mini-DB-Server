use mini_db_server::storage::Storage;
use mini_db_server::schema::{TableSchema, DataType, Constraint};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_create_table_with_schema() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create a schema per la tabella users
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull])
        .add_column("email", DataType::Text, vec![Constraint::Unique])
        .add_column("age", DataType::Integer, vec![]);

    // Create the table
    storage.create_table(users_schema).expect("Error creating table");

    // Verify that la tabella sia stata creata
    let tables = storage.list_tables();
    assert!(tables.contains(&"users".to_string()));
    
    // Verify that lo schema sia salvato
    let schema = storage.get_schema("users").expect("Schema not found");
    assert_eq!(schema.name, "users");
    assert_eq!(schema.columns.len(), 4);
}

#[test]
fn test_insert_with_schema_validation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);

    storage.create_table(users_schema).expect("Error creating table");

    // Test inserimento valido
    let mut valid_data = HashMap::new();
    valid_data.insert("id".to_string(), "1".to_string());
    valid_data.insert("name".to_string(), "Alice".to_string());

    let result = storage.insert("users", "user1", valid_data);
    assert!(result.is_ok());

    // Test inserimento non valido (manca campo NOT NULL)
    let mut invalid_data = HashMap::new();
    invalid_data.insert("id".to_string(), "2".to_string());
    // Manca il campo "name" che è NOT NULL

    let result = storage.insert("users", "user2", invalid_data);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non può essere NULL"));
}

#[test]
fn test_data_type_validation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema con diversi tipi di dati
    let test_schema = TableSchema::new("test_table")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey])
        .add_column("score", DataType::Real, vec![])
        .add_column("active", DataType::Boolean, vec![])
        .add_column("description", DataType::Text, vec![]);

    storage.create_table(test_schema).expect("Error creating table");

    // Test con dati validi
    let mut valid_data = HashMap::new();
    valid_data.insert("id".to_string(), "1".to_string());
    valid_data.insert("score".to_string(), "95.5".to_string());
    valid_data.insert("active".to_string(), "true".to_string());
    valid_data.insert("description".to_string(), "Test description".to_string());

    let result = storage.insert("test_table", "test1", valid_data);
    assert!(result.is_ok());

    // Test con tipo intero non valido
    let mut invalid_data = HashMap::new();
    invalid_data.insert("id".to_string(), "not_a_number".to_string());
    invalid_data.insert("description".to_string(), "Test".to_string());

    let result = storage.insert("test_table", "test2", invalid_data);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non è un intero valido"));
}

#[test]
fn test_drop_table() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Crea una tabella
    let test_schema = TableSchema::new("temp_table")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey]);

    storage.create_table(test_schema).expect("Error creating table");

    // Verify that esista
    assert!(storage.list_tables().contains(&"temp_table".to_string()));

    // Elimina la tabella
    storage.drop_table("temp_table").expect("Errore nell'eliminazione della tabella");

    // Verify that non esista più
    assert!(!storage.list_tables().contains(&"temp_table".to_string()));
    assert!(storage.get_schema("temp_table").is_none());
}