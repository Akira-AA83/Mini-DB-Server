use mini_db_server::storage::Storage;
use mini_db_server::schema::{TableSchema, DataType, Constraint};
use tempfile::TempDir;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[test]
fn test_storage_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema prima di inserire dati
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Error creating schema");

    let table = "users";

    // Insert test
    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("name".to_string(), "Alice".to_string());
    storage.insert(table, "1", values.clone()).expect("Insert failed");

    // Select test
    let fetched = storage.select(table, "1").expect("Select failed");
    assert_eq!(fetched.get("name"), Some(&"Alice".to_string()));

    // Update test
    let mut new_values = HashMap::new();
    new_values.insert("name".to_string(), "Alicia".to_string());
    storage.update(table, "1", new_values).expect("Update failed");

    let updated = storage.select(table, "1").expect("Select failed");
    assert_eq!(updated.get("name"), Some(&"Alicia".to_string()));

    // Delete test
    storage.delete(table, "1").expect("Delete failed");
    let deleted = storage.select(table, "1");
    assert!(deleted.is_none());
}

#[test]
fn test_storage_basic_crud() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Error creating schema");

    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("name".to_string(), "Alice".to_string());
    storage.insert("users", "1", values.clone()).expect("Insert failed");

    let fetched = storage.select("users", "1").expect("Select failed");
    assert_eq!(fetched.get("name"), Some(&"Alice".to_string()));

    let mut new_values = HashMap::new();
    new_values.insert("name".to_string(), "Bob".to_string());
    storage.update("users", "1", new_values).expect("Update failed");

    let updated = storage.select("users", "1").expect("Select failed");
    assert_eq!(updated.get("name"), Some(&"Bob".to_string()));

    storage.delete("users", "1").expect("Delete failed");
    assert!(storage.select("users", "1").is_none());
}

#[test]
fn test_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Error creating schema");

    storage.batch_operations("users", vec![
        ("2".to_string(), Some(HashMap::from([
            ("id".to_string(), "2".to_string()),
            ("name".to_string(), "Charlie".to_string())
        ]))),
        ("3".to_string(), Some(HashMap::from([
            ("id".to_string(), "3".to_string()),
            ("name".to_string(), "Dana".to_string())
        ]))),
    ]).expect("Transaction failed");

    let fetched_2 = storage.select("users", "2").expect("Select failed");
    let fetched_3 = storage.select("users", "3").expect("Select failed");

    assert_eq!(fetched_2.get("name"), Some(&"Charlie".to_string()));
    assert_eq!(fetched_3.get("name"), Some(&"Dana".to_string()));
}

#[test]
fn test_index_select() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Error creating schema");

    let table = "users";

    // Insert test
    let mut values = HashMap::new();
    values.insert("id".to_string(), "42".to_string());
    values.insert("name".to_string(), "Neo".to_string());
    storage.insert(table, "abc123", values.clone()).expect("Insert failed");

    // **Test search by ID**
    let fetched = storage.select_by_id(table, "42").expect("Get failed");
    assert_eq!(fetched.get("name"), Some(&"Neo".to_string()));
}

#[test]
fn test_index_performance() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create schema
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Error creating schema");

    let table = "users";

    // Popoliamo il DB con 1000 record (ridotto per velocità test)
    for i in 1..=1000 {
        let mut values = HashMap::new();
        values.insert("id".to_string(), i.to_string());
        values.insert("name".to_string(), format!("User{}", i));
        storage.insert(table, &i.to_string(), values).expect("Insert failed");
    }

    let search_id = "500";

    // **Test senza indice: iterando tutti i dati**
    let start_no_index = Instant::now();
    let _result_no_index = storage.search_by_prefix(table, "5").expect("Errore nella ricerca");
    let duration_no_index = start_no_index.elapsed();

    // **Test con indice: selezionando direttamente per ID**
    let start_with_index = Instant::now();
    let _result_with_index = storage.select_by_id(table, search_id);
    let duration_with_index = start_with_index.elapsed();

    println!(
        "Tempo di esecuzione:\nSenza indice: {:?}\nCon indice: {:?}",
        duration_no_index, duration_with_index
    );

    assert!(
        duration_with_index < duration_no_index,
        "L'indice non sta migliorando la velocità di SELECT!"
    );
}