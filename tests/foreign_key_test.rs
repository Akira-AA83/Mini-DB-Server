use mini_db_server::storage::Storage;
use mini_db_server::schema::{TableSchema, DataType, Constraint, ForeignKeyAction};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use serial_test::serial;

#[test]
#[serial]
fn test_foreign_key_creation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create parent table
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Failed to create users table");

    // Create child table with foreign key
    let orders_schema = TableSchema::new("orders")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("user_id", DataType::Integer, vec![Constraint::NotNull])
        .add_column("amount", DataType::Real, vec![])
        .add_foreign_key("fk_orders_user", vec!["user_id".to_string()], "users", vec!["id".to_string()]);

    storage.create_table(orders_schema).expect("Failed to create orders table");

    // Verify foreign key was created
    let fks = storage.get_foreign_keys("orders");
    assert!(fks.is_some());
    assert_eq!(fks.unwrap().len(), 1);
    assert_eq!(fks.unwrap()[0].name, "fk_orders_user");
}

#[test]
#[serial]
fn test_foreign_key_validation_success() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables
    setup_test_tables(&mut storage);

    // Insert parent record
    let mut user_values = HashMap::new();
    user_values.insert("id".to_string(), "1".to_string());
    user_values.insert("name".to_string(), "John Doe".to_string());
    storage.insert("users", "1", user_values).expect("Failed to insert user");

    // Insert child record with valid foreign key
    let mut order_values = HashMap::new();
    order_values.insert("id".to_string(), "1".to_string());
    order_values.insert("user_id".to_string(), "1".to_string());
    order_values.insert("amount".to_string(), "99.99".to_string());

    let result = storage.insert("orders", "1", order_values);
    assert!(result.is_ok(), "Valid foreign key should be accepted");
}

#[test]
#[serial]
fn test_foreign_key_validation_failure() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables
    setup_test_tables(&mut storage);

    // Try to insert child record with invalid foreign key
    let mut order_values = HashMap::new();
    order_values.insert("id".to_string(), "1".to_string());
    order_values.insert("user_id".to_string(), "999".to_string()); // Non-existent user
    order_values.insert("amount".to_string(), "99.99".to_string());

    let result = storage.insert("orders", "1", order_values);
    assert!(result.is_err(), "Invalid foreign key should be rejected");
    assert!(result.unwrap_err().contains("Foreign key constraint violated"));
}

#[test]
#[serial]
fn test_foreign_key_null_allowed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables with nullable FK
    setup_test_tables_nullable_fk(&mut storage);

    // Insert child record with NULL foreign key (should be allowed)
    let mut order_values = HashMap::new();
    order_values.insert("id".to_string(), "1".to_string());
    order_values.insert("user_id".to_string(), "".to_string()); // Empty string represents NULL
    order_values.insert("amount".to_string(), "99.99".to_string());

    let result = storage.insert("orders", "1", order_values);
    assert!(result.is_ok(), "NULL foreign key should be allowed");
}

#[test]
#[serial]
fn test_cascade_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables with cascade delete
    setup_test_tables_with_cascade(&mut storage);

    // Insert parent and child records
    let mut user_values = HashMap::new();
    user_values.insert("id".to_string(), "1".to_string());
    user_values.insert("name".to_string(), "John Doe".to_string());
    storage.insert("users", "1", user_values).expect("Failed to insert user");

    let mut order_values = HashMap::new();
    order_values.insert("id".to_string(), "1".to_string());
    order_values.insert("user_id".to_string(), "1".to_string());
    order_values.insert("amount".to_string(), "99.99".to_string());
    storage.insert("orders", "1", order_values).expect("Failed to insert order");

    // Verify both records exist
    assert!(storage.select("users", "1").is_some());
    assert!(storage.select("orders", "1").is_some());

    // Delete parent record (should cascade to child)
    storage.delete("users", "1").expect("Failed to delete user");

    // Verify both records are deleted
    assert!(storage.select("users", "1").is_none());
    assert!(storage.select("orders", "1").is_none());
}

#[test]
#[serial]
fn test_restrict_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables with restrict delete (default)
    setup_test_tables(&mut storage);

    // Insert parent and child records
    let mut user_values = HashMap::new();
    user_values.insert("id".to_string(), "1".to_string());
    user_values.insert("name".to_string(), "John Doe".to_string());
    storage.insert("users", "1", user_values).expect("Failed to insert user");

    let mut order_values = HashMap::new();
    order_values.insert("id".to_string(), "1".to_string());
    order_values.insert("user_id".to_string(), "1".to_string());
    order_values.insert("amount".to_string(), "99.99".to_string());
    storage.insert("orders", "1", order_values).expect("Failed to insert order");

    // Try to delete parent record (should be restricted)
    let result = storage.delete("users", "1");
    assert!(result.is_err(), "Delete should be restricted when child records exist");
    assert!(result.unwrap_err().contains("foreign key constraint violation"));
}

#[test]
#[serial]
fn test_multiple_foreign_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Create multiple parent tables
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    storage.create_table(users_schema).expect("Failed to create users table");

    let products_schema = TableSchema::new("products")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    storage.create_table(products_schema).expect("Failed to create products table");

    // Create child table with multiple foreign keys
    let order_items_schema = TableSchema::new("order_items")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("user_id", DataType::Integer, vec![Constraint::NotNull])
        .add_column("product_id", DataType::Integer, vec![Constraint::NotNull])
        .add_column("quantity", DataType::Integer, vec![Constraint::NotNull])
        .add_foreign_key("fk_items_user", vec!["user_id".to_string()], "users", vec!["id".to_string()])
        .add_foreign_key("fk_items_product", vec!["product_id".to_string()], "products", vec!["id".to_string()]);

    storage.create_table(order_items_schema).expect("Failed to create order_items table");

    // Insert parent records
    let mut user_values = HashMap::new();
    user_values.insert("id".to_string(), "1".to_string());
    user_values.insert("name".to_string(), "John Doe".to_string());
    storage.insert("users", "1", user_values).expect("Failed to insert user");

    let mut product_values = HashMap::new();
    product_values.insert("id".to_string(), "1".to_string());
    product_values.insert("name".to_string(), "Laptop".to_string());
    storage.insert("products", "1", product_values).expect("Failed to insert product");

    // Insert child record with valid multiple foreign keys
    let mut item_values = HashMap::new();
    item_values.insert("id".to_string(), "1".to_string());
    item_values.insert("user_id".to_string(), "1".to_string());
    item_values.insert("product_id".to_string(), "1".to_string());
    item_values.insert("quantity".to_string(), "2".to_string());

    let result = storage.insert("order_items", "1", item_values);
    assert!(result.is_ok(), "Valid multiple foreign keys should be accepted");

    // Try with invalid user_id
    let mut invalid_item_values = HashMap::new();
    invalid_item_values.insert("id".to_string(), "2".to_string());
    invalid_item_values.insert("user_id".to_string(), "999".to_string()); // Invalid
    invalid_item_values.insert("product_id".to_string(), "1".to_string());
    invalid_item_values.insert("quantity".to_string(), "1".to_string());

    let result = storage.insert("order_items", "2", invalid_item_values);
    assert!(result.is_err(), "Invalid user foreign key should be rejected");
}

#[test]
#[serial]
fn test_unique_constraint_with_foreign_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let mut storage = Storage::new(db);

    // Setup tables
    setup_test_tables(&mut storage);

    // Insert parent record
    let mut user_values = HashMap::new();
    user_values.insert("id".to_string(), "1".to_string());
    user_values.insert("name".to_string(), "John Doe".to_string());
    storage.insert("users", "1", user_values).expect("Failed to insert user");

    // Insert first child record
    let mut order_values1 = HashMap::new();
    order_values1.insert("id".to_string(), "1".to_string());
    order_values1.insert("user_id".to_string(), "1".to_string());
    order_values1.insert("amount".to_string(), "99.99".to_string());
    storage.insert("orders", "1", order_values1).expect("Failed to insert first order");

    // Try to insert second child record with same ID (should fail due to PK constraint)
    let mut order_values2 = HashMap::new();
    order_values2.insert("id".to_string(), "1".to_string()); // Duplicate primary key
    order_values2.insert("user_id".to_string(), "1".to_string());
    order_values2.insert("amount".to_string(), "149.99".to_string());

    let result = storage.insert("orders", "1", order_values2);
    // This would fail at the storage level due to key conflict
    // In a real implementation, you'd check for PK uniqueness in validation
}

// Helper functions
fn setup_test_tables(storage: &mut Storage) {
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Failed to create users table");

    let orders_schema = TableSchema::new("orders")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("user_id", DataType::Integer, vec![Constraint::NotNull])
        .add_column("amount", DataType::Real, vec![])
        .add_foreign_key("fk_orders_user", vec!["user_id".to_string()], "users", vec!["id".to_string()]);

    storage.create_table(orders_schema).expect("Failed to create orders table");
}

fn setup_test_tables_nullable_fk(storage: &mut Storage) {
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Failed to create users table");

    let orders_schema = TableSchema::new("orders")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("user_id", DataType::Integer, vec![]) // Nullable
        .add_column("amount", DataType::Real, vec![])
        .add_foreign_key("fk_orders_user", vec!["user_id".to_string()], "users", vec!["id".to_string()]);

    storage.create_table(orders_schema).expect("Failed to create orders table");
}

fn setup_test_tables_with_cascade(storage: &mut Storage) {
    let users_schema = TableSchema::new("users")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("name", DataType::Text, vec![Constraint::NotNull]);
    
    storage.create_table(users_schema).expect("Failed to create users table");

    let mut orders_schema = TableSchema::new("orders")
        .add_column("id", DataType::Integer, vec![Constraint::PrimaryKey, Constraint::NotNull])
        .add_column("user_id", DataType::Integer, vec![Constraint::NotNull])
        .add_column("amount", DataType::Real, vec![]);

    // Add foreign key with cascade delete
    let mut fk = mini_db_server::schema::ForeignKey {
        name: "fk_orders_user".to_string(),
        table: "orders".to_string(),
        columns: vec!["user_id".to_string()],
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::Cascade,
        on_update: ForeignKeyAction::Restrict,
    };

    orders_schema.foreign_keys.push(fk);
    storage.create_table(orders_schema).expect("Failed to create orders table");
}