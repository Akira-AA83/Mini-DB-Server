use mini_db_server::parser::SQLParser;
use mini_db_server::query::QueryExecutor;
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn test_full_sql_workflow() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 100, 60);
    
    // 1. Create the table
    let create_table_sql = r#"
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            salary REAL,
            active BOOLEAN
        )
    "#;
    
    let create_parsed = SQLParser::parse_query(create_table_sql).expect("CREATE parsing fallito");
    let create_result = query_executor.execute_query(&create_parsed, None).expect("CREATE fallito");
    assert!(create_result.contains("creata con successo"));
    
    // 2. Inserisci alcuni record
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(), 
        None
    ).unwrap();
    
    let insert_queries = vec![
        "INSERT INTO employees (id, name, email, salary, active) VALUES (1, 'Alice Johnson', 'alice@company.com', 75000.50, true)",
        "INSERT INTO employees (id, name, email, salary, active) VALUES (2, 'Bob Smith', 'bob@company.com', 65000.00, true)",
        "INSERT INTO employees (id, name, email, salary, active) VALUES (3, 'Charlie Brown', 'charlie@company.com', 55000.75, false)",
    ];
    
    for insert_sql in insert_queries {
        let insert_parsed = SQLParser::parse_query(insert_sql).expect("INSERT parsing fallito");
        let _insert_result = query_executor.execute_query(&insert_parsed, Some(tx_id.clone())).expect("INSERT fallito");
    }
    
    query_executor.execute_query(
        &SQLParser::parse_query("COMMIT").unwrap(),
        None
    ).unwrap();
    
    // 3. Testa SELECT per verificare i dati
    let select_all_sql = "SELECT * FROM employees";
    let select_parsed = SQLParser::parse_query(select_all_sql).expect("SELECT parsing fallito");
    let select_result = query_executor.execute_query(&select_parsed, None).expect("SELECT fallito");
    
    assert!(select_result.contains("Alice Johnson"));
    assert!(select_result.contains("Bob Smith"));
    assert!(select_result.contains("Charlie Brown"));
    
    // 4. Testa UPDATE
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(),
        None
    ).unwrap();
    
    let update_sql = "UPDATE employees SET salary = 80000.00 WHERE name = 'Alice Johnson'";
    let update_parsed = SQLParser::parse_query(update_sql).expect("UPDATE parsing fallito");
    let _update_result = query_executor.execute_query(&update_parsed, Some(tx_id.clone())).expect("UPDATE fallito");
    
    query_executor.execute_query(
        &SQLParser::parse_query("COMMIT").unwrap(),
        None
    ).unwrap();
    
    // 5. Verify that l'update sia andato a buon fine
    let select_updated_sql = "SELECT * FROM employees WHERE name = 'Alice Johnson'";
    let select_updated_parsed = SQLParser::parse_query(select_updated_sql).expect("SELECT parsing fallito");
    let select_updated_result = query_executor.execute_query(&select_updated_parsed, None).expect("SELECT fallito");
    
    assert!(select_updated_result.contains("80000"));
    
    // 6. Testa DELETE
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(),
        None
    ).unwrap();
    
    let delete_sql = "DELETE FROM employees WHERE active = false";
    let delete_parsed = SQLParser::parse_query(delete_sql).expect("DELETE parsing fallito");
    let _delete_result = query_executor.execute_query(&delete_parsed, Some(tx_id.clone())).expect("DELETE fallito");
    
    query_executor.execute_query(
        &SQLParser::parse_query("COMMIT").unwrap(),
        None
    ).unwrap();
    
    // 7. Verify that Charlie sia stato eliminato
    let select_final_sql = "SELECT * FROM employees";
    let select_final_parsed = SQLParser::parse_query(select_final_sql).expect("SELECT parsing fallito");
    let select_final_result = query_executor.execute_query(&select_final_parsed, None).expect("SELECT fallito");
    
    assert!(select_final_result.contains("Alice Johnson"));
    assert!(select_final_result.contains("Bob Smith"));
    assert!(!select_final_result.contains("Charlie Brown")); // Charlie should have been deleted
}

#[test]
fn test_create_multiple_tables() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 100, 60);
    
    // Crea tabella users
    let create_users_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)";
    let create_users_parsed = SQLParser::parse_query(create_users_sql).expect("CREATE users parsing fallito");
    let _create_users_result = query_executor.execute_query(&create_users_parsed, None).expect("CREATE users fallito");
    
    // Crea tabella orders
    let create_orders_sql = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, created_at TIMESTAMP)";
    let create_orders_parsed = SQLParser::parse_query(create_orders_sql).expect("CREATE orders parsing fallito");
    let _create_orders_result = query_executor.execute_query(&create_orders_parsed, None).expect("CREATE orders fallito");
    
    // Testa inserimenti in entrambe le tabelle
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(),
        None
    ).unwrap();
    
    // Inserisci utente
    let insert_user_sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
    let insert_user_parsed = SQLParser::parse_query(insert_user_sql).expect("INSERT user parsing fallito");
    let _insert_user_result = query_executor.execute_query(&insert_user_parsed, Some(tx_id.clone())).expect("INSERT user fallito");
    
    // Inserisci ordine
    let insert_order_sql = "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.99)";
    let insert_order_parsed = SQLParser::parse_query(insert_order_sql).expect("INSERT order parsing fallito");
    let _insert_order_result = query_executor.execute_query(&insert_order_parsed, Some(tx_id.clone())).expect("INSERT order fallito");
    
    query_executor.execute_query(
        &SQLParser::parse_query("COMMIT").unwrap(),
        None
    ).unwrap();
    
    // Verify that i dati siano stati inseriti correttamente in entrambe le tabelle
    let select_users_sql = "SELECT * FROM users";
    let select_users_parsed = SQLParser::parse_query(select_users_sql).expect("SELECT users parsing fallito");
    let select_users_result = query_executor.execute_query(&select_users_parsed, None).expect("SELECT users fallito");
    
    let select_orders_sql = "SELECT * FROM orders";
    let select_orders_parsed = SQLParser::parse_query(select_orders_sql).expect("SELECT orders parsing fallito");
    let select_orders_result = query_executor.execute_query(&select_orders_parsed, None).expect("SELECT orders fallito");
    
    assert!(select_users_result.contains("John Doe"));
    assert!(select_orders_result.contains("99.99"));
}

#[test]
fn test_constraint_validation() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 100, 60);
    
    // Crea tabella con vincoli stringenti
    let create_sql = "CREATE TABLE strict_table (id INTEGER PRIMARY KEY, required_field TEXT NOT NULL, unique_field TEXT UNIQUE)";
    let create_parsed = SQLParser::parse_query(create_sql).expect("CREATE parsing fallito");
    let _create_result = query_executor.execute_query(&create_parsed, None).expect("CREATE fallito");
    
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(),
        None
    ).unwrap();
    
    // Prova inserimento valido
    let valid_insert_sql = "INSERT INTO strict_table (id, required_field, unique_field) VALUES (1, 'valid', 'unique1')";
    let valid_insert_parsed = SQLParser::parse_query(valid_insert_sql).expect("INSERT parsing fallito");
    let valid_result = query_executor.execute_query(&valid_insert_parsed, Some(tx_id.clone()));
    assert!(valid_result.is_ok(), "L'inserimento valido should work");
    
    query_executor.execute_query(
        &SQLParser::parse_query("COMMIT").unwrap(),
        None
    ).unwrap();
    
    // Prova inserimento non valido (campo obbligatorio mancante)
    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(
        &SQLParser::parse_query("BEGIN TRANSACTION").unwrap(),
        None
    ).unwrap();
    
    let invalid_insert_sql = "INSERT INTO strict_table (id, unique_field) VALUES (2, 'unique2')"; // Manca required_field
    let invalid_insert_parsed = SQLParser::parse_query(invalid_insert_sql).expect("INSERT parsing fallito");
    let invalid_result = query_executor.execute_query(&invalid_insert_parsed, Some(tx_id.clone()));
    
    // L'inserimento should fail
    assert!(invalid_result.is_err(), "L'inserimento senza campo obbligatorio should fail");
    assert!(invalid_result.unwrap_err().contains("non può essere NULL"));
}