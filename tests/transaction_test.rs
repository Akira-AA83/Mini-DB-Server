use mini_db_server::query::QueryExecutor;
use mini_db_server::parser::ParsedQuery;
use std::collections::HashMap;
use uuid::Uuid;
use tempfile::tempdir;
use std::sync::Arc;

#[test]
fn test_transaction_commit() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 10, 60);

    // Avvia una transazione (nuovo formato)
    query_executor.execute_query(&ParsedQuery::BeginTransaction, None).unwrap();

    // Esegui un'operazione dentro la transazione
    query_executor.execute_query(&ParsedQuery::Insert { 
        table: "users".to_string(), 
        values: HashMap::from([
            ("id".to_string(), "1".to_string()), 
            ("name".to_string(), "Alice".to_string())
        ]) 
    }, None).unwrap();

    // Commit della transazione (nuovo formato)
    let commit = query_executor.execute_query(&ParsedQuery::Commit, None);
    assert!(commit.is_ok());
}

#[test]
fn test_transaction_rollback() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 10, 60);

    // Avvia una transazione (nuovo formato)
    query_executor.execute_query(&ParsedQuery::BeginTransaction, None).unwrap();

    // Esegui un'operazione dentro la transazione
    query_executor.execute_query(&ParsedQuery::Insert { 
        table: "users".to_string(), 
        values: HashMap::from([
            ("id".to_string(), "2".to_string()), 
            ("name".to_string(), "Bob".to_string())
        ]) 
    }, None).unwrap();

    // Rollback della transazione (nuovo formato)
    let rollback = query_executor.execute_query(&ParsedQuery::Rollback, None);
    assert!(rollback.is_ok());

    // Controlla che l'inserimento non sia avvenuto
    let select_query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],  // * Aggiunto campo columns
        joins: vec![],
        conditions: None,  // * Changed to Option<String>
        order_by: None,
        limit: None,
        group_by: None,
        aggregates: None,
        having: None,
        ctes: None,
        window_functions: None,
        case_expressions: None,
    };
    let result = query_executor.execute_query(&select_query, None).expect("Select failed");
    assert!(!result.contains("Bob"), "L'utente Bob non dovrebbe esistere dopo il rollback");
}