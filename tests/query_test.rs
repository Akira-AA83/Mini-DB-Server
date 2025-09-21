use mini_db_server::query::QueryExecutor;
use mini_db_server::parser::ParsedQuery;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};
use uuid::Uuid;
use sled::Db;
use lazy_static::lazy_static;
use mini_db_server::query::QueryResponse;

use serial_test::serial;


/// A `Once` to initialize the database only once for all tests.
static INIT: Once = Once::new();

/// Shared database for all tests.
lazy_static! {
    static ref DB: Arc<Db> = {
        INIT.call_once(|| {
            let _ = std::fs::remove_dir_all("test_db");
        });
        // Use an absolute or relative path to the test directory
        Arc::new(sled::open("./test_db").expect("Error opening DB"))
    };
}

// In query_test.rs
fn clear_database() {
    let db = DB.clone();
    for tree_name in db.tree_names() {
        let tree = db.open_tree(tree_name).unwrap();
        for item in tree.iter() {
            if let Ok((key, _)) = item {
                tree.remove(key).unwrap();
            }
        }
    }
    db.flush().unwrap();
}

/// Helper to get a `QueryExecutor` with the shared database.
fn get_query_executor() -> Arc<QueryExecutor> {
    QueryExecutor::new(Arc::clone(&DB), 100, 60)
}

/// Helper to clean the database before each test
/*fn clear_database() {
    let db = DB.clone();
    for tree_name in db.tree_names() {
        let tree = db.open_tree(tree_name).unwrap();
        tree.clear().unwrap();
    }
}*/

#[test]
#[serial]
fn test_insert_and_select() {
    clear_database();

    let query_executor = get_query_executor();

    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(&ParsedQuery::BeginTransaction, Some(tx_id.clone())).unwrap();

    let tree = query_executor.get_db().open_tree("users").unwrap();
    println!("DEBUG TEST: Tabella `users` esiste? {}", tree.is_empty());


    let mut insert_values = HashMap::new();
    insert_values.insert("id".to_string(), "1".to_string());
    insert_values.insert("name".to_string(), "Alice".to_string());

    let insert_query = ParsedQuery::Insert {
        table: "users".to_string(),
        values: insert_values,
    };

    query_executor.execute_query(&insert_query, Some(tx_id.clone())).expect("Insert failed");
    query_executor.execute_query(&ParsedQuery::Commit, Some(tx_id.clone())).unwrap();

    let select_query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],  // * FIXED: Added missing columns field
        joins: vec![],
        conditions: Some("name = 'Alice'".to_string()),  // * FIXED: Changed to Option<String>
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
    
    let response: QueryResponse = serde_json::from_str(&result).expect("Error in deserialization of response");
    println!("DEBUG PARSED RESPONSE: {:?}", response);

    let parsed_result = response.results.expect("No results in query");
    println!("DEBUG PARSED RESULT: {:?}", parsed_result);

    let name = &parsed_result[0]["name"];
    println!("DEBUG NAME: {}", name);

    assert_eq!(name, "Alice");
    
}

#[test]
#[serial]
fn test_update() {
    clear_database();
    let query_executor = get_query_executor();

    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(&ParsedQuery::BeginTransaction, Some(tx_id.clone())).unwrap();

    let mut insert_values = HashMap::new();
    insert_values.insert("id".to_string(), "2".to_string());
    insert_values.insert("name".to_string(), "Bob".to_string());

    let insert_query = ParsedQuery::Insert {
        table: "users".to_string(),
        values: insert_values,
    };
    query_executor.execute_query(&insert_query, Some(tx_id.clone())).expect("Insert failed");

    let mut update_values = HashMap::new();
    update_values.insert("name".to_string(), "Robert".to_string());

    let update_query = ParsedQuery::Update {
        table: "users".to_string(),
        values: update_values,
        conditions: Some("id = '2'".to_string()),  // * FIXED: Changed to Option<String>
    };
    query_executor.execute_query(&update_query, Some(tx_id.clone())).expect("Update failed");
    query_executor.execute_query(&ParsedQuery::Commit, Some(tx_id.clone())).unwrap();

    let check_query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],  // * FIXED: Added missing columns field
        joins: vec![],
        conditions: Some("id = '2'".to_string()),  // * FIXED: Changed to Option<String>
        order_by: None,
        limit: None,
        group_by: None,
        aggregates: None,
        having: None,
        ctes: None,
        window_functions: None,
        case_expressions: None,
    };

    let result = query_executor.execute_query(&check_query, None).expect("Select failed");
    assert!(result.contains("Robert"));
}

#[test]
#[serial]
fn test_delete() {
    clear_database();
    let query_executor = get_query_executor();

    let tx_id = Uuid::new_v4().to_string();
    query_executor.execute_query(&ParsedQuery::BeginTransaction, Some(tx_id.clone())).unwrap();

    let mut insert_values = HashMap::new();
    insert_values.insert("id".to_string(), "3".to_string());
    insert_values.insert("name".to_string(), "Charlie".to_string());

    let insert_query = ParsedQuery::Insert {
        table: "users".to_string(),
        values: insert_values,
    };
    query_executor.execute_query(&insert_query, Some(tx_id.clone())).expect("Insert failed");

    let delete_query = ParsedQuery::Delete {
        table: "users".to_string(),
        conditions: Some("id = '3'".to_string()),  // * FIXED: Changed to Option<String>
    };
    query_executor.execute_query(&delete_query, Some(tx_id.clone())).expect("Delete failed");
    query_executor.execute_query(&ParsedQuery::Commit, Some(tx_id.clone())).unwrap();

    let check_query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],  // * FIXED: Added missing columns field
        joins: vec![],
        conditions: None,  // * FIXED: Changed to Option<String> (no conditions)
        order_by: None,
        limit: None,
        group_by: None,
        aggregates: None,
        having: None,
        ctes: None,
        window_functions: None,
        case_expressions: None,
    };

    let result = query_executor.execute_query(&check_query, None).expect("Select failed");
    assert!(!result.contains("Charlie"));
}
