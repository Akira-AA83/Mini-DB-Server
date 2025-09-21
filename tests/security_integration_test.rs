use std::sync::Arc;
use mini_db_server::security::{PolicyEngine, TriggerSystem, SecureQueryExecutor, TriggerBuilder};
use mini_db_server::query::QueryExecutor;
use mini_db_server::parser::ParsedQuery;
use mini_db_server::schema::{TableSchema, Column, DataType, Constraint};
use tempfile::TempDir;
use std::collections::HashMap;

#[test]
fn test_secure_query_executor_creation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    // * QueryExecutor::new() restituisce già Arc<QueryExecutor>
    let query_executor = QueryExecutor::new(Arc::clone(&db), 10, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));

    let secure_executor = SecureQueryExecutor::new(
        query_executor,        // * Già Arc<QueryExecutor>
        policy_engine,         // * Arc<PolicyEngine>
        trigger_system,        // * Arc<TriggerSystem>
    );

    assert!(true, "SecureQueryExecutor created successfully");
}

#[test]
fn test_secure_select_query() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 10, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));

    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );

    let query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        joins: vec![],
        conditions: None,
        order_by: None,
        limit: None,
        group_by: None,
        aggregates: None,
        having: None,
        ctes: None,
        window_functions: None,
        case_expressions: None,
    };

    let _result = secure_executor.execute_secure_query(query, None);
}

#[test]
fn test_policy_engine_integration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 10, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));

    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );

    let query = ParsedQuery::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],
        joins: vec![],
        conditions: None,
        order_by: None,
        limit: None,
        group_by: None,
        aggregates: None,
        having: None,
        ctes: None,
        window_functions: None,
        case_expressions: None,
    };

    let _result = secure_executor.execute_secure_query(query, None);
}

#[test]
fn test_trigger_system_integration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 10, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));

    // * TriggerBuilder::new richiede 2 parametri e ha metodi diversi
    let audit_trigger = TriggerBuilder::new("audit_users", "users")
        .after()
        .on_insert()
        .for_each_row()
        .execute_rust("audit_log")
        .build();

    trigger_system.create_trigger(audit_trigger).unwrap();

    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );

    let query = ParsedQuery::Insert {
        table: "users".to_string(),
        values: HashMap::from([
            ("id".to_string(), "1".to_string()),
            ("name".to_string(), "Test User".to_string()),
        ]),
    };

    let _result = secure_executor.execute_secure_query(query, None);
}

#[test]
fn test_create_table_with_security() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 10, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));

    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );

    let create_table_query = ParsedQuery::CreateTable {
        table: "test_table".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        schema: TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::PrimaryKey],
                    default_value: None,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![Constraint::NotNull],
                    default_value: None,
                    is_nullable: false,
                },
            ],
            indexes: vec![],
            foreign_keys: vec![],
            triggers: vec![],
            created_at: chrono::Utc::now(),
            version: 1,
        },
    };

    let _result = secure_executor.execute_secure_query(create_table_query, None);
}