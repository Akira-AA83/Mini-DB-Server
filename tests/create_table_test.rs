use mini_db_server::parser::SQLParser;
use mini_db_server::query::QueryExecutor;
use mini_db_server::parser::ParsedQuery;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_create_table_parser() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
    let parsed = SQLParser::parse_query(sql).unwrap();
    
    if let ParsedQuery::CreateTable { schema, table, columns } = parsed {
        assert_eq!(table, "users");
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 3);
        assert!(columns.len() >= 3);
    } else {
        panic!("Expected CreateTable variant");
    }
}

#[test]
fn test_create_table_with_constraints() {
    let sql = "CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price DECIMAL CHECK (price > 0),
        category_id INTEGER REFERENCES categories(id)
    )";
    
    let parsed = SQLParser::parse_query(sql).unwrap();
    
    if let ParsedQuery::CreateTable { schema, table, columns } = parsed {
        assert_eq!(table, "products");
        assert_eq!(schema.name, "products");
        assert_eq!(schema.columns.len(), 4);
        assert!(columns.len() >= 4);
    } else {
        panic!("Expected CreateTable variant");
    }
}

#[test] 
fn test_create_table_with_multiple_constraints() {
    let sql = "CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        total DECIMAL NOT NULL CHECK (total >= 0),
        status TEXT DEFAULT 'pending'
    )";
    
    let parsed = SQLParser::parse_query(sql).unwrap();
    
    if let ParsedQuery::CreateTable { schema, table, columns } = parsed {
        assert_eq!(table, "orders");
        assert_eq!(schema.name, "orders");
        assert_eq!(schema.columns.len(), 4);
        assert!(columns.len() >= 4);
    } else {
        panic!("Expected CreateTable variant");
    }
}

#[test]
fn test_create_table_execution() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 10, 60);

    // Test simple CREATE TABLE
    query_executor.execute_query(&ParsedQuery::BeginTransaction, None).unwrap();
    
    let sql = "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    let parsed = SQLParser::parse_query(sql).unwrap();
    let result = query_executor.execute_query(&parsed, None);
    
    assert!(result.is_ok(), "CREATE TABLE should succeed");
    
    query_executor.execute_query(&ParsedQuery::Commit, None).unwrap();
}

#[test]
fn test_create_table_with_foreign_key() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    let query_executor = QueryExecutor::new(db, 10, 60);

    query_executor.execute_query(&ParsedQuery::BeginTransaction, None).unwrap();
    
    let sql = "CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        user_id INTEGER REFERENCES users(id)
    )";
    
    let parsed = SQLParser::parse_query(sql).unwrap();
    
    if let ParsedQuery::CreateTable { schema, table, columns } = parsed {
        assert_eq!(table, "posts");
        assert_eq!(schema.name, "posts");
        assert_eq!(schema.columns.len(), 3);
        assert!(columns.len() >= 3);
        
        let result = query_executor.execute_query(
            &ParsedQuery::CreateTable { schema, table, columns }, 
            None
        );
        assert!(result.is_ok(), "CREATE TABLE with foreign key should succeed");
    } else {
        panic!("Expected CreateTable variant");
    }
    
    query_executor.execute_query(&ParsedQuery::Commit, None).unwrap();
}