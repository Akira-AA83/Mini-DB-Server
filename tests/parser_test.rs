use mini_db_server::parser::{SQLParser, ParsedQuery};

#[test]
fn test_select_query_with_joins() {
    let query = "SELECT users.id, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice' ORDER BY orders.amount LIMIT 10";
    let parsed = SQLParser::parse_query(query).expect("Parsing fallito");
    
    if let ParsedQuery::Select { table, joins, conditions, order_by, limit, .. } = parsed {
        assert_eq!(table, "users");
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0], ("users".to_string(), "id".to_string(), "user_id".to_string()));
        // * FIXED: conditions is now Option<String>, check if it contains the expected condition
        assert!(conditions.as_ref().map(|c| c.contains("users.name = 'Alice'")).unwrap_or(false));
        assert_eq!(order_by, Some("orders.amount".to_string()));
        assert_eq!(limit, Some(10));
    } else {
        panic!("Il parsing della query SELECT con JOIN non ha restituito il risultato atteso");
    }
}

#[test]
fn test_select_query_with_group_by() {
    let query = "SELECT users.country, COUNT(*) FROM users GROUP BY users.country";
    let parsed = SQLParser::parse_query(query).expect("Parsing fallito");
    
    if let ParsedQuery::Select { group_by, aggregates, .. } = parsed {
        assert!(group_by.is_some());
        assert_eq!(group_by.unwrap(), vec!["users.country".to_string()]);
        assert!(aggregates.is_some());
        assert!(aggregates.unwrap().contains_key("*"));
    } else {
        panic!("Il parsing della query SELECT con GROUP BY non ha restituito il risultato atteso");
    }
}

#[test]
fn test_select_query_with_order_by() {
    let query = "SELECT * FROM users ORDER BY created_at DESC";
    let parsed = SQLParser::parse_query(query).expect("Parsing fallito");
    
    if let ParsedQuery::Select { order_by, .. } = parsed {
        assert_eq!(order_by, Some("created_at".to_string()));
    } else {
        panic!("Il parsing della query SELECT con ORDER BY non ha restituito il risultato atteso");
    }
}

#[test]
fn test_select_query_with_limit() {
    let query = "SELECT * FROM users LIMIT 5";
    let parsed = SQLParser::parse_query(query).expect("Parsing fallito");
    
    if let ParsedQuery::Select { limit, .. } = parsed {
        assert_eq!(limit, Some(5));
    } else {
        panic!("Il parsing della query SELECT con LIMIT non ha restituito il risultato atteso");
    }
}