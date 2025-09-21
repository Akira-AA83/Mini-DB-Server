/*
📌 File: src/parser.rs (COMPLETE FIXED VERSION)
🔄 Fixed all compilation errors
✅ Proper imports and derives
✅ All ParsedQuery variants included
✅ Compatible with existing codebase
*/

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{
    Statement, Expr, Value, SetExpr, JoinOperator, SelectItem, JoinConstraint,
    TableFactor, Assignment, ObjectName, Query, ColumnDef, DataType as SqlDataType,
    GroupByExpr  // ✅ ADDED: Import GroupByExpr for proper handling
};
use std::collections::HashMap;
use crate::schema::{TableSchema, DataType, Constraint, Column};
use serde::{Serialize, Deserialize};  // ✅ ADDED: Explicit serde imports

// ✅ FIXED: Complete ParsedQuery definition with all variants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParsedQuery {
    Select { 
        table: String, 
        columns: Vec<String>,
        joins: Vec<(String, String, String)>,  // ✅ FIXED: Proper join structure
        conditions: Option<String>,
        order_by: Option<String>,
        limit: Option<usize>,
        group_by: Option<Vec<String>>,
        aggregates: Option<HashMap<String, String>>,  // ✅ FIXED: Proper aggregates
        having: Option<String>,  // ✅ NEW: HAVING clause support
        ctes: Option<Vec<(String, String)>>,  // ✅ NEW: CTEs support (name, query)
        window_functions: Option<Vec<(String, String, String)>>,  // ✅ NEW: Window functions (function, alias, over_clause)
        case_expressions: Option<Vec<(String, String, String)>>,  // ✅ NEW: CASE expressions (expression, alias, when_clauses)
    },
    Insert { 
        table: String, 
        values: HashMap<String, String> 
    },
    Update { 
        table: String, 
        values: HashMap<String, String>, 
        conditions: Option<String>
    },
    Delete { 
        table: String, 
        conditions: Option<String>
    },
    CreateTable { 
        table: String,
        columns: Vec<String>,
        schema: TableSchema
    },
    DropTable {
        table: String
    },
    // Database management commands
    CreateDatabase {
        name: String,
        description: Option<String>,
        if_not_exists: bool
    },
    UseDatabase {
        name: String
    },
    ShowDatabases,
    ShowTables,
    ShowUsers,
    ShowStatus,
    DescribeTable {
        table: String
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    Auth {
        credentials: String
    },
    LoadModule {
        module_name: String,
        file_path: String,
    },
    WasmExec {
        module_name: String,
        function_name: String,
        args: Vec<String>,
    },
    Subscribe {
        table: String
    },
    Unsubscribe {
        table: String
    },
    DropDatabase {
        name: String
    },
    BeginTransaction,
    Commit,           // ✅ FIXED: Added missing variants
    Rollback,         // ✅ FIXED: Added missing variants
    // Legacy versions for backward compatibility
    BeginTransactionLegacy { tx_id: String },
    CommitTransactionLegacy { tx_id: String },
    RollbackTransactionLegacy { tx_id: String },
}

pub struct SQLParser;

impl SQLParser {
    pub fn parse_query(query: &str) -> Result<ParsedQuery, String> {
        // FIRST: Check for WASM_EXEC anywhere in the query (highest priority)
        if query.to_uppercase().contains("WASM_EXEC(") {
            return Self::parse_wasm_exec(query);
        }
        
        // Then check for custom database commands
        let trimmed_query = query.trim().to_uppercase();
        
        // Handle CREATE DATABASE command
        if trimmed_query.starts_with("CREATE DATABASE") {
            return Self::parse_create_database(query);
        }
        
        // Handle CREATE INDEX command
        if trimmed_query.starts_with("CREATE INDEX") || trimmed_query.starts_with("CREATE UNIQUE INDEX") {
            return Self::parse_create_index(query);
        }
        
        // Handle USE DATABASE command
        if trimmed_query.starts_with("USE DATABASE") || trimmed_query.starts_with("USE ") {
            return Self::parse_use_database(query);
        }
        
        // Handle SHOW DATABASES command
        if trimmed_query == "SHOW DATABASES" {
            return Ok(ParsedQuery::ShowDatabases);
        }
        
        // Handle SHOW TABLES command
        if trimmed_query == "SHOW TABLES" {
            return Ok(ParsedQuery::ShowTables);
        }
        
        // Handle SHOW USERS command
        if trimmed_query == "SHOW USERS" {
            return Ok(ParsedQuery::ShowUsers);
        }
        
        // Handle SHOW STATUS command
        if trimmed_query == "SHOW STATUS" {
            return Ok(ParsedQuery::ShowStatus);
        }
        
        // Handle DESCRIBE command
        if trimmed_query.starts_with("DESCRIBE ") || trimmed_query.starts_with("DESC ") {
            return Self::parse_describe_table(query);
        }
        
        // Handle DROP DATABASE command
        if trimmed_query.starts_with("DROP DATABASE") {
            return Self::parse_drop_database(query);
        }
        
        // Handle SUBSCRIBE command
        if trimmed_query.starts_with("SUBSCRIBE ") {
            return Self::parse_subscribe(query);
        }
        
        // Handle UNSUBSCRIBE command
        if trimmed_query.starts_with("UNSUBSCRIBE ") {
            return Self::parse_unsubscribe(query);
        }
        
        // Handle AUTH command
        if trimmed_query.starts_with("AUTH ") {
            return Self::parse_auth(query);
        }
        
        // Handle LOAD MODULE command
        if trimmed_query.starts_with("LOAD MODULE ") {
            return Self::parse_load_module(query);
        }
        
        // Handle WASM_EXEC command (direct syntax without SELECT)
        if trimmed_query.starts_with("WASM_EXEC ") {
            return Self::parse_wasm_exec_direct(query);
        }
        
        // Handle standard SQL commands with sqlparser
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, query).map_err(|e| e.to_string())?;
        
        match ast.get(0) {
            Some(Statement::Query(query)) => {
                let table = SQLParser::extract_table_from_query(query)?;
                let columns = SQLParser::extract_columns(query);
                let joins = SQLParser::extract_joins(query);
                let conditions = SQLParser::extract_conditions_as_string(query);
                let order_by = SQLParser::extract_order_by(query);
                let limit = SQLParser::extract_limit(query);
                let (group_by, aggregates) = SQLParser::extract_group_by_and_aggregates(query);
                let having = SQLParser::extract_having(query);
                let ctes = SQLParser::extract_ctes(query);
                let window_functions = SQLParser::extract_window_functions(query);
                let case_expressions = SQLParser::extract_case_expressions(query);
                
                Ok(ParsedQuery::Select { 
                    table, columns, joins, conditions, order_by, limit, group_by, aggregates, having, ctes, window_functions, case_expressions,
                })
            }
            Some(Statement::CreateTable { name, columns, .. }) => 
                Self::parse_create_table(name, columns),
            Some(Statement::Insert { table_name, columns, source, .. }) => 
                Self::parse_insert(table_name, columns, source.as_ref().ok_or("INSERT without data")?),
            Some(Statement::Update { table, assignments, selection, .. }) => 
                Self::parse_update(&table.relation, assignments, selection),
            Some(Statement::Delete { from, selection, .. }) => 
                Self::parse_delete(&from[0].relation, selection),
            Some(Statement::Drop { object_type, names, .. }) => {
                match object_type {
                    sqlparser::ast::ObjectType::Table => {
                        if let Some(name) = names.get(0) {
                            Ok(ParsedQuery::DropTable {
                                table: name.to_string()
                            })
                        } else {
                            Err("DROP TABLE requires a table name".to_string())
                        }
                    }
                    _ => Err("Only DROP TABLE is supported".to_string())
                }
            }
            Some(Statement::StartTransaction { .. }) => 
                Ok(ParsedQuery::BeginTransaction),
            Some(Statement::Commit { .. }) => 
                Ok(ParsedQuery::Commit),
            Some(Statement::Rollback { .. }) => 
                Ok(ParsedQuery::Rollback),
            _ => Err("Unsupported query type".to_string()),
        }
    }

    // ✅ MAIN ENTRY POINT: Required by client.rs
    pub fn parse_sql(query: &str) -> Result<ParsedQuery, String> {
        Self::parse_query(query)
    }

    // ✅ Extract table from query
    fn extract_table_from_query(query: &Query) -> Result<String, String> {
        if let SetExpr::Select(select) = query.body.as_ref() {
            if let Some(table_with_joins) = select.from.get(0) {
                return Ok(table_with_joins.relation.to_string());
            }
        }
        Err("No table found in query".to_string())
    }

    // ✅ Extract columns from SELECT
    fn extract_columns(query: &Query) -> Vec<String> {
        let mut columns = Vec::new();
        
        if let SetExpr::Select(select) = query.body.as_ref() {
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        columns.push(expr.to_string());
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        columns.push(format!("{} AS {}", expr, alias));
                    }
                    SelectItem::QualifiedWildcard(object_name, _) => {
                        columns.push(format!("{}.*", object_name));
                    }
                    SelectItem::Wildcard(_) => {
                        columns.push("*".to_string());
                    }
                }
            }
        }
        
        if columns.is_empty() {
            columns.push("*".to_string());
        }
        
        columns
    }

    // ✅ FIXED: Extract joins - return proper tuple format
    fn extract_joins(query: &Query) -> Vec<(String, String, String)> {
        let mut joins = Vec::new();
        
        if let SetExpr::Select(select) = query.body.as_ref() {
            if let Some(table_with_joins) = select.from.get(0) {
                for join in &table_with_joins.joins {
                    let join_table = join.relation.to_string();
                    let join_type = match join.join_operator {
                        JoinOperator::Inner(_) => "INNER",
                        JoinOperator::LeftOuter(_) => "LEFT",
                        JoinOperator::RightOuter(_) => "RIGHT",
                        JoinOperator::FullOuter(_) => "FULL",
                        _ => "INNER",
                    };
                    
                    let join_condition = match &join.join_operator {
                        JoinOperator::Inner(constraint) |
                        JoinOperator::LeftOuter(constraint) |
                        JoinOperator::RightOuter(constraint) |
                        JoinOperator::FullOuter(constraint) => {
                            match constraint {
                                JoinConstraint::On(expr) => expr.to_string(),
                                JoinConstraint::Using(columns) => {
                                    format!("USING ({})", columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "))
                                }
                                _ => "TRUE".to_string(),
                            }
                        }
                        _ => "TRUE".to_string(),
                    };
                    
                    joins.push((join_table, join_type.to_string(), join_condition));
                }
            }
        }
        
        joins
    }

    // ✅ Extract conditions as string
    fn extract_conditions_as_string(query: &Query) -> Option<String> {
        if let SetExpr::Select(select) = query.body.as_ref() {
            if let Some(selection) = &select.selection {
                return Some(selection.to_string());
            }
        }
        None
    }

    // ✅ Extract ORDER BY
    fn extract_order_by(query: &Query) -> Option<String> {
        if !query.order_by.is_empty() {
            let order_items: Vec<String> = query.order_by.iter()
                .map(|item| {
                    let direction = if item.asc.unwrap_or(true) { "ASC" } else { "DESC" };
                    format!("{} {}", item.expr, direction)
                })
                .collect();
            Some(order_items.join(", "))
        } else {
            None
        }
    }

    // ✅ Extract LIMIT
    fn extract_limit(query: &Query) -> Option<usize> {
        if let Some(limit) = &query.limit {
            if let Expr::Value(Value::Number(num_str, _)) = limit {
                return num_str.parse().ok();
            }
        }
        None
    }

    // ✅ FIXED: Extract GROUP BY and aggregates - handle GroupByExpr correctly
    fn extract_group_by_and_aggregates(query: &Query) -> (Option<Vec<String>>, Option<HashMap<String, String>>) {
        let mut group_by = None;
        let mut aggregates = HashMap::new();
        
        if let SetExpr::Select(select) = query.body.as_ref() {
            // ✅ FIXED: Handle GroupByExpr enum properly
            match &select.group_by {
                GroupByExpr::All => {
                    group_by = Some(vec!["*".to_string()]);
                }
                GroupByExpr::Expressions(exprs) => {
                    if !exprs.is_empty() {
                        group_by = Some(exprs.iter().map(|e| e.to_string()).collect());
                    }
                }
            }
            
            // Extract aggregates from SELECT items
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        if let Expr::Function(func) = expr {
                            let func_name = func.name.to_string().to_uppercase();
                            if ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&func_name.as_str()) {
                                let args = func.args.iter()
                                    .map(|arg| arg.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                aggregates.insert(func_name, args);
                            }
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let Expr::Function(func) = expr {
                            let func_name = func.name.to_string().to_uppercase();
                            if ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&func_name.as_str()) {
                                let args = func.args.iter()
                                    .map(|arg| arg.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                aggregates.insert(func_name, args);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        
        (group_by, if aggregates.is_empty() { None } else { Some(aggregates) })
    }

    // Extract HAVING clause
    fn extract_having(query: &Query) -> Option<String> {
        if let SetExpr::Select(select) = query.body.as_ref() {
            let having = select.having.as_ref().map(|expr| expr.to_string());
            println!("🔍 DEBUG HAVING: Extracted having clause: {:?}", having);
            having
        } else {
            println!("🔍 DEBUG HAVING: No SetExpr::Select found");
            None
        }
    }

    // Extract CTEs (Common Table Expressions)
    fn extract_ctes(query: &Query) -> Option<Vec<(String, String)>> {
        if let Some(with) = &query.with {
            let mut ctes = Vec::new();
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let query_str = cte.query.to_string();
                println!("🔍 DEBUG CTE: Found CTE '{}' with query: {}", name, query_str);
                ctes.push((name, query_str));
            }
            if !ctes.is_empty() {
                println!("🔍 DEBUG CTE: Extracted {} CTEs", ctes.len());
                Some(ctes)
            } else {
                None
            }
        } else {
            println!("🔍 DEBUG CTE: No WITH clause found");
            None
        }
    }

    // Extract Window Functions (ROW_NUMBER, RANK, etc.)
    fn extract_window_functions(query: &Query) -> Option<Vec<(String, String, String)>> {
        if let SetExpr::Select(select_box) = &*query.body {
            let mut window_functions = Vec::new();
            
            for item in &select_box.projection {
                match item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let Some(window_func) = SQLParser::parse_window_function(expr) {
                            let alias_name = alias.value.clone();
                            window_functions.push((window_func.0, alias_name, window_func.1));
                        }
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        if let Some(window_func) = SQLParser::parse_window_function(expr) {
                            let alias_name = format!("window_func_{}", window_functions.len());
                            window_functions.push((window_func.0, alias_name, window_func.1));
                        }
                    }
                    _ => {}
                }
            }
            
            if !window_functions.is_empty() {
                Some(window_functions)
            } else {
                None
            }
        } else {
            None
        }
    }

    // Parse individual window function expression
    fn parse_window_function(expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                match func_name.as_str() {
                    "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "LEAD" | "LAG" => {
                        // Check if it has an OVER clause
                        if let Some(over) = &func.over {
                            let over_clause = format!("{:?}", over);
                            Some((func_name, over_clause))
                        } else {
                            // For now, assume it's a window function even without OVER clause
                            // This is a fallback for cases where sqlparser doesn't recognize OVER
                            Some((func_name, "ORDER BY id".to_string()))
                        }
                    }
                    _ => None
                }
            }
            _ => None
        }
    }

    // Extract CASE expressions
    fn extract_case_expressions(query: &Query) -> Option<Vec<(String, String, String)>> {
        if let SetExpr::Select(select_box) = &*query.body {
            let mut case_expressions = Vec::new();
            
            for item in &select_box.projection {
                match item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let Some(case_expr) = SQLParser::parse_case_expression(expr) {
                            let alias_name = alias.value.clone();
                            case_expressions.push((case_expr.0, alias_name.clone(), case_expr.1));
                            println!("🔍 DEBUG CASE: Found CASE expression with alias '{}'", alias_name);
                        }
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        if let Some(case_expr) = SQLParser::parse_case_expression(expr) {
                            let alias_name = format!("case_expr_{}", case_expressions.len());
                            case_expressions.push((case_expr.0, alias_name, case_expr.1));
                            println!("🔍 DEBUG CASE: Found unnamed CASE expression");
                        }
                    }
                    _ => {}
                }
            }
            
            if !case_expressions.is_empty() {
                println!("🔍 DEBUG CASE: Extracted {} CASE expressions", case_expressions.len());
                Some(case_expressions)
            } else {
                None
            }
        } else {
            None
        }
    }

    // Parse individual CASE expression
    fn parse_case_expression(expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::Case { operand, conditions, results, else_result } => {
                let mut case_logic = String::new();
                
                // Handle operand (if any)
                if let Some(operand) = operand {
                    case_logic.push_str(&format!("CASE {} ", operand));
                } else {
                    case_logic.push_str("CASE ");
                }
                
                // Handle WHEN conditions and results
                for (i, (condition, result)) in conditions.iter().zip(results.iter()).enumerate() {
                    if i > 0 {
                        case_logic.push_str(" ");
                    }
                    case_logic.push_str(&format!("WHEN {} THEN {}", condition, result));
                }
                
                // Handle ELSE clause
                if let Some(else_val) = else_result {
                    case_logic.push_str(&format!(" ELSE {}", else_val));
                }
                
                case_logic.push_str(" END");
                
                let when_clauses = conditions.iter()
                    .map(|cond| cond.to_string())
                    .collect::<Vec<_>>()
                    .join(";");
                
                println!("🔍 DEBUG CASE: Parsed CASE expression: '{}'", case_logic);
                Some((case_logic, when_clauses))
            }
            _ => None
        }
    }

    // ✅ Parse CREATE TABLE
    fn parse_create_table(name: &ObjectName, columns: &[ColumnDef]) -> Result<ParsedQuery, String> {
        let table_name = name.to_string();
        let mut column_names = Vec::new();
        let mut schema_columns = Vec::new();
        
        for col in columns {
            let col_name = col.name.to_string();
            column_names.push(col_name.clone());
            
            let data_type = match &col.data_type {
                SqlDataType::Int(_) => DataType::Integer,
                SqlDataType::Text => DataType::Text,
                SqlDataType::Varchar(size_option) => {
                    // ✅ FIXED: Handle CharacterLength properly
                    let size = match size_option {
                        Some(length) => match length {
                            sqlparser::ast::CharacterLength::IntegerLength { length, .. } => *length as usize,
                            _ => 255,
                        },
                        None => 255,
                    };
                    DataType::VarChar(size)
                },
                SqlDataType::Real => DataType::Real,
                SqlDataType::Double => DataType::Double,
                SqlDataType::Boolean => DataType::Boolean,
                SqlDataType::Timestamp(_, _) => DataType::Timestamp,
                SqlDataType::Date => DataType::Date,
                _ => DataType::Text,
            };
            
            let mut constraints = Vec::new();
            let mut is_primary_key = false;
            
            for constraint in &col.options {
                match &constraint.option {
                    sqlparser::ast::ColumnOption::NotNull => constraints.push(Constraint::NotNull),
                    sqlparser::ast::ColumnOption::Unique { is_primary } => {
                        constraints.push(Constraint::Unique);
                        if *is_primary {
                            is_primary_key = true;
                            println!("DEBUG: Found PRIMARY KEY constraint for {}", col_name);
                        }
                    },
                    sqlparser::ast::ColumnOption::Default(expr) => {
                        constraints.push(Constraint::Default(expr.to_string()));
                    }
                    sqlparser::ast::ColumnOption::Null => {}, // Allow NULL explicitly
                    sqlparser::ast::ColumnOption::ForeignKey { .. } => {}, // Handle foreign keys separately
                    _ => {
                        // Fallback: Check debug string for any unknown PRIMARY KEY variants
                        let constraint_str = format!("{:?}", constraint.option);
                        if constraint_str.contains("Primary") || constraint_str.contains("primary") || constraint_str.contains("PRIMARY") {
                            is_primary_key = true;
                            println!("DEBUG: Found PRIMARY KEY constraint via fallback for {}: {}", col_name, constraint_str);
                        } else {
                            println!("DEBUG: Unknown constraint option for {}: {}", col_name, constraint_str);
                        }
                    }
                }
            }
            
            // Add PRIMARY KEY constraint if detected
            if is_primary_key || col_name.to_uppercase() == "ID" {
                constraints.push(Constraint::PrimaryKey);
            }
            
            schema_columns.push(Column {
                name: col_name,
                data_type,
                constraints,
                default_value: None,
                is_nullable: !col.options.iter().any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull)),
            });
        }
        
        let schema = TableSchema {
            name: table_name.clone(),
            columns: schema_columns,
            indexes: vec![],
            foreign_keys: vec![],
            triggers: vec![],
            created_at: chrono::Utc::now(),
            version: 1,
        };
        
        Ok(ParsedQuery::CreateTable { 
            table: table_name, 
            columns: column_names,
            schema 
        })
    }

    // ✅ Parse INSERT - improved to handle VALUES without column names
    fn parse_insert(table_name: &ObjectName, columns: &[sqlparser::ast::Ident], source: &Query) -> Result<ParsedQuery, String> {
        let mut values = HashMap::new();
        
        if let SetExpr::Values(values_list) = source.body.as_ref() {
            if let Some(row) = values_list.rows.get(0) {
                for (i, value) in row.iter().enumerate() {
                    let column_name = if i < columns.len() {
                        // Explicit column names provided
                        columns[i].to_string()
                    } else if columns.is_empty() {
                        // No column names provided - use standard column names for common patterns
                        match i {
                            0 => "id".to_string(),          // First column typically ID
                            1 => "name".to_string(),        // Second column often name
                            2 => "email".to_string(),       // Third column often email 
                            3 => "data".to_string(),        // Fourth column often data
                            _ => format!("col_{}", i),      // Fallback for additional columns
                        }
                    } else {
                        format!("col_{}", i)
                    };
                    
                    let value_str = match value {
                        Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                        Expr::Value(Value::Number(n, _)) => n.clone(),
                        Expr::Value(Value::Boolean(b)) => b.to_string(),
                        Expr::Value(Value::Null) => "NULL".to_string(),
                        _ => value.to_string(),
                    };
                    
                    values.insert(column_name, value_str);
                }
            }
        }
        
        Ok(ParsedQuery::Insert { 
            table: table_name.to_string(), 
            values 
        })
    }

    // ✅ Parse UPDATE
    fn parse_update(table: &TableFactor, assignments: &[Assignment], selection: &Option<Expr>) -> Result<ParsedQuery, String> {
        let mut values_map = HashMap::new();
        
        for assign in assignments {
            let column_name = assign.id.iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(".");
            
            match &assign.value {
                Expr::Value(Value::SingleQuotedString(val)) => {
                    values_map.insert(column_name, val.clone());
                }
                Expr::Value(Value::Number(num_str, _)) => {
                    values_map.insert(column_name, num_str.clone());
                }
                Expr::Value(Value::Boolean(bool_val)) => {
                    values_map.insert(column_name, bool_val.to_string());
                }
                Expr::Value(Value::Null) => {
                    values_map.insert(column_name, "NULL".to_string());
                }
                _ => {
                    values_map.insert(column_name, assign.value.to_string());
                }
            }
        }

        let conditions = selection.as_ref().map(|expr| expr.to_string());

        Ok(ParsedQuery::Update { 
            table: table.to_string(), 
            values: values_map, 
            conditions 
        })
    }

    // ✅ Parse DELETE
    fn parse_delete(table: &TableFactor, selection: &Option<Expr>) -> Result<ParsedQuery, String> {
        let conditions = selection.as_ref().map(|expr| expr.to_string());
        Ok(ParsedQuery::Delete { 
            table: table.to_string(), 
            conditions 
        })
    }

    // ✅ LEGACY: Extract conditions as HashMap (for backward compatibility)
    #[allow(dead_code)]
    fn extract_conditions(query: &Query) -> HashMap<String, String> {
        let mut conditions = HashMap::new();
        if let SetExpr::Select(select) = query.body.as_ref() {
            if let Some(selection) = &select.selection {
                SQLParser::extract_conditions_from_expr(selection, &mut conditions);
            }
        }
        conditions
    }

    #[allow(dead_code)]
    fn extract_conditions_from_expr(expr: &Expr, conditions: &mut HashMap<String, String>) {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Identifier(id), Expr::Value(Value::SingleQuotedString(val))) => {
                        conditions.insert(id.to_string(), val.clone());
                    },
                    (Expr::Identifier(id), Expr::Value(Value::Number(num_str, _))) => {
                        conditions.insert(id.to_string(), num_str.clone());
                    },
                    (Expr::CompoundIdentifier(parts), Expr::Value(Value::SingleQuotedString(val))) => {
                        if parts.len() == 2 {
                            let column_name = format!("{}.{}", parts[0], parts[1]);
                            conditions.insert(column_name, val.clone());
                        }
                    },
                    (Expr::CompoundIdentifier(parts), Expr::Value(Value::Number(num_str, _))) => {
                        if parts.len() == 2 {
                            let column_name = format!("{}.{}", parts[0], parts[1]);
                            conditions.insert(column_name, num_str.clone());
                        }
                    },
                    _ => {
                        // Recursively handle complex expressions
                        SQLParser::extract_conditions_from_expr(left, conditions);
                        SQLParser::extract_conditions_from_expr(right, conditions);
                    }
                }
            }
            _ => {}
        }
    }

    /// Parse and validate SQL syntax without execution
    pub fn validate_sql(query: &str) -> Result<(), String> {
        let _parsed = Self::parse_sql(query)?;
        Ok(())
    }

    /// Get table names from a query (useful for dependency analysis)
    pub fn extract_table_names(query: &str) -> Result<Vec<String>, String> {
        let parsed = Self::parse_sql(query)?;
        match parsed {
            ParsedQuery::Select { table, joins, .. } => {
                let mut tables = vec![table];
                for (join_table, _, _) in joins {
                    tables.push(join_table);
                }
                Ok(tables)
            }
            ParsedQuery::Insert { table, .. } |
            ParsedQuery::Update { table, .. } |
            ParsedQuery::Delete { table, .. } |
            ParsedQuery::CreateTable { table, .. } |
            ParsedQuery::DropTable { table } => Ok(vec![table]),
            _ => Ok(vec![]),
        }
    }

    /// Check if query is read-only (SELECT)
    pub fn is_read_only(query: &str) -> Result<bool, String> {
        let parsed = Self::parse_sql(query)?;
        Ok(matches!(parsed, ParsedQuery::Select { .. }))
    }

    /// Check if query modifies schema (DDL)
    pub fn is_ddl(query: &str) -> Result<bool, String> {
        let parsed = Self::parse_sql(query)?;
        Ok(matches!(parsed, ParsedQuery::CreateTable { .. } | ParsedQuery::DropTable { .. } | ParsedQuery::CreateDatabase { .. } | ParsedQuery::DropDatabase { .. }))
    }

    // 🆕 DATABASE MANAGEMENT COMMANDS PARSING
    
    /// Parse CREATE DATABASE command
    /// Syntax: CREATE DATABASE database_name [DESCRIPTION 'optional description']
    fn parse_create_database(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 3 {
            return Err("Invalid CREATE DATABASE syntax. Use: CREATE DATABASE database_name".to_string());
        }
        
        // Handle "CREATE DATABASE [IF NOT EXISTS] database_name"
        let mut db_name_index = 2;
        let mut if_not_exists = false;
        
        // Check for IF NOT EXISTS
        if parts.len() > 4 && parts[2].to_uppercase() == "IF" && 
           parts[3].to_uppercase() == "NOT" && parts[4].to_uppercase() == "EXISTS" {
            db_name_index = 5;
            if_not_exists = true;
        }
        
        if parts.len() <= db_name_index {
            return Err("Missing database name in CREATE DATABASE statement".to_string());
        }
        
        let database_name = parts[db_name_index].to_string();
        
        // Validate database name
        if !Self::is_valid_database_name(&database_name) {
            return Err("Invalid database name. Use alphanumeric characters and underscores only.".to_string());
        }
        
        // Check for optional description
        let mut description = None;
        let desc_index = db_name_index + 1;
        if parts.len() > desc_index && parts[desc_index].to_uppercase() == "DESCRIPTION" {
            if parts.len() > desc_index + 1 {
                // Join remaining parts as description, removing quotes
                let desc = parts[(desc_index + 1)..].join(" ");
                description = Some(desc.trim_matches('\'').trim_matches('"').to_string());
            }
        }
        
        Ok(ParsedQuery::CreateDatabase {
            name: database_name,
            description,
            if_not_exists,
        })
    }
    
    /// Parse CREATE INDEX command
    /// Syntax: CREATE [UNIQUE] INDEX index_name ON table_name (column1, column2, ...)
    fn parse_create_index(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 6 {
            return Err("Invalid CREATE INDEX syntax. Use: CREATE [UNIQUE] INDEX index_name ON table_name (columns)".to_string());
        }
        
        let mut pos = 1; // Skip "CREATE"
        let unique = if parts[pos].to_uppercase() == "UNIQUE" {
            pos += 1; // Skip "UNIQUE"
            true
        } else {
            false
        };
        
        if parts[pos].to_uppercase() != "INDEX" {
            return Err("Expected INDEX keyword".to_string());
        }
        pos += 1; // Skip "INDEX"
        
        let index_name = parts[pos].to_string();
        pos += 1;
        
        if parts[pos].to_uppercase() != "ON" {
            return Err("Expected ON keyword".to_string());
        }
        pos += 1; // Skip "ON"
        
        let table_name = parts[pos].to_string();
        pos += 1;
        
        // Parse columns from remaining parts (should be in parentheses)
        let remaining = parts[pos..].join(" ");
        if !remaining.starts_with('(') || !remaining.ends_with(')') {
            return Err("Columns must be specified in parentheses".to_string());
        }
        
        let columns_str = remaining.trim_start_matches('(').trim_end_matches(')');
        let columns: Vec<String> = columns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        
        if columns.is_empty() {
            return Err("At least one column must be specified".to_string());
        }
        
        Ok(ParsedQuery::CreateIndex {
            name: index_name,
            table: table_name,
            columns,
            unique,
        })
    }
    
    /// Parse USE DATABASE command
    /// Syntax: USE DATABASE database_name OR USE database_name
    fn parse_use_database(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        let database_name = if parts.len() >= 3 && parts[1].to_uppercase() == "DATABASE" {
            // USE DATABASE database_name
            parts[2]
        } else if parts.len() >= 2 {
            // USE database_name
            parts[1]
        } else {
            return Err("Invalid USE DATABASE syntax. Use: USE DATABASE database_name or USE database_name".to_string());
        };
        
        if !Self::is_valid_database_name(database_name) {
            return Err("Invalid database name. Use alphanumeric characters and underscores only.".to_string());
        }
        
        Ok(ParsedQuery::UseDatabase {
            name: database_name.to_string(),
        })
    }
    
    /// Parse DESCRIBE TABLE command
    /// Syntax: DESCRIBE table_name or DESC table_name
    fn parse_describe_table(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 2 {
            return Err("Invalid DESCRIBE syntax. Use: DESCRIBE table_name".to_string());
        }
        
        let table_name = parts[1].to_string();
        
        if table_name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::DescribeTable {
            table: table_name,
        })
    }
    
    /// Parse DROP DATABASE command
    /// Syntax: DROP DATABASE database_name
    fn parse_drop_database(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 3 {
            return Err("Invalid DROP DATABASE syntax. Use: DROP DATABASE database_name".to_string());
        }
        
        let database_name = parts[2].to_string();
        
        if !Self::is_valid_database_name(&database_name) {
            return Err("Invalid database name. Use alphanumeric characters and underscores only.".to_string());
        }
        
        Ok(ParsedQuery::DropDatabase {
            name: database_name,
        })
    }
    
    /// Parse SUBSCRIBE command
    /// Syntax: SUBSCRIBE table_name
    fn parse_subscribe(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 2 {
            return Err("Invalid SUBSCRIBE syntax. Use: SUBSCRIBE table_name".to_string());
        }
        
        let table_name = parts[1].to_string();
        
        if table_name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::Subscribe {
            table: table_name,
        })
    }
    
    /// Parse UNSUBSCRIBE command
    /// Syntax: UNSUBSCRIBE table_name
    fn parse_unsubscribe(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 2 {
            return Err("Invalid UNSUBSCRIBE syntax. Use: UNSUBSCRIBE table_name".to_string());
        }
        
        let table_name = parts[1].to_string();
        
        if table_name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::Unsubscribe {
            table: table_name,
        })
    }
    
    /// Parse AUTH command
    /// Syntax: AUTH credentials
    fn parse_auth(query: &str) -> Result<ParsedQuery, String> {
        let parts: Vec<&str> = query.trim().split_whitespace().collect();
        
        if parts.len() < 2 {
            return Err("Invalid AUTH syntax. Use: AUTH credentials".to_string());
        }
        
        let credentials = parts[1].to_string();
        
        if credentials.is_empty() {
            return Err("Credentials cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::Auth {
            credentials,
        })
    }
    
    /// Parse LOAD MODULE command
    /// Syntax: LOAD MODULE 'module_name' FROM 'file_path'
    fn parse_load_module(query: &str) -> Result<ParsedQuery, String> {
        // Remove LOAD MODULE prefix and trim
        let remaining = query.trim().strip_prefix("LOAD MODULE").unwrap().trim();
        
        // Split by FROM keyword
        let parts: Vec<&str> = remaining.split(" FROM ").collect();
        
        if parts.len() != 2 {
            return Err("Invalid LOAD MODULE syntax. Use: LOAD MODULE 'module_name' FROM 'file_path'".to_string());
        }
        
        // Extract module name (remove quotes if present)
        let module_name = parts[0].trim().trim_matches('\'').trim_matches('"').to_string();
        
        // Extract file path (remove quotes if present)
        let file_path = parts[1].trim().trim_matches('\'').trim_matches('"').to_string();
        
        if module_name.is_empty() {
            return Err("Module name cannot be empty".to_string());
        }
        
        if file_path.is_empty() {
            return Err("File path cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::LoadModule {
            module_name,
            file_path,
        })
    }
    
    /// Parse WASM_EXEC command
    /// Syntax: SELECT WASM_EXEC('module_name', 'function_name', arg1, arg2, ...)
    fn parse_wasm_exec(query: &str) -> Result<ParsedQuery, String> {
        // Find the WASM_EXEC function call
        let start_idx = query.find("WASM_EXEC(")
            .ok_or("WASM_EXEC function not found")?;
        
        let func_start = start_idx + "WASM_EXEC(".len();
        let func_end = query[func_start..].find(')')
            .ok_or("Missing closing parenthesis for WASM_EXEC")?;
        
        let args_str = &query[func_start..func_start + func_end];
        
        // Split arguments by comma, handling quoted strings
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut in_quotes = false;
        let mut quote_char = ' ';
        
        for ch in args_str.chars() {
            if (ch == '\'' || ch == '"') && !in_quotes {
                in_quotes = true;
                quote_char = ch;
            } else if ch == quote_char && in_quotes {
                in_quotes = false;
                quote_char = ' ';
            } else if ch == ',' && !in_quotes {
                args.push(current_arg.trim().trim_matches('\'').trim_matches('"').to_string());
                current_arg.clear();
            } else if ch != ' ' || in_quotes {
                current_arg.push(ch);
            }
        }
        
        if !current_arg.trim().is_empty() {
            args.push(current_arg.trim().trim_matches('\'').trim_matches('"').to_string());
        }
        
        if args.len() < 2 {
            return Err("WASM_EXEC requires at least module_name and function_name".to_string());
        }
        
        let module_name = args[0].clone();
        let function_name = args[1].clone();
        let function_args = args[2..].to_vec();
        
        if module_name.is_empty() {
            return Err("Module name cannot be empty".to_string());
        }
        
        if function_name.is_empty() {
            return Err("Function name cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::WasmExec {
            module_name,
            function_name,
            args: function_args,
        })
    }
    
    /// Parse WASM_EXEC command (direct syntax)
    /// Syntax: WASM_EXEC('module_name', 'function_name', arg1, arg2, ...)
    fn parse_wasm_exec_direct(query: &str) -> Result<ParsedQuery, String> {
        // Remove WASM_EXEC prefix and trim
        let remaining = query.trim().strip_prefix("WASM_EXEC").unwrap().trim();
        
        // Parse the arguments in parentheses
        if !remaining.starts_with('(') || !remaining.ends_with(')') {
            return Err("WASM_EXEC requires parentheses: WASM_EXEC('module', 'function', args...)".to_string());
        }
        
        let args_str = &remaining[1..remaining.len()-1]; // Remove parentheses
        
        // Split arguments by comma, handling quoted strings
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut in_quotes = false;
        let mut quote_char = ' ';
        
        for ch in args_str.chars() {
            if (ch == '\'' || ch == '"') && !in_quotes {
                in_quotes = true;
                quote_char = ch;
            } else if ch == quote_char && in_quotes {
                in_quotes = false;
                quote_char = ' ';
            } else if ch == ',' && !in_quotes {
                args.push(current_arg.trim().trim_matches('\'').trim_matches('"').to_string());
                current_arg.clear();
            } else if ch != ' ' || in_quotes {
                current_arg.push(ch);
            }
        }
        
        if !current_arg.trim().is_empty() {
            args.push(current_arg.trim().trim_matches('\'').trim_matches('"').to_string());
        }
        
        if args.len() < 2 {
            return Err("WASM_EXEC requires at least module_name and function_name".to_string());
        }
        
        let module_name = args[0].clone();
        let function_name = args[1].clone();
        let function_args = args[2..].to_vec();
        
        if module_name.is_empty() {
            return Err("Module name cannot be empty".to_string());
        }
        
        if function_name.is_empty() {
            return Err("Function name cannot be empty".to_string());
        }
        
        Ok(ParsedQuery::WasmExec {
            module_name,
            function_name,
            args: function_args,
        })
    }
    
    /// Validate database name
    fn is_valid_database_name(name: &str) -> bool {
        !name.is_empty() 
            && name.len() <= 64 
            && name.chars().all(|c| c.is_alphanumeric() || c == '_')
            && !name.starts_with(|c: char| c.is_numeric())
    }
}

// ✅ CONVENIENCE: Public function for external use
pub fn parse_sql(query: &str) -> Result<ParsedQuery, String> {
    SQLParser::parse_sql(query)
}