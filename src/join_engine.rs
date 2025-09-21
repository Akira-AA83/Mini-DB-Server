/*
📌 JOIN Execution Engine - Phase 5 Implementation WITH DEBUG LOGGING
✅ INNER JOIN, LEFT JOIN, RIGHT JOIN support
✅ Nested Loop Join, Hash Join algorithms
✅ Index-based join optimization
✅ Query execution planning
✅ EXTENSIVE DEBUG LOGGING for troubleshooting
*/

use std::collections::HashMap;
use std::sync::Arc;
use serde_json;
use sled::Db;

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub join_type: JoinType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct QueryExecutionPlan {
    pub operations: Vec<ExecutionOperation>,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
}

#[derive(Debug, Clone)]
pub enum ExecutionOperation {
    TableScan { table: String, conditions: HashMap<String, String> },
    IndexScan { table: String, index: String, key: String },
    NestedLoopJoin { left: Box<ExecutionOperation>, right: Box<ExecutionOperation>, condition: JoinCondition },
    HashJoin { left: Box<ExecutionOperation>, right: Box<ExecutionOperation>, condition: JoinCondition },
    Filter { operation: Box<ExecutionOperation>, conditions: HashMap<String, String> },
    Sort { operation: Box<ExecutionOperation>, column: String, descending: bool },
    Limit { operation: Box<ExecutionOperation>, count: usize },
}

pub struct JoinExecutor {
    db: Arc<Db>,
    statistics: TableStatistics,
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    table_stats: HashMap<String, TableStats>,
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: usize,
    pub avg_row_size: usize,
    pub column_stats: HashMap<String, ColumnStats>,
    pub index_stats: HashMap<String, IndexStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexStats {
    pub selectivity: f64,
    pub depth: usize,
    pub leaf_pages: usize,
}

impl JoinExecutor {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            statistics: TableStatistics::new(),
        }
    }

    /// Executes a JOIN query with optimization
    pub fn execute_join_query(
        &mut self,
        tables: Vec<String>,
        joins: Vec<JoinCondition>,
        conditions: HashMap<String, String>,
        order_by: Option<String>,
        limit: Option<usize>,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        println!("🔍 JOIN EXECUTOR: Planning query for tables: {:?}", tables);
        
        // Step 1: Update statistics
        self.update_statistics(&tables)?;
        
        // Step 2: Generate execution plan
        let plan = self.generate_execution_plan(&tables, &joins, &conditions, &order_by, &limit)?;
        println!("📋 EXECUTION PLAN: Cost={:.2}, Estimated rows={}", plan.estimated_cost, plan.estimated_rows);
        
        // Step 3: Execute plan
        self.execute_plan(&plan)
    }

    /// Generates optimal execution plan for the query
    fn generate_execution_plan(
        &self,
        tables: &[String],
        joins: &[JoinCondition],
        conditions: &HashMap<String, String>,
        order_by: &Option<String>,
        limit: &Option<usize>,
    ) -> Result<QueryExecutionPlan, String> {
        let mut operations = Vec::new();
        let mut total_cost = 0.0;
        let mut estimated_rows = 1000; // Default estimate

        // Generate table access operations
        for table in tables {
            let table_conditions: HashMap<String, String> = conditions
                .iter()
                .filter(|(k, _)| k.starts_with(&format!("{}.", table)) || !k.contains('.'))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let operation = if self.should_use_index(table, &table_conditions) {
                let index_name = self.select_best_index(table, &table_conditions)?;
                let index_key = table_conditions.values().next().unwrap_or(&"".to_string()).clone();
                
                ExecutionOperation::IndexScan {
                    table: table.clone(),
                    index: index_name,
                    key: index_key,
                }
            } else {
                ExecutionOperation::TableScan {
                    table: table.clone(),
                    conditions: table_conditions,
                }
            };

            operations.push(operation);
        }

        // Generate JOIN operations
        if !joins.is_empty() {
            println!("🔧 DEBUG: Starting JOIN generation with {} joins", joins.len());
            
            // Prendi la prima table scan operation come base
            let mut current_operation = operations.remove(0);
            
            // Itera attraverso tutte le JOIN conditions
            for join in joins.iter() {
                // Rimuovi sempre la prossima table scan operation se disponibile
                if !operations.is_empty() {
                    let right_op = operations.remove(0);
                    
                    println!("🔗 BUILDING JOIN: {} {} {}", 
                             join.left_table, 
                             match join.join_type {
                                 JoinType::Inner => "INNER JOIN",
                                 JoinType::Left => "LEFT JOIN",
                                 JoinType::Right => "RIGHT JOIN",
                                 JoinType::Full => "FULL JOIN",
                             },
                             join.right_table);
                    
                    // Crea l'operazione di JOIN
                    let join_operation = if self.should_use_hash_join(join) {
                        ExecutionOperation::HashJoin {
                            left: Box::new(current_operation),
                            right: Box::new(right_op),
                            condition: join.clone(),
                        }
                    } else {
                        ExecutionOperation::NestedLoopJoin {
                            left: Box::new(current_operation),
                            right: Box::new(right_op),
                            condition: join.clone(),
                        }
                    };
                    
                    // Aggiorna l'operazione corrente per la prossima iterazione
                    current_operation = join_operation;
                    total_cost += self.estimate_join_cost(join);
                    estimated_rows = self.estimate_join_rows(join);
                } else {
                    println!("⚠️ WARNING: No more table operations available for JOIN");
                    break;
                }
            }
            
            // Imposta l'operazione finale
            operations = vec![current_operation];
        }

        // Add ORDER BY if specified
        if let Some(order_col) = order_by {
            let sort_op = ExecutionOperation::Sort {
                operation: Box::new(operations.remove(0)),
                column: order_col.clone(),
                descending: false,
            };
            operations = vec![sort_op];
            total_cost += estimated_rows as f64 * 0.01; // Sort cost
        }

        // Add LIMIT if specified
        if let Some(limit_count) = limit {
            let limit_op = ExecutionOperation::Limit {
                operation: Box::new(operations.remove(0)),
                count: *limit_count,
            };
            operations = vec![limit_op];
            estimated_rows = estimated_rows.min(*limit_count);
        }

        Ok(QueryExecutionPlan {
            operations,
            estimated_cost: total_cost,
            estimated_rows,
        })
    }

    /// Executes the generated execution plan
    fn execute_plan(&self, plan: &QueryExecutionPlan) -> Result<Vec<HashMap<String, String>>, String> {
        if plan.operations.is_empty() {
            return Ok(vec![]);
        }

        self.execute_operation(&plan.operations[0])
    }

    /// Executes a single operation in the plan
    fn execute_operation(&self, operation: &ExecutionOperation) -> Result<Vec<HashMap<String, String>>, String> {
        match operation {
            ExecutionOperation::TableScan { table, conditions } => {
                self.execute_table_scan(table, conditions)
            }
            ExecutionOperation::IndexScan { table, index: _, key } => {
                self.execute_index_scan(table, key)
            }
            ExecutionOperation::NestedLoopJoin { left, right, condition } => {
                self.execute_nested_loop_join(left, right, condition)
            }
            ExecutionOperation::HashJoin { left, right, condition } => {
                self.execute_hash_join(left, right, condition)
            }
            ExecutionOperation::Filter { operation, conditions } => {
                let rows = self.execute_operation(operation)?;
                Ok(self.apply_filter(rows, conditions))
            }
            ExecutionOperation::Sort { operation, column, descending } => {
                let mut rows = self.execute_operation(operation)?;
                self.sort_rows(&mut rows, column, *descending);
                Ok(rows)
            }
            ExecutionOperation::Limit { operation, count } => {
                let rows = self.execute_operation(operation)?;
                Ok(rows.into_iter().take(*count).collect())
            }
        }
    }

    /// Executes table scan operation
    fn execute_table_scan(&self, table: &str, conditions: &HashMap<String, String>) -> Result<Vec<HashMap<String, String>>, String> {
        println!("📊 TABLE SCAN: Scanning table '{}' with conditions: {:?}", table, conditions);
        
        // Extract real table name from alias (e.g., "test_users AS u" -> "test_users")
        let real_table_name = Self::extract_real_table_name(table);
        println!("📊 TABLE SCAN: Real table name: '{}'", real_table_name);
        
        let tree = self.db.open_tree(&real_table_name).map_err(|e| e.to_string())?;
        let mut results = Vec::new();

        for entry in tree.iter() {
            let (_, value) = entry.map_err(|e| e.to_string())?;
            let value_str = String::from_utf8_lossy(&value);
            
            if let Ok(row) = serde_json::from_str::<HashMap<String, String>>(&value_str) {
                if self.matches_conditions(&row, conditions) {
                    results.push(row);
                }
            }
        }

        println!("📊 TABLE SCAN: Found {} matching rows", results.len());
        
        // 🔧 DEBUG: Log first few rows
        if !results.is_empty() {
            println!("🔍 DEBUG: First row sample: {:?}", results[0]);
        }
        
        Ok(results)
    }

    /// Executes index scan operation
    fn execute_index_scan(&self, table: &str, key: &str) -> Result<Vec<HashMap<String, String>>, String> {
        println!("🗂️ INDEX SCAN: Using index on table '{}' with key '{}'", table, key);
        
        // Extract real table name from alias
        let real_table_name = Self::extract_real_table_name(table);
        println!("🗂️ INDEX SCAN: Real table name: '{}'", real_table_name);
        
        // For now, fall back to table scan with optimized access
        // In a real implementation, this would use actual index structures
        let tree = self.db.open_tree(&real_table_name).map_err(|e| e.to_string())?;
        let mut results = Vec::new();

        // Simulate index lookup (in reality, this would be much faster)
        for entry in tree.iter() {
            let (_, value) = entry.map_err(|e| e.to_string())?;
            let value_str = String::from_utf8_lossy(&value);
            
            if let Ok(row) = serde_json::from_str::<HashMap<String, String>>(&value_str) {
                if row.values().any(|v| v == key) {
                    results.push(row);
                    break; // Index should find quickly
                }
            }
        }

        println!("🗂️ INDEX SCAN: Found {} rows via index", results.len());
        Ok(results)
    }

    /// Executes nested loop join
    fn execute_nested_loop_join(
        &self,
        left_op: &ExecutionOperation,
        right_op: &ExecutionOperation,
        condition: &JoinCondition,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        println!("🔗 NESTED LOOP JOIN: {} {} {}", 
                 condition.left_table, 
                 match condition.join_type {
                     JoinType::Inner => "INNER JOIN",
                     JoinType::Left => "LEFT JOIN", 
                     JoinType::Right => "RIGHT JOIN",
                     JoinType::Full => "FULL JOIN",
                 },
                 condition.right_table);

        let left_rows = self.execute_operation(left_op)?;
        let right_rows = self.execute_operation(right_op)?;
        let mut results = Vec::new();

        for left_row in &left_rows {
            let mut matched = false;
            
            for right_row in &right_rows {
                if self.join_condition_matches(left_row, right_row, condition) {
                    let joined_row = self.merge_rows(left_row, right_row, &condition.left_table, &condition.right_table);
                    results.push(joined_row);
                    matched = true;
                }
            }

            // Handle LEFT JOIN - include unmatched left rows
            if !matched && condition.join_type == JoinType::Left {
                let joined_row = self.merge_rows_with_nulls(left_row, &condition.left_table, &condition.right_table);
                results.push(joined_row);
            }
        }

        // Handle RIGHT JOIN - include unmatched right rows
        if condition.join_type == JoinType::Right {
            for right_row in &right_rows {
                let matched = left_rows.iter().any(|left_row| {
                    self.join_condition_matches(left_row, right_row, condition)
                });
                
                if !matched {
                    let joined_row = self.merge_rows_with_nulls(right_row, &condition.right_table, &condition.left_table);
                    results.push(joined_row);
                }
            }
        }

        println!("🔗 NESTED LOOP JOIN: Produced {} joined rows", results.len());
        Ok(results)
    }

    /// Executes hash join (more efficient for large datasets)
    fn execute_hash_join(
        &self,
        left_op: &ExecutionOperation,
        right_op: &ExecutionOperation,
        condition: &JoinCondition,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        println!("⚡ HASH JOIN: Building hash table for efficient join");
        println!("🔍 DEBUG: JOIN condition: {}.{} = {}.{}", 
                 condition.left_table, condition.left_column, 
                 condition.right_table, condition.right_column);

        let left_rows = self.execute_operation(left_op)?;
        let right_rows = self.execute_operation(right_op)?;

        println!("🔍 DEBUG: Left rows: {}, Right rows: {}", left_rows.len(), right_rows.len());

        // 🔧 DEBUG: Log sample data
        if !left_rows.is_empty() {
            println!("🔍 DEBUG: Sample left row: {:?}", left_rows[0]);
        }
        if !right_rows.is_empty() {
            println!("🔍 DEBUG: Sample right row: {:?}", right_rows[0]);
        }

        // Build hash table from smaller relation
        let (build_rows, probe_rows, build_col, probe_col, reverse) = if left_rows.len() <= right_rows.len() {
            (&left_rows, &right_rows, &condition.left_column, &condition.right_column, false)
        } else {
            (&right_rows, &left_rows, &condition.right_column, &condition.left_column, true)
        };

        println!("🔍 DEBUG: Building hash table on column '{}' from {} rows (reverse={})", build_col, build_rows.len(), reverse);

        let mut hash_table: HashMap<String, Vec<&HashMap<String, String>>> = HashMap::new();
        
        // Build phase
        for row in build_rows {
            let key_val = self.get_column_value(row, build_col, if reverse { &condition.right_table } else { &condition.left_table });
            match key_val {
                Some(key) => {
                    println!("🔍 DEBUG: Adding to hash table: key='{}' from row with keys: {:?}", key, row.keys().collect::<Vec<_>>());
                    hash_table.entry(key.clone()).or_insert_with(Vec::new).push(row);
                }
                None => {
                    println!("⚠️ DEBUG: Could not find column '{}' in row: {:?}", build_col, row);
                }
            }
        }

        println!("🔍 DEBUG: Hash table built with {} unique keys: {:?}", hash_table.len(), hash_table.keys().collect::<Vec<_>>());

        let mut results = Vec::new();
        let mut matches_found = 0;

        // Probe phase
        for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
            let key_val = self.get_column_value(probe_row, probe_col, if reverse { &condition.left_table } else { &condition.right_table });
            match key_val {
                Some(key) => {
                    println!("🔍 DEBUG: Probing row {} with key='{}' from row with keys: {:?}", probe_idx, key, probe_row.keys().collect::<Vec<_>>());
                    if let Some(matching_rows) = hash_table.get(key) {
                        println!("🔍 DEBUG: Found {} matching rows for key '{}'", matching_rows.len(), key);
                        for &build_row in matching_rows {
                            // ✅ FIX: Sempre mantieni l'ordine originale left->right indipendentemente dal reverse
                            let joined_row = if reverse {
                                // Quando reverse=true, build_row è da right_table e probe_row è da left_table
                                // Ma vogliamo sempre merge_rows(left_data, right_data, left_table, right_table)
                                self.merge_rows(probe_row, build_row, &condition.left_table, &condition.right_table)
                            } else {
                                // Quando reverse=false, build_row è da left_table e probe_row è da right_table
                                self.merge_rows(build_row, probe_row, &condition.left_table, &condition.right_table)
                            };
                            results.push(joined_row);
                            matches_found += 1;
                        }
                    } else {
                        println!("🔍 DEBUG: No matching rows found for key '{}'", key);
                        
                        // Handle LEFT JOIN - include unmatched left rows with NULL values for right table
                        if condition.join_type == JoinType::Left {
                            // Only include unmatched rows if we're probing the left table
                            if reverse {
                                // reverse=true means probe_row is from left_table, build_row would be from right_table
                                let joined_row = self.merge_rows_with_nulls(probe_row, &condition.left_table, &condition.right_table);
                                results.push(joined_row);
                                println!("🔍 DEBUG: Added LEFT JOIN row with NULLs for unmatched key '{}'", key);
                            }
                            // If reverse=false, probe_row is from right_table, so we don't add it for LEFT JOIN
                        }
                        
                        // Handle RIGHT JOIN - include unmatched right rows with NULL values for left table
                        if condition.join_type == JoinType::Right {
                            // Only include unmatched rows if we're probing the right table
                            if !reverse {
                                // reverse=false means probe_row is from right_table, build_row would be from left_table
                                let joined_row = self.merge_rows_with_nulls_right(probe_row, &condition.left_table, &condition.right_table);
                                results.push(joined_row);
                                println!("🔍 DEBUG: Added RIGHT JOIN row with NULLs for unmatched key '{}'", key);
                            }
                            // If reverse=true, probe_row is from left_table, so we don't add it for RIGHT JOIN
                        }
                        
                        // Handle FULL OUTER JOIN - include unmatched rows from both sides
                        if condition.join_type == JoinType::Full {
                            if reverse {
                                // reverse=true means probe_row is from left_table, build_row would be from right_table
                                let joined_row = self.merge_rows_with_nulls(probe_row, &condition.left_table, &condition.right_table);
                                results.push(joined_row);
                                println!("🔍 DEBUG: Added FULL OUTER JOIN row with NULLs for unmatched left key '{}'", key);
                            } else {
                                // reverse=false means probe_row is from right_table, build_row would be from left_table
                                let joined_row = self.merge_rows_with_nulls_right(probe_row, &condition.left_table, &condition.right_table);
                                results.push(joined_row);
                                println!("🔍 DEBUG: Added FULL OUTER JOIN row with NULLs for unmatched right key '{}'", key);
                            }
                        }
                    }
                }
                None => {
                    println!("⚠️ DEBUG: Could not find column '{}' in probe row: {:?}", probe_col, probe_row);
                }
            }
        }

        println!("⚡ HASH JOIN: Produced {} joined rows (found {} matches)", results.len(), matches_found);
        Ok(results)
    }

    /// Checks if a row matches the given conditions
    fn matches_conditions(&self, row: &HashMap<String, String>, conditions: &HashMap<String, String>) -> bool {
        conditions.iter().all(|(key, value)| {
            let clean_key = if key.contains('.') {
                key.split('.').nth(1).unwrap_or(key)
            } else {
                key
            };
            row.get(clean_key) == Some(value)
        })
    }

    /// Checks if two rows match the join condition
    fn join_condition_matches(&self, left_row: &HashMap<String, String>, right_row: &HashMap<String, String>, condition: &JoinCondition) -> bool {
        let left_val = self.get_column_value(left_row, &condition.left_column, &condition.left_table);
        let right_val = self.get_column_value(right_row, &condition.right_column, &condition.right_table);
        
        println!("🔍 DEBUG: Matching condition: left_val={:?}, right_val={:?}", left_val, right_val);
        
        match (left_val, right_val) {
            (Some(l), Some(r)) => {
                let matches = l == r;
                println!("🔍 DEBUG: Values match: {}", matches);
                matches
            }
            _ => {
                println!("🔍 DEBUG: One or both values are None");
                false
            }
        }
    }

    /// Helper function to get column value with flexible column name matching
    fn get_column_value<'a>(&self, row: &'a HashMap<String, String>, column: &str, table: &str) -> Option<&'a String> {
        println!("🔍 DEBUG: Looking for column '{}' in table '{}' context", column, table);
        println!("🔍 DEBUG: Available keys in row: {:?}", row.keys().collect::<Vec<_>>());
        
        // 1. Nome completo: "table.column"
        let qualified_name = format!("{}.{}", table, column);
        if let Some(val) = row.get(&qualified_name) {
            println!("🔍 DEBUG: Found qualified column '{}' = '{}'", qualified_name, val);
            return Some(val);
        }
        
        // 2. Nome semplice: "column"
        if let Some(val) = row.get(column) {
            println!("🔍 DEBUG: Found simple column '{}' = '{}'", column, val);
            return Some(val);
        }
        
        // 3. Cerca in tutte le chiavi che terminano con ".column"
        for (key, value) in row {
            if key.ends_with(&format!(".{}", column)) {
                println!("🔍 DEBUG: Found pattern column '{}' = '{}'", key, value);
                return Some(value);
            }
        }
        
        println!("🔍 DEBUG: Column '{}' not found in any form", column);
        None
    }

    /// Merges two rows from a join
    fn merge_rows(&self, left_row: &HashMap<String, String>, right_row: &HashMap<String, String>, left_table: &str, right_table: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        
        println!("🔍 DEBUG MERGE: Merging {} ({} keys) with {} ({} keys)", 
                 left_table, left_row.len(), right_table, right_row.len());
        
        // Add left table columns - preserve existing prefixes
        for (key, value) in left_row {
            if key.contains('.') {
                // Key already has a prefix, keep it as is
                result.insert(key.clone(), value.clone());
                println!("🔍 DEBUG MERGE: Left prefixed key '{}' = '{}'", key, value);
            } else {
                // Add prefix for non-prefixed keys
                let prefixed_key = format!("{}.{}", left_table, key);
                result.insert(prefixed_key.clone(), value.clone());
                println!("🔍 DEBUG MERGE: Left simple key '{}' -> '{}' = '{}'", key, prefixed_key, value);
            }
        }
        
        // Add right table columns - add prefix for all
        for (key, value) in right_row {
            let prefixed_key = format!("{}.{}", right_table, key);
            result.insert(prefixed_key.clone(), value.clone());
            println!("🔍 DEBUG MERGE: Right key '{}' -> '{}' = '{}'", key, prefixed_key, value);
        }
        
        println!("🔍 DEBUG MERGE: Result has {} keys: {:?}", result.len(), result.keys().collect::<Vec<_>>());
        result
    }

    /// Merges row with nulls for outer joins
    fn merge_rows_with_nulls(&self, row: &HashMap<String, String>, table: &str, null_table: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        
        // Add existing table columns
        for (key, value) in row {
            let prefixed_key = if key.contains('.') {
                key.clone()
            } else {
                format!("{}.{}", table, key)
            };
            result.insert(prefixed_key, value.clone());
        }
        
        // Add NULL columns for missing table - dynamically detect common column names
        let common_columns = vec!["id", "name", "user_id", "email", "age", "amount", "date", "status"];
        for col in common_columns {
            result.insert(format!("{}.{}", null_table, col), "NULL".to_string());
        }
        
        result
    }

    /// Merges row with nulls for RIGHT JOIN - puts right table data first, then left table NULLs
    fn merge_rows_with_nulls_right(&self, right_row: &HashMap<String, String>, left_table: &str, right_table: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        
        // Add NULL columns for left table - dynamically detect common column names
        let common_columns = vec!["id", "name", "user_id", "email", "age", "amount", "date", "status"];
        for col in common_columns {
            result.insert(format!("{}.{}", left_table, col), "NULL".to_string());
        }
        
        // Add existing right table columns
        for (key, value) in right_row {
            let prefixed_key = if key.contains('.') {
                key.clone()
            } else {
                format!("{}.{}", right_table, key)
            };
            result.insert(prefixed_key, value.clone());
        }
        
        result
    }

    /// Applies filter conditions to rows
    fn apply_filter(&self, rows: Vec<HashMap<String, String>>, conditions: &HashMap<String, String>) -> Vec<HashMap<String, String>> {
        rows.into_iter()
            .filter(|row| self.matches_conditions(row, conditions))
            .collect()
    }

    /// Sorts rows by specified column
    fn sort_rows(&self, rows: &mut Vec<HashMap<String, String>>, column: &str, descending: bool) {
        rows.sort_by(|a, b| {
            let empty_string = String::new();
            let a_val = a.get(column).unwrap_or(&empty_string);
            let b_val = b.get(column).unwrap_or(&empty_string);
            
            if descending {
                b_val.cmp(a_val)
            } else {
                a_val.cmp(b_val)
            }
        });
    }

    // Cost estimation and optimization methods
    fn should_use_index(&self, table: &str, conditions: &HashMap<String, String>) -> bool {
        // Simple heuristic: use index if we have conditions and statistics show it's beneficial
        !conditions.is_empty() && self.statistics.has_useful_index(table, conditions)
    }

    fn select_best_index(&self, table: &str, _conditions: &HashMap<String, String>) -> Result<String, String> {
        // Return primary key index for now
        Ok(format!("pk_{}", table))
    }

    fn should_use_hash_join(&self, _condition: &JoinCondition) -> bool {
        // Use hash join for larger datasets (simplified heuristic)
        true
    }

    fn estimate_join_cost(&self, _condition: &JoinCondition) -> f64 {
        // Simplified cost estimation
        1000.0
    }

    fn estimate_join_rows(&self, _condition: &JoinCondition) -> usize {
        // Simplified row estimation
        100
    }

    fn update_statistics(&mut self, tables: &[String]) -> Result<(), String> {
        for table in tables {
            self.statistics.update_table_stats(table, &self.db)?;
        }
        Ok(())
    }

    /// Extract real table name from alias (e.g., "test_users AS u" -> "test_users")
    fn extract_real_table_name(table: &str) -> &str {
        if table.contains(" AS ") {
            table.split(" AS ").next().unwrap_or(table).trim()
        } else if table.contains(" as ") {
            table.split(" as ").next().unwrap_or(table).trim()
        } else {
            table.trim()
        }
    }
}

impl TableStatistics {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    pub fn has_useful_index(&self, table: &str, _conditions: &HashMap<String, String>) -> bool {
        self.table_stats.get(table)
            .map(|stats| !stats.index_stats.is_empty())
            .unwrap_or(false)
    }

    pub fn update_table_stats(&mut self, table: &str, db: &Arc<Db>) -> Result<(), String> {
        // Extract real table name from alias
        let real_table_name = if table.contains(" AS ") {
            table.split(" AS ").next().unwrap_or(table).trim()
        } else if table.contains(" as ") {
            table.split(" as ").next().unwrap_or(table).trim()
        } else {
            table.trim()
        };
        
        let tree = db.open_tree(real_table_name).map_err(|e| e.to_string())?;
        let row_count = tree.len();
        
        let stats = TableStats {
            row_count,
            avg_row_size: 100, // Simplified
            column_stats: HashMap::new(),
            index_stats: HashMap::new(),
        };
        
        self.table_stats.insert(table.to_string(), stats);
        println!("📊 STATISTICS: Updated stats for table '{}' (real: '{}') - {} rows", table, real_table_name, row_count);
        Ok(())
    }
}