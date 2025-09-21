use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use futures_util::{StreamExt, SinkExt};
use std::collections::HashMap;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::query::QueryExecutor;
use crate::parser::{SQLParser, ParsedQuery};
use serde_json::json;
use crate::connection_manager::DatabaseConnectionManager;
use crate::modules::DatabaseEvent;
use uuid::Uuid;

// Client connection info including current database
#[derive(Clone)]
struct ClientInfo {
    sender: broadcast::Sender<String>,
    current_database: String,
}

#[derive(Clone)]
pub struct SyncServer {
    clients: Arc<Mutex<HashMap<String, Vec<ClientInfo>>>>,
    query_executor: Arc<QueryExecutor>,
    default_database: String,
}

impl SyncServer {
    pub fn new(db_path: &str, cache_size: usize, cache_ttl: u64) -> Self {
        // ✅ Crea il database UNA VOLTA SOLA e condividilo con gli altri componenti
        let db = Arc::new(sled::open(db_path).expect("Errore nell'aprire il DB"));

        let server = Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            query_executor: QueryExecutor::new(Arc::clone(&db), cache_size, cache_ttl),
            default_database: "default".to_string(),
        };
        
        // Set up WebSocket notification callback for real-time broadcasting
        server.setup_notification_callback();
        
        server
    }
    
    /// Set up the notification callback to connect module system to WebSocket broadcasting
    fn setup_notification_callback(&self) {
        let clients = Arc::clone(&self.clients);
        
        let callback = Arc::new(move |database: &str, table: &str, message: &str| {
            // This will be called by modules when they want to send notifications
            println!("📡 Module notification: {}.{} -> {}", database, table, message);
            
            // Create a simple runtime for async operations within the callback
            let rt = tokio::runtime::Handle::try_current();
            if let Ok(handle) = rt {
                let clients_clone = Arc::clone(&clients);
                let database = database.to_string();
                let table = table.to_string();
                let message = message.to_string();
                
                handle.spawn(async move {
                    Self::broadcast_to_subscribers(clients_clone, &database, &table, &message).await;
                });
            } else {
                println!("⚠️ No tokio runtime available for notification broadcasting");
            }
        });
        
        self.query_executor.set_notification_callback(callback);
    }
    
    /// Helper method for broadcasting to subscribers (static to avoid self reference issues)
    async fn broadcast_to_subscribers(
        clients: Arc<Mutex<HashMap<String, Vec<ClientInfo>>>>,
        database: &str, 
        table: &str, 
        message: &str
    ) {
        let clients_map = clients.lock().await;
        let subscription_key = format!("{}_{}", database, table);
        
        if let Some(client_list) = clients_map.get(&subscription_key) {
            let notification = serde_json::json!({
                "type": "table_notification",
                "database": database,
                "table": table,
                "notification": true,
                "data": message,
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            let notification_str = notification.to_string();
            let mut successful_sends = 0;
            let mut failed_sends = 0;
            
            for client_info in client_list {
                if let Err(e) = client_info.sender.send(notification_str.clone()) {
                    println!("⚠️ Failed to broadcast to one client: {}", e);
                    failed_sends += 1;
                } else {
                    successful_sends += 1;
                }
            }
            
            println!("📡 Broadcasted notification to {}/{} subscribers for table {}.{}", 
                     successful_sends, client_list.len(), database, table);
            
            if failed_sends > 0 {
                println!("⚠️ {} clients failed to receive notification", failed_sends);
            }
        } else {
            println!("📡 No subscribers found for table {} in database {}", table, database);
        }
    }
    
    /// Broadcast notification to all clients subscribed to a specific table
    pub async fn broadcast_table_notification(&self, database: &str, table: &str, message: &str) {
        let clients_map = self.clients.lock().await;
        let subscription_key = format!("{}_{}", database, table);
        
        if let Some(client_list) = clients_map.get(&subscription_key) {
            let notification = serde_json::json!({
                "type": "table_notification",
                "database": database,
                "table": table,
                "notification": true,
                "data": message,
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            let notification_str = notification.to_string();
            let mut successful_sends = 0;
            
            for client_info in client_list {
                if let Err(e) = client_info.sender.send(notification_str.clone()) {
                    println!("⚠️ Failed to broadcast to one client: {}", e);
                } else {
                    successful_sends += 1;
                }
            }
            
            println!("📡 Broadcasted notification to {} subscribers for table {}", successful_sends, table);
        } else {
            println!("📡 No subscribers found for table {} in database {}", table, database);
        }
    }

    pub fn with_shared_db(db: Arc<sled::Db>, cache_size: usize, cache_ttl: u64) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            query_executor: QueryExecutor::new(db, cache_size, cache_ttl),
            default_database: "default".to_string(),
        }
    }

    pub async fn start(&self, addr: &str) {
        let listener = TcpListener::bind(addr).await.expect("Errore nel bind del WebSocket server");

        println!("🔄 WebSocket Sync Server avviato su {}", addr);
        let server = Arc::new(self.clone());

        while let Ok((stream, _)) = listener.accept().await {
            let server_clone = Arc::clone(&server);
            let clients = Arc::clone(&self.clients);
            let query_executor = Arc::clone(&self.query_executor);
            tokio::spawn(Self::handle_client(server_clone, stream));
        }
    }

    pub async fn start_with_listener(&self, listener: TcpListener) {
        println!("🔄 WebSocket Sync Server avviato su {:?}", listener.local_addr().unwrap());
        let server = Arc::new(self.clone());

        while let Ok((stream, _)) = listener.accept().await {
            let server_clone = Arc::clone(&server);
            let clients = Arc::clone(&self.clients);
            let query_executor = Arc::clone(&self.query_executor);
            tokio::spawn(Self::handle_client(server_clone, stream));
        }
    }

    async fn handle_client(
        server: Arc<SyncServer>,
        stream: TcpStream
    ) {
        let peer_addr = stream.peer_addr().ok();
        println!("🔗 New connection attempt from {:?}", peer_addr);
        
        let ws_stream = match accept_async(stream).await {
            Ok(ws) => {
                println!("✅ WebSocket handshake successful for {:?}", peer_addr);
                ws
            }
            Err(e) => {
                println!("❌ WebSocket handshake failed for {:?}: {}", peer_addr, e);
                return;
            }
        };
        
        let (mut write, mut read) = ws_stream.split();
        
        // Send welcome message with current database info
        let welcome = json!({
            "type": "welcome",
            "message": "Connected to Mini-DB WebSocket Server",
            "current_database": server.default_database,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "available_commands": [
                "SHOW TABLES",
                "SELECT * FROM table_name", 
                "SUBSCRIBE table_name",
                "Any SQL query..."
            ]
        });
        
        if let Err(e) = write.send(tokio_tungstenite::tungstenite::Message::Text(welcome.to_string())).await {
            if !e.to_string().contains("SendAfterClosing") {
                println!("❌ Failed to send welcome message to {:?}: {}", peer_addr, e);
            }
            return;
        }
        
        println!("📤 Welcome message sent to {:?}", peer_addr);
    
        let (tx, mut rx) = broadcast::channel::<String>(10);
        let client_id = format!("{:?}", peer_addr.unwrap_or_else(|| "unknown".parse().unwrap()));
        let mut current_database = server.default_database.clone();
        let mut current_query_executor = Arc::clone(&server.query_executor);
        let mut active_transaction_id: Option<String> = None;
        
        // ✅ CRITICAL FIX: Start broadcast receiver task for real-time notifications
        let write_clone = Arc::new(Mutex::new(write));
        let write_for_notifications = Arc::clone(&write_clone);
        tokio::spawn(async move {
            while let Ok(notification) = rx.recv().await {
                println!("🔥 Sending notification to WebSocket client: {}", notification);
                let mut writer = write_for_notifications.lock().await;
                if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(notification)).await {
                    if !e.to_string().contains("SendAfterClosing") {
                        println!("⚠️ Failed to send notification to client: {}", e);
                    }
                    break; // Exit if connection is closed
                }
            }
        });
    
        while let Some(Ok(msg)) = read.next().await {
            if let Ok(query_str) = msg.to_text() {
                // ✅ FILTER: Skip empty queries (confirmed Unity WebSocket client artifact)
                if query_str.trim().is_empty() {
                    // Silently skip empty queries to reduce log noise
                    continue;
                }
                
                println!("📩 Query ricevuta: {}", query_str);
    
                // ✅ Gestisci i comandi di iscrizione
                if query_str.starts_with("SUBSCRIBE ") {
                    let table = query_str.replace("SUBSCRIBE ", "");
                    let mut clients_map = server.clients.lock().await;
                    let client_info = ClientInfo {
                        sender: tx.clone(),
                        current_database: current_database.clone(),
                    };
                    let subscription_key = format!("{}_{}", current_database, table);
                    
                    // ✅ CRITICAL FIX: Add to Vec instead of overwriting
                    clients_map.entry(subscription_key.clone())
                        .or_insert_with(Vec::new)
                        .push(client_info);
                    
                    let subscriber_count = clients_map.get(&subscription_key).map(|v| v.len()).unwrap_or(0);
                    println!("📡 Client iscritto alla tabella: {} nel database: {} (total subscribers: {})", 
                             table, current_database, subscriber_count);
    
                    // ✅ Invia conferma di iscrizione (sistemato il lifetime)
                    let ack_message = format!("ACK: SUBSCRIBE {} ON DATABASE {}", table, current_database);
                    let mut writer = write_clone.lock().await;
                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(ack_message)).await {
                        if !e.to_string().contains("SendAfterClosing") {
                            println!("⚠️ Errore nell'invio dell'ACK: {:?}", e);
                        }
                    }
                    continue;
                }
    
                // 🗄️ Handle database switching commands
                if let Ok(parsed_query) = SQLParser::parse_query(query_str) {
                    match &parsed_query {
                        ParsedQuery::UseDatabase { name } => {
                            // Switch database for this client
                            let new_db_path = format!("{}.db", name);
                            
                            match DatabaseConnectionManager::global().get_connection(&new_db_path) {
                                Ok(new_db) => {
                                    current_database = name.clone();
                                    current_query_executor = QueryExecutor::new(new_db, 100, 60);
                                    
                                    // IMPORTANT: Set up callback for the new QueryExecutor
                                    let clients_for_callback = Arc::clone(&server.clients);
                                    let callback = Arc::new(move |database: &str, table: &str, message: &str| {
                                        println!("📡 Module notification: {}.{} -> {}", database, table, message);
                                        
                                        let rt = tokio::runtime::Handle::try_current();
                                        if let Ok(handle) = rt {
                                            let clients_clone = Arc::clone(&clients_for_callback);
                                            let database = database.to_string();
                                            let table = table.to_string();
                                            let message = message.to_string();
                                            
                                            handle.spawn(async move {
                                                Self::broadcast_to_subscribers(clients_clone, &database, &table, &message).await;
                                            });
                                        } else {
                                            println!("⚠️ No tokio runtime available for notification broadcasting");
                                        }
                                    });
                                    
                                    current_query_executor.set_notification_callback(callback);
                                    println!("✅ WebSocket notification callback registered for database: {}", name);
                                    
                                    let response = json!({
                                        "status": 200,
                                        "message": format!("Switched to database '{}'", name),
                                        "current_database": name,
                                        "timestamp": chrono::Utc::now().to_rfc3339()
                                    });
                                    
                                    let mut writer = write_clone.lock().await;
                                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(response.to_string())).await {
                                        if !e.to_string().contains("SendAfterClosing") {
                                            println!("⚠️ Errore nell'invio della risposta: {:?}", e);
                                        }
                                    }
                                    
                                    println!("🔄 Client {} switched to database: {}", client_id, name);
                                    continue;
                                }
                                Err(e) => {
                                    let error_msg = format!("ERROR: Failed to switch to database '{}': {}", name, e);
                                    let mut writer = write_clone.lock().await;
                                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(error_msg)).await {
                                        if !e.to_string().contains("SendAfterClosing") {
                                            println!("⚠️ Errore nell'invio dell'errore: {:?}", e);
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                        _ => {
                            // Regular query processing
                        }
                    }
                }
    
                // ✅ Esegui la query SQL - supporto multi-statement
                let statements: Vec<&str> = query_str.split(';')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                
                let mut all_results = Vec::new();
                let mut has_error = false;
                let statements_count = statements.len();
                
                for statement in &statements {
                    match SQLParser::parse_query(statement) {
                        Ok(parsed_query) => {
                            // ✅ Estrai il nome della tabella PRIMA di eseguire la query
                            let table_name = Self::extract_table_name(&parsed_query);
                            
                            // Handle transaction commands specially
                            let tx_id = match &parsed_query {
                                ParsedQuery::BeginTransaction => {
                                    // Generate new transaction ID
                                    let new_tx_id = uuid::Uuid::new_v4().to_string();
                                    active_transaction_id = Some(new_tx_id.clone());
                                    active_transaction_id.clone()
                                }
                                ParsedQuery::Commit | ParsedQuery::Rollback => {
                                    // Use active transaction ID
                                    active_transaction_id.clone()
                                }
                                _ => active_transaction_id.clone()
                            };
                            
                            match current_query_executor.execute_query(&parsed_query, tx_id) {
                                Ok(result) => {
                                    println!("✅ Query eseguita con successo: {}", result);
                                    all_results.push(result.clone());
                                    
                                    // Clear transaction ID after COMMIT or ROLLBACK
                                    match &parsed_query {
                                        ParsedQuery::Commit | ParsedQuery::Rollback => {
                                            active_transaction_id = None;
                                        }
                                        _ => {}
                                    }
                                    
                                    // Send result for this statement
                                    let mut writer = write_clone.lock().await;
                                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(result)).await {
                                        if !e.to_string().contains("SendAfterClosing") {
                                            println!("⚠️ Errore nell'invio del risultato: {:?}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    // DEBUG: Intercetta tutti gli errori per trovare "Missing ID field"
                                    if e.contains("field") || e.contains("Field") || e.contains("ID") || e.contains("id") || e.contains("cannot be NULL") {
                                        println!("🔍 DEBUG SYNC: Found error with field/ID/NULL: {}", e);
                                    }
                                    println!("🔍 DEBUG SYNC: General error: {}", e);
                                    has_error = true;
                                    let error_response = json!({
                                        "status": 400,
                                        "message": format!("Query failed: {}", e),
                                        "timestamp": chrono::Utc::now().to_rfc3339()
                                    });
                                    
                                    let mut writer = write_clone.lock().await;
                                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(error_response.to_string())).await {
                                        if !e.to_string().contains("SendAfterClosing") {
                                            println!("⚠️ Errore nell'invio dell'errore: {:?}", e);
                                        }
                                    }
                                    break; // Stop processing on error
                                }
                            }
                        }
                        Err(e) => {
                            println!("🔍 DEBUG SYNC: Parse error: {}", e);
                            // DEBUG: Intercetta errori di parsing
                            if e.contains("field") || e.contains("Field") || e.contains("ID") || e.contains("id") || e.contains("cannot be NULL") {
                                println!("🔍 DEBUG SYNC: Parse error with field/ID/NULL: {}", e);
                            }
                            has_error = true;
                            let error_response = json!({
                                "status": 400,
                                "message": format!("Parse error: {}", e),
                                "timestamp": chrono::Utc::now().to_rfc3339()
                            });
                            
                            let mut writer = write_clone.lock().await;
                            if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(error_response.to_string())).await {
                                if !e.to_string().contains("SendAfterClosing") {
                                    println!("⚠️ Errore nell'invio dell'errore: {:?}", e);
                                }
                            }
                            break; // Stop processing on parse error
                        }
                    }
                }
                
                // If no errors and multiple statements, send summary
                if !has_error && statements_count > 1 {
                    let summary = json!({
                        "status": 200,
                        "message": format!("{} statements executed successfully", statements_count),
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    });
                    
                    let mut writer = write_clone.lock().await;
                    if let Err(e) = writer.send(tokio_tungstenite::tungstenite::Message::Text(summary.to_string())).await {
                        if !e.to_string().contains("SendAfterClosing") {
                            println!("⚠️ Errore nell'invio del summary: {:?}", e);
                        }
                    }
                }
            }
        }
    }
    
    fn extract_table_name(parsed_query: &ParsedQuery) -> Option<String> {
        match parsed_query {
            ParsedQuery::Select { table, .. } => Some(table.clone()),
            ParsedQuery::Insert { table, .. } => Some(table.clone()),
            ParsedQuery::Update { table, .. } => Some(table.clone()),
            ParsedQuery::Delete { table, .. } => Some(table.clone()),
            ParsedQuery::CreateTable { table, .. } => Some(table.clone()),
            ParsedQuery::DropTable { table } => Some(table.clone()),
            _ => None,
        }
    }
}


impl SyncServer {
    // ✅ Usa il nome del metodo esistente nel codebase
    pub async fn notify_changes(&self, database: &str, table: &str, change: &str) {
        let clients_lock = self.clients.lock().await;
        
        println!("🔍 Verifica dei client registrati prima dell'invio della notifica...");

        if clients_lock.is_empty() {
            println!("⚠️ Nessun client registrato per nessuna tabella.");
        } else {
            // Stampiamo i client registrati per ogni tabella
            for (table_name, _) in clients_lock.iter() {
                println!("📌 Client registrato per tabella: {}", table_name);
            }
        }
        
        let table_key = format!("{}_{}", database, table);
        if let Some(client_list) = clients_lock.get(&table_key) {
            let msg = json!({
                "database": database,
                "table": table,
                "change": change,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }).to_string();
    
            println!("📡 Inviando notifica ai {} client della tabella `{}`: {}", client_list.len(), table, msg);
    
            let mut successful_sends = 0;
            for client_info in client_list {
                if let Err(e) = client_info.sender.send(msg.clone()) {
                    println!("⚠️ Errore nell'invio della notifica a uno dei client di {}_{}: {:?}", database, table, e);
                } else {
                    successful_sends += 1;
                }
            }
            
            println!("✅ Notifica inviata con successo a {}/{} client di {}", successful_sends, client_list.len(), table);
        } else {
            println!("⚠️ Nessun client registrato per la tabella {}", table);
        }
    }
}