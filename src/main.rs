use tokio::net::TcpListener;
use mini_db_server::client::AdminClient;
use mini_db_server::sync::SyncServer;
use mini_db_server::connection_manager;
use std::env;
use std::sync::Arc;

use mini_db_server::wasm::WasmEngine;

mod system_setup;
use system_setup::{SystemSetup, interactive_setup};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    println!("🎯 Mini-DB Server - Modular Gaming Database");
    println!("==========================================\n");
    
    // Check for first-time installation
    let system_setup = SystemSetup::new("mini_db_system.db");
    if system_setup.is_first_installation() {
        println!("🆕 First-time installation detected!");
        if let Err(e) = interactive_setup() {
            eprintln!("❌ Setup failed: {}", e);
            std::process::exit(1);
        }
        println!();
    } else {
        // Update startup info for existing installation
        let _ = system_setup.update_startup_info();
    }
    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let mut db_path = "mini_db.db".to_string();
    let mut ws_port = 8080u16;
    let mut demo_mode = false;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--db" | "-d" => {
                if i + 1 < args.len() {
                    db_path = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --db requires a database path");
                    std::process::exit(1);
                }
            }
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    ws_port = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: Invalid port number");
                        std::process::exit(1);
                    });
                    i += 2;
                } else {
                    eprintln!("Error: --port requires a port number");
                    std::process::exit(1);
                }
            }
            "--demo" => {
                demo_mode = true;
                i += 1;
            }
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_help();
                std::process::exit(1);
            }
        }
    }
    
    println!("🔧 Configuration:");
    println!("   Database: {}", db_path);
    println!("   WebSocket Port: {}", ws_port);
    println!("   Demo Mode: {}", demo_mode);
    println!();
    
    // Initialize database and setup
    if demo_mode {
        setup_demo_data(&db_path).await?;
    } else {
        setup_production_database(&db_path).await?;
    }
    
    // Start the integrated server
    start_server(&db_path, ws_port).await?;
    
    Ok(())
}

async fn start_server(db_path: &str, ws_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Mini-DB Server...");
    
    // Initialize WASM Engine for external modules
    println!("🎮 Initializing WASM Engine for external modules...");
    let wasm_engine = Arc::new(WasmEngine::new().map_err(|e| format!("Failed to initialize WASM engine: {}", e))?);
    
    // Auto-load available WASM modules
    if std::path::Path::new("modules/tictactoe.wasm").exists() {
        if let Err(e) = wasm_engine.load_module("tictactoe", "modules/tictactoe.wasm") {
            println!("⚠️ Warning: Failed to load TicTacToe module: {}", e);
        } else {
            println!("   ✅ TicTacToe module loaded successfully");
        }
    }
    
    // Check for other modules
    if let Ok(entries) = std::fs::read_dir("modules") {
        let mut module_count = 0;
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".wasm") && name != "tictactoe.wasm" {
                        let module_name = name.trim_end_matches(".wasm");
                        let module_path = format!("modules/{}", name);
                        if let Err(e) = wasm_engine.load_module(module_name, &module_path) {
                            println!("   ⚠️ Warning: Failed to load {} module: {}", module_name, e);
                        } else {
                            println!("   ✅ {} module loaded successfully", module_name);
                            module_count += 1;
                        }
                    }
                }
            }
        }
        if module_count > 0 {
            println!("   📦 Total external modules loaded: {}", module_count + 1);
        }
    } else {
        println!("   📁 Creating modules directory...");
        std::fs::create_dir_all("modules").ok();
    }
    
    println!("   🏛️ Server core remains immutable - all game logic is external!");
    
    // Get shared database connection from connection manager
    let db = connection_manager::DatabaseConnectionManager::global()
        .get_connection(db_path)
        .map_err(|e| format!("Failed to get shared database connection: {}", e))?;
    
    // Create the sync server with shared database connection
    let mut sync_server = SyncServer::with_shared_db(db, 1000, 3600);
    
    // Auto-configure modules if module_config.toml exists
    if std::path::Path::new("module_config.toml").exists() {
        println!("📋 Found module_config.toml - configuring external modules...");
        // Configuration will be loaded automatically by the query executor during database operations
        println!("   ✅ Module configuration file detected, will be applied during query processing");
    } else {
        println!("   📁 No module_config.toml found - WASM modules will run without table bindings");
    }
    
    // Bind to all interfaces for WSL2/Docker compatibility
    let ws_addr = format!("0.0.0.0:{}", ws_port);
    let listener = TcpListener::bind(&ws_addr).await?;
    
    println!("✅ Mini-DB Server started successfully!");
    println!("🌐 WebSocket API: ws://0.0.0.0:{}", ws_port);
    
    // Get the actual IP for external access
    if let Ok(output) = tokio::process::Command::new("hostname")
        .arg("-I")
        .output()
        .await
    {
        if let Ok(ip_str) = String::from_utf8(output.stdout) {
            let ip = ip_str.split_whitespace().next().unwrap_or("127.0.0.1");
            println!("🔗 External Access: ws://{}:{}", ip, ws_port);
        }
    }
    
    println!("📱 Connect with Angular Admin Panel or any WebSocket client");
    println!("💡 Send SQL queries as text messages");
    println!("📋 Available commands:");
    println!("   • SHOW TABLES");
    println!("   • SHOW STATUS");
    println!("   • SELECT * FROM table_name");
    println!("   • SUBSCRIBE table_name");
    println!("   • And any standard SQL...");
    println!();
    
    // Start the WebSocket server
    sync_server.start_with_listener(listener).await;
    
    Ok(())
}

async fn setup_demo_data(db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Setting up demo database...");
    
    let mut admin = AdminClient::new(db_path, "admin123")?;
    admin.authenticate("admin123")?;
    
    // Create demo tables with proper primary keys
    let demo_tables = vec![
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            category TEXT,
            stock INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            total REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )"
    ];

    for table_sql in &demo_tables {
        match admin.execute_admin_query(table_sql) {
            Ok(_) => println!("   ✅ Table created"),
            Err(e) => println!("   ⚠️ Table creation: {}", e),
        }
    }

    // Insert sample data
    let sample_data = vec![
        "INSERT INTO users (id, username, email) VALUES (1, 'alice', 'alice@example.com')",
        "INSERT INTO users (id, username, email) VALUES (2, 'bob', 'bob@example.com')",
        "INSERT INTO users (id, username, email) VALUES (3, 'charlie', 'charlie@example.com')",
        "INSERT INTO products (id, name, price, category, stock) VALUES (1, 'Laptop', 999.99, 'Electronics', 10)",
        "INSERT INTO products (id, name, price, category, stock) VALUES (2, 'Coffee Mug', 12.99, 'Kitchen', 50)",
        "INSERT INTO products (id, name, price, category, stock) VALUES (3, 'Book', 24.99, 'Education', 25)",
        "INSERT INTO orders (id, user_id, total, status) VALUES (1, 1, 999.99, 'completed')",
        "INSERT INTO orders (id, user_id, total, status) VALUES (2, 2, 12.99, 'pending')",
        "INSERT INTO orders (id, user_id, total, status) VALUES (3, 1, 24.99, 'shipped')"
    ];

    for insert_sql in &sample_data {
        match admin.execute_admin_query(insert_sql) {
            Ok(_) => println!("   ✅ Sample data inserted"),
            Err(e) => println!("   ⚠️ Sample data: {}", e),
        }
    }

    println!("✅ Demo database ready!\n");
    Ok(())
}

async fn setup_production_database(db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Setting up production database...");
    
    let mut admin = AdminClient::new(db_path, "admin123")?;
    admin.authenticate("admin123")?;
    
    // Create admin user if not exists
    match admin.create_user("admin", "admin@minidb.com", "admin123", vec!["admin".to_string()]) {
        Ok(_) => println!("   ✅ Admin user created"),
        Err(e) => println!("   ⚠️ Admin user: {}", e),
    }
    
    println!("✅ Production database ready!\n");
    Ok(())
}

fn print_help() {
    println!("Mini-DB Server - A SpacetimeDB-inspired SQL database with real-time sync");
    println!();
    println!("USAGE:");
    println!("    mini-db [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    -d, --db <PATH>         Database file path (default: mini_db.db)");
    println!("    -p, --port <PORT>       WebSocket port (default: 8080)");
    println!("    --demo                  Start with demo data");
    println!("    -h, --help              Print this help message");
    println!();
    println!("EXAMPLES:");
    println!("    mini-db --demo                    # Start with demo data");
    println!("    mini-db --db myapp.db --port 9000   # Custom database and port");
    println!("    mini-db --db production.db          # Production mode");
    println!();
}