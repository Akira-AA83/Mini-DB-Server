// System setup and first-time installation for Mini-DB Server
use mini_db_server::client::AdminClient;
use std::path::Path;
use bcrypt::{hash, DEFAULT_COST};

pub struct SystemSetup {
    system_db_path: String,
}

impl SystemSetup {
    pub fn new(system_db_path: &str) -> Self {
        Self {
            system_db_path: system_db_path.to_string(),
        }
    }

    /// Check if this is the first time running the server
    pub fn is_first_installation(&self) -> bool {
        !Path::new(&self.system_db_path).exists()
    }

    /// Initialize system database with required tables
    pub fn initialize_system_database(&self) -> Result<(), String> {
        println!("🔧 Initializing system database...");

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        // Create system tables
        let system_tables = vec![
            // Admin management
            "CREATE TABLE system_admins (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                email TEXT,
                created_at TEXT,
                last_login TEXT,
                is_active INTEGER DEFAULT 1,
                permissions TEXT DEFAULT 'full_admin'
            )",
            
            // Server configuration
            "CREATE TABLE server_config (
                id INTEGER PRIMARY KEY,
                config_key TEXT UNIQUE NOT NULL,
                config_value TEXT NOT NULL,
                description TEXT,
                updated_at TEXT
            )",
            
            // Database registry
            "CREATE TABLE user_databases (
                database_name TEXT PRIMARY KEY,
                database_path TEXT NOT NULL,
                owner_admin_id INTEGER,
                created_at TEXT,
                is_active INTEGER DEFAULT 1,
                description TEXT
            )",
            
            // Installation info
            "CREATE TABLE installation_info (
                id INTEGER PRIMARY KEY,
                server_version TEXT NOT NULL,
                installation_date TEXT,
                last_startup TEXT,
                uptime_total INTEGER DEFAULT 0,
                is_first_run INTEGER DEFAULT 1
            )",
        ];

        for table_sql in &system_tables {
            match admin.execute_admin_query(table_sql) {
                Ok(_) => println!("   ✅ System table created"),
                Err(e) => {
                    println!("   ❌ Failed to create system table: {}", e);
                    return Err(format!("System table creation failed: {}", e));
                }
            }
        }

        Ok(())
    }

    /// Create the initial admin user
    pub fn create_initial_admin(&self, username: &str, password: &str, email: Option<&str>) -> Result<(), String> {
        println!("👤 Creating initial admin user: {}", username);

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        // Hash the password
        let password_hash = hash(password, DEFAULT_COST)
            .map_err(|e| format!("Failed to hash password: {}", e))?;

        let email_value = email.unwrap_or("admin@minidb.local");
        
        let insert_admin_sql = format!(
            "INSERT INTO system_admins (username, password_hash, email, permissions) VALUES ('{}', '{}', '{}', 'full_admin')",
            username, password_hash, email_value
        );

        match admin.execute_admin_query(&insert_admin_sql) {
            Ok(_) => {
                println!("   ✅ Initial admin user created successfully");
                Ok(())
            }
            Err(e) => {
                println!("   ❌ Failed to create initial admin: {}", e);
                Err(format!("Initial admin creation failed: {}", e))
            }
        }
    }

    /// Set initial server configuration
    pub fn set_initial_config(&self) -> Result<(), String> {
        println!("⚙️ Setting initial server configuration...");

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        let config_entries = vec![
            ("websocket_port", "8080", "WebSocket server port"),
            ("max_connections", "100", "Maximum concurrent connections"),
            ("default_database", "mini_db.db", "Default user database"),
            ("log_level", "info", "Server logging level"),
            ("session_timeout", "3600", "Session timeout in seconds"),
            ("backup_enabled", "true", "Enable automatic backups"),
            ("backup_interval", "86400", "Backup interval in seconds (24h)"),
        ];

        for (key, value, description) in config_entries {
            let config_sql = format!(
                "INSERT OR REPLACE INTO server_config (config_key, config_value, description) VALUES ('{}', '{}', '{}')",
                key, value, description
            );

            match admin.execute_admin_query(&config_sql) {
                Ok(_) => println!("   ✅ Config set: {} = {}", key, value),
                Err(e) => println!("   ⚠️ Config warning: {}", e),
            }
        }

        Ok(())
    }

    /// Record installation information
    pub fn record_installation(&self) -> Result<(), String> {
        println!("📝 Recording installation information...");

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        let version = env!("CARGO_PKG_VERSION");
        let install_sql = format!(
            "INSERT INTO installation_info (server_version, is_first_run) VALUES ('{}', 1)",
            version
        );

        match admin.execute_admin_query(&install_sql) {
            Ok(_) => {
                println!("   ✅ Installation recorded (version: {})", version);
                Ok(())
            }
            Err(e) => {
                println!("   ⚠️ Installation recording warning: {}", e);
                Ok(()) // Non-critical
            }
        }
    }

    /// Register the default user database
    pub fn register_default_database(&self, admin_id: i32) -> Result<(), String> {
        println!("🗄️ Registering default user database...");

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        let register_sql = format!(
            "INSERT INTO user_databases (database_name, database_path, owner_admin_id, description) VALUES ('default', 'mini_db.db', {}, 'Default user database')",
            admin_id
        );

        match admin.execute_admin_query(&register_sql) {
            Ok(_) => {
                println!("   ✅ Default database registered");
                Ok(())
            }
            Err(e) => {
                println!("   ⚠️ Database registration warning: {}", e);
                Ok(()) // Non-critical
            }
        }
    }

    /// Complete first-time setup
    pub fn run_first_time_setup(&self, admin_username: &str, admin_password: &str, admin_email: Option<&str>) -> Result<(), String> {
        println!("🚀 Running first-time Mini-DB setup...");
        println!("==========================================");

        // 1. Initialize system database
        self.initialize_system_database()?;

        // 2. Create initial admin
        self.create_initial_admin(admin_username, admin_password, admin_email)?;

        // 3. Set initial configuration
        self.set_initial_config()?;

        // 4. Record installation
        self.record_installation()?;

        // 5. Register default database
        self.register_default_database(1)?; // First admin has ID 1

        println!("✅ First-time setup completed successfully!");
        println!("🔑 Admin login: {} / {}", admin_username, admin_password);
        println!("🌐 WebSocket server will start on port 8080");
        println!("==========================================");

        Ok(())
    }

    /// Update server startup information
    pub fn update_startup_info(&self) -> Result<(), String> {
        if !Path::new(&self.system_db_path).exists() {
            return Ok(()); // System DB doesn't exist yet
        }

        let mut admin = AdminClient::new(&self.system_db_path, "system_master_key_2025")
            .map_err(|e| format!("Failed to create system admin client: {}", e))?;
        
        admin.authenticate("system_master_key_2025")
            .map_err(|e| format!("Failed to authenticate system admin: {}", e))?;

        let update_sql = "UPDATE installation_info SET last_startup = CURRENT_TIMESTAMP, is_first_run = 0 WHERE id = 1";

        match admin.execute_admin_query(update_sql) {
            Ok(_) => Ok(()),
            Err(_) => Ok(()), // Non-critical
        }
    }
}

/// Interactive setup for first-time installation
pub fn interactive_setup() -> Result<(), String> {
    use std::io::{self, Write};

    println!("🎉 Welcome to Mini-DB Server!");
    println!("This appears to be your first time running the server.");
    println!("Let's set up your admin account.\n");

    // Check for environment variables first (for automated testing)
    let username = std::env::var("MINIDB_ADMIN_USER").unwrap_or_else(|_| {
        print!("Enter admin username [admin]: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();
        if input.is_empty() { "admin".to_string() } else { input.to_string() }
    });

    let password = std::env::var("MINIDB_ADMIN_PASS").unwrap_or_else(|_| {
        print!("Enter admin password [admin123]: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();
        if input.is_empty() { "admin123".to_string() } else { input.to_string() }
    });

    let email = std::env::var("MINIDB_ADMIN_EMAIL").unwrap_or_else(|_| {
        print!("Enter admin email [admin@minidb.local]: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();
        if input.is_empty() { "admin@minidb.local".to_string() } else { input.to_string() }
    });

    println!("\n🔧 Setting up Mini-DB with:");
    println!("   Username: {}", username);
    println!("   Email: {}", email);
    println!("   Password: [hidden]");

    let setup = SystemSetup::new("mini_db_system.db");
    setup.run_first_time_setup(&username, &password, Some(&email))?;

    Ok(())
}