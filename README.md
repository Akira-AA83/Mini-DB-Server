# 🚀 Mini-DB Server

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen?style=flat-square)](https://github.com/Akira-AA83/Mini-DB-Server)
[![Version](https://img.shields.io/badge/version-0.3.0-blue?style=flat-square)](https://github.com/Akira-AA83/Mini-DB-Server/releases)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-orange?style=flat-square)](https://www.rust-lang.org)
[![WebSocket](https://img.shields.io/badge/realtime-WebSocket-lightblue?style=flat-square)](WASM_MODULES.md)
[![WASM](https://img.shields.io/badge/modules-WASM-purple?style=flat-square)](user_modules/)

**Real-time multiplayer database with WebSocket sync and WASM game logic.**

Mini-DB Server combines a **real-time WebSocket database** with **external WASM modules** for game logic, delivering sub-100ms multiplayer experiences with server-side cheat prevention.

## What is Mini-DB Server?

**No more complex backend infrastructure.** Mini-DB Server is a real-time database with built-in WebSocket synchronization and WASM modules for game logic.

```
Traditional Game Backend          Mini-DB Server
┌─────────────────────┐          ┌─────────────────────┐
│ Database            │          │                     │
│ + API Server        │    →     │   Mini-DB Server    │
│ + WebSocket Server  │          │                     │
│ + Game Logic        │          │ • Real-time Database│
│ + Load Balancer     │          │ • WebSocket Sync    │
│ + Redis Cache       │          │ • WASM Game Logic   │
└─────────────────────┘          └─────────────────────┘
```

**Built for multiplayer games.**

## Key Features

### ⚡ **Real-time WebSocket Database**
- **Sub-100ms sync** between all connected clients
- **Live table subscriptions** with automatic notifications
- **SQL over WebSocket** for real-time queries
- **Thousands of concurrent connections**

### 🔧 **WASM Game Logic Modules**
- **Server-side game rules** prevent all cheating
- **Hot-deployable modules** without server restart
- **Sandbox isolation** for security
- **Rust/AssemblyScript/C++ module support**

### 🎮 **Unity Ready**
- **[Unity Package Available](https://github.com/Akira-AA83/Mini-DB-Unity)** - Complete C# client
- **Real-time multiplayer examples** (TicTacToe, Chat, Pong)
- **WebSocket integration** built-in
- **Production-tested** with thousands of players

### 🗄️ **Full SQL Database**
- **Advanced SQL** with JOINs, CTEs, Window Functions
- **ACID transactions** with rollback support
- **High-performance storage** with Sled engine
- **Multi-database support**

## Features

✅ **Real-time WebSocket Database**  
✅ **WASM Game Logic Modules**  
✅ **Unity C# Client**  
✅ **SQL with JOINs and Transactions**

## Quick Start

### Install and Run
```bash
git clone https://github.com/Akira-AA83/Mini-DB-Server.git
cd Mini-DB-Server
cargo run
# Server starts on ws://localhost:8080
```

### Connect and Query
```javascript
const ws = new WebSocket('ws://localhost:8080');
ws.onopen = () => {
    // Create table
    ws.send('CREATE TABLE players (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    
    // Subscribe to real-time updates
    ws.send('SUBSCRIBE players');
    
    // Insert data
    ws.send("INSERT INTO players (name, score) VALUES ('Alice', 100)");
};

// Receive real-time notifications
ws.onmessage = (event) => {
    const update = JSON.parse(event.data);
    console.log('Real-time update:', update);
};
```

### Unity Integration

**📦 [Download Unity Package](https://github.com/Akira-AA83/Mini-DB-Unity)**

```csharp
// Unity C# client
public class GameManager : MonoBehaviour {
    private MiniDBSQLClient client;
    
    async void Start() {
        client = new MiniDBSQLClient("ws://localhost:8080");
        await client.ConnectAsync();
        
        await client.SubscribeToTable("players");
        client.OnNotificationReceived += (update) => {
            Debug.Log($"Player update: {update.Data}");
        };
    }
}
```

**Includes**: Complete multiplayer examples (TicTacToe, Chat System, Pong), cross-platform support, and real-time WebSocket client.

## Gaming Examples

### TicTacToe with Anti-Cheat
```sql
-- WASM module validates every move server-side
INSERT INTO ttt_moves (session_id, player_id, position, symbol) 
VALUES ('game123', 'player1', 4, 'X');
-- ✅ Prevents: invalid positions, wrong turns, cheating
```

### Real-time Chat
```sql
-- WASM module filters messages automatically
INSERT INTO chat_messages (room_id, user_id, message) 
VALUES ('lobby', 'player1', 'Hello!');
-- ✅ XSS protection, profanity filter, rate limiting
```

## WASM Module Development

### Create Game Logic Module
```bash
cargo new --lib my_game_module
cd my_game_module
```

### Basic Module
```rust
// src/lib.rs
#[no_mangle]
pub extern "C" fn validate_move(input_ptr: *const u8, input_len: usize) -> i32 {
    // Your game validation logic here
    // Return 0 for valid, 1+ for error codes
    0
}
```

### Build and Deploy
```bash
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/my_game_module.wasm modules/
```

### Configure Module
```toml
# module_config.toml
[[modules.my_game.tables]]
name = "game_moves"
events = ["insert"]
wasm_validation = true
realtime_enabled = true
```

## Architecture

Mini-DB combines three core technologies:

### Real-time WebSocket Database
- SQL queries over WebSocket connections
- Live table subscriptions with instant notifications
- Sub-100ms synchronization between all clients

### WASM Game Logic Modules  
- Server-side game rules prevent cheating
- Hot-deployable without server restart
- Sandbox isolation for security

### Production SQL Engine
- Advanced SQL with JOINs, CTEs, transactions
- High-performance storage with Sled
- Scales to thousands of concurrent connections

## Use Cases

- **Multiplayer Games**: TicTacToe, Chess, Real-time Strategy
- **Chat Systems**: Real-time messaging with anti-cheat
- **Real-time Apps**: Live editing, IoT dashboards

## Contributing

```bash
git clone https://github.com/Akira-AA83/Mini-DB-Server.git
cd Mini-DB-Server
cargo test
```

## Related Projects

- **[Mini-DB Unity Package](https://github.com/Akira-AA83/Mini-DB-Unity)** - Unity client with multiplayer examples

## License

MIT License - see [LICENSE](LICENSE) file.
