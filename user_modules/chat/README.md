# 🗨️ Chat WASM Module

Real-time chat system implementation using WASM for Mini-DB modular architecture.

## Features

- ✅ **Message Validation** - Content sanitization and length checks
- ✅ **Room Management** - Create, join, leave rooms
- ✅ **User Management** - Participant tracking and roles
- ✅ **Content Filtering** - XSS protection and harmful content removal
- ✅ **Real-time Events** - Live message broadcasting
- ✅ **Room Statistics** - User counts and activity tracking

## Exported Functions

### `validate_message(json_string) -> ValidationResult`
Validates and sanitizes chat messages before database insertion.

### `validate_room_creation(json_string) -> ValidationResult`
Validates room creation parameters and settings.

### `process_chat_action(json_string) -> ActionResult`
Processes chat actions like join, leave, send message.

### `get_room_stats(room_id) -> RoomStats`
Returns statistics for a specific chat room.

## Message Types

- `text` - Regular user messages
- `system` - System notifications
- `action` - User actions (join/leave)
- `join` - User joined room
- `leave` - User left room

## Room Types

- `public` - Open to all users
- `private` - Invite-only rooms
- `game` - Game-specific chat rooms

## Security Features

- Message length limits (1000 chars)
- Username validation (1-50 chars)
- XSS prevention
- Content sanitization
- Input validation

## Build

```bash
cd user_modules/chat
cargo build --release --target wasm32-unknown-unknown
```

## Usage

The module is automatically loaded by Mini-DB server when placed in the `modules/` directory and configured in `module_config.toml`.