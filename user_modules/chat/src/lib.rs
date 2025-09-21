use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChatMessage {
    pub message_id: Option<i32>,
    pub room_id: String,
    pub user_id: String,
    pub username: String,
    pub message_content: String,
    pub message_type: String,
    pub timestamp: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChatRoom {
    pub room_id: String,
    pub room_name: String,
    pub room_type: String,
    pub created_by: String,
    pub max_users: i32,
    pub created_at: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChatParticipant {
    pub room_id: String,
    pub user_id: String,
    pub username: String,
    pub joined_at: Option<i64>,
    pub role: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ValidationResult {
    pub valid: bool,
    pub error: Option<String>,
    pub sanitized_content: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatAction {
    pub action_type: String,
    pub room_id: String,
    pub user_id: String,
    pub data: serde_json::Value,
}

// Export functions for WASM
#[no_mangle]
pub extern "C" fn validate_message(input_ptr: *const u8, input_len: usize) -> *mut u8 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let input_str = match std::str::from_utf8(input) {
        Ok(s) => s,
        Err(_) => return create_response(ValidationResult {
            valid: false,
            error: Some("Invalid UTF-8 input".to_string()),
            sanitized_content: None,
        }),
    };

    let message: ChatMessage = match serde_json::from_str(input_str) {
        Ok(m) => m,
        Err(e) => return create_response(ValidationResult {
            valid: false,
            error: Some(format!("JSON parse error: {}", e)),
            sanitized_content: None,
        }),
    };

    let validation = validate_chat_message(&message);
    create_response(validation)
}

#[no_mangle]
pub extern "C" fn validate_room_creation(input_ptr: *const u8, input_len: usize) -> *mut u8 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let input_str = match std::str::from_utf8(input) {
        Ok(s) => s,
        Err(_) => return create_response(ValidationResult {
            valid: false,
            error: Some("Invalid UTF-8 input".to_string()),
            sanitized_content: None,
        }),
    };

    let room: ChatRoom = match serde_json::from_str(input_str) {
        Ok(r) => r,
        Err(e) => return create_response(ValidationResult {
            valid: false,
            error: Some(format!("JSON parse error: {}", e)),
            sanitized_content: None,
        }),
    };

    let validation = validate_chat_room(&room);
    create_response(validation)
}

#[no_mangle]
pub extern "C" fn process_chat_action(input_ptr: *const u8, input_len: usize) -> *mut u8 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let input_str = match std::str::from_utf8(input) {
        Ok(s) => s,
        Err(_) => return create_response(serde_json::json!({
            "success": false,
            "error": "Invalid UTF-8 input"
        })),
    };

    let action: ChatAction = match serde_json::from_str(input_str) {
        Ok(a) => a,
        Err(e) => return create_response(serde_json::json!({
            "success": false,
            "error": format!("JSON parse error: {}", e)
        })),
    };

    let result = execute_chat_action(&action);
    create_response(result)
}

#[no_mangle]
pub extern "C" fn get_room_stats(input_ptr: *const u8, input_len: usize) -> *mut u8 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let input_str = match std::str::from_utf8(input) {
        Ok(s) => s,
        Err(_) => return create_response(serde_json::json!({
            "error": "Invalid UTF-8 input"
        })),
    };

    let room_id = input_str.trim_matches('"');
    let stats = calculate_room_stats(room_id);
    create_response(stats)
}

// Internal validation functions
fn validate_chat_message(message: &ChatMessage) -> ValidationResult {
    // Check message content length
    if message.message_content.is_empty() {
        return ValidationResult {
            valid: false,
            error: Some("Message content cannot be empty".to_string()),
            sanitized_content: None,
        };
    }

    if message.message_content.len() > 1000 {
        return ValidationResult {
            valid: false,
            error: Some("Message content too long (max 1000 characters)".to_string()),
            sanitized_content: None,
        };
    }

    // Sanitize content (remove potential harmful content)
    let sanitized = sanitize_message_content(&message.message_content);

    // Check for valid room_id format
    if message.room_id.is_empty() || message.room_id.len() > 100 {
        return ValidationResult {
            valid: false,
            error: Some("Invalid room_id".to_string()),
            sanitized_content: None,
        };
    }

    // Check username
    if message.username.is_empty() || message.username.len() > 50 {
        return ValidationResult {
            valid: false,
            error: Some("Invalid username (1-50 characters)".to_string()),
            sanitized_content: None,
        };
    }

    // Validate message type
    let valid_types = ["text", "system", "action", "join", "leave"];
    if !valid_types.contains(&message.message_type.as_str()) {
        return ValidationResult {
            valid: false,
            error: Some("Invalid message type".to_string()),
            sanitized_content: None,
        };
    }

    ValidationResult {
        valid: true,
        error: None,
        sanitized_content: Some(sanitized),
    }
}

fn validate_chat_room(room: &ChatRoom) -> ValidationResult {
    // Check room name
    if room.room_name.is_empty() || room.room_name.len() > 100 {
        return ValidationResult {
            valid: false,
            error: Some("Room name must be 1-100 characters".to_string()),
            sanitized_content: None,
        };
    }

    // Check room type
    let valid_types = ["public", "private", "game"];
    if !valid_types.contains(&room.room_type.as_str()) {
        return ValidationResult {
            valid: false,
            error: Some("Invalid room type".to_string()),
            sanitized_content: None,
        };
    }

    // Check max users
    if room.max_users < 1 || room.max_users > 1000 {
        return ValidationResult {
            valid: false,
            error: Some("Max users must be between 1 and 1000".to_string()),
            sanitized_content: None,
        };
    }

    // Sanitize room name
    let sanitized_name = sanitize_text(&room.room_name);

    ValidationResult {
        valid: true,
        error: None,
        sanitized_content: Some(sanitized_name),
    }
}

fn execute_chat_action(action: &ChatAction) -> serde_json::Value {
    match action.action_type.as_str() {
        "send_message" => {
            // Process message sending
            serde_json::json!({
                "success": true,
                "action": "message_sent",
                "room_id": action.room_id,
                "timestamp": chrono::Utc::now().timestamp()
            })
        }
        "join_room" => {
            // Process room joining
            serde_json::json!({
                "success": true,
                "action": "room_joined",
                "room_id": action.room_id,
                "user_id": action.user_id,
                "timestamp": chrono::Utc::now().timestamp()
            })
        }
        "leave_room" => {
            // Process room leaving
            serde_json::json!({
                "success": true,
                "action": "room_left",
                "room_id": action.room_id,
                "user_id": action.user_id,
                "timestamp": chrono::Utc::now().timestamp()
            })
        }
        "create_room" => {
            // Process room creation
            serde_json::json!({
                "success": true,
                "action": "room_created",
                "room_id": action.room_id,
                "created_by": action.user_id,
                "timestamp": chrono::Utc::now().timestamp()
            })
        }
        _ => {
            serde_json::json!({
                "success": false,
                "error": format!("Unknown action type: {}", action.action_type)
            })
        }
    }
}

fn calculate_room_stats(room_id: &str) -> serde_json::Value {
    // In a real implementation, this would query the database
    serde_json::json!({
        "room_id": room_id,
        "total_messages": 42,
        "active_users": 5,
        "last_activity": chrono::Utc::now().timestamp(),
        "stats_generated_at": chrono::Utc::now().timestamp()
    })
}

fn sanitize_message_content(content: &str) -> String {
    // Basic sanitization - remove/replace potentially harmful content
    content
        .replace("<script", "&lt;script")
        .replace("</script>", "&lt;/script&gt;")
        .replace("<iframe", "&lt;iframe")
        .replace("javascript:", "")
        .trim()
        .to_string()
}

fn sanitize_text(text: &str) -> String {
    // Basic text sanitization
    text
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace() || ".,!?-_()[]{}".contains(*c))
        .collect::<String>()
        .trim()
        .to_string()
}

// Helper function to create response
fn create_response<T: Serialize>(data: T) -> *mut u8 {
    let json = serde_json::to_string(&data).unwrap_or_else(|_| "{}".to_string());
    let bytes = json.into_bytes();
    let len = bytes.len();
    
    let ptr = unsafe {
        let layout = std::alloc::Layout::from_size_align(len + 4, 1).unwrap();
        let ptr = std::alloc::alloc(layout) as *mut u32;
        *ptr = len as u32;
        let data_ptr = ptr.add(1) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, len);
        ptr as *mut u8
    };
    
    ptr
}

#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut u8 {
    let layout = std::alloc::Layout::from_size_align(size, 1).unwrap();
    unsafe { std::alloc::alloc(layout) }
}

#[no_mangle]
pub extern "C" fn deallocate(ptr: *mut u8, size: usize) {
    let layout = std::alloc::Layout::from_size_align(size, 1).unwrap();
    unsafe { std::alloc::dealloc(ptr, layout) }
}