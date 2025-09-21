/*
Password Security Tests with bcrypt
* Verify secure hashing, password validation and advanced user management
*/

use std::sync::Arc;
use mini_db_server::security::{PolicyEngine, TriggerSystem, SecureQueryExecutor};
use mini_db_server::query::QueryExecutor;
use tempfile::TempDir;
use serial_test::serial;

#[test]
#[serial]
fn test_password_hashing_security() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_password_hashing.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing bcrypt password hashing
    
    // Test 1: Create user with strong password
    let result = secure_executor.create_user(
        "testuser",
        "test@example.com",
        "StrongPassword123!",
        vec!["user".to_string()]
    );
    assert!(result.is_ok());
    // User created with strong password
    
    // Test 2: Authentication with correct password
    let auth_result = secure_executor.login("testuser", "StrongPassword123!");
    assert!(auth_result.is_ok());
    // Authentication successful with correct password
    
    secure_executor.logout().unwrap();
    
    // Test 3: Authentication with wrong password should fail
    let wrong_auth = secure_executor.login("testuser", "WrongPassword123!");
    assert!(wrong_auth.is_err());
    // Authentication correctly failed with wrong password
    
    // Test 4: Try to create user with weak password
    let weak_password_result = secure_executor.create_user(
        "weakuser",
        "weak@example.com",
        "123",  // Too short
        vec!["user".to_string()]
    );
    assert!(weak_password_result.is_err());
    // User creation correctly failed with weak password
    
    // Test 5: Try to create user with common password
    let common_password_result = secure_executor.create_user(
        "commonuser",
        "common@example.com",
        "password",  // Too common
        vec!["user".to_string()]
    );
    assert!(common_password_result.is_err());
    // User creation correctly failed with common password
}

#[test]
#[serial]
fn test_password_change_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_password_change.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing password change functionality
    
    // Create user
    let user_id = secure_executor.create_user(
        "changeuser",
        "change@example.com",
        "OldPassword123!",
        vec!["user".to_string()]
    ).unwrap();
    
    // Login to get session
    secure_executor.login("changeuser", "OldPassword123!").unwrap();
    
    // Test 1: Change password successfully
    let change_result = secure_executor.change_password(
        &user_id,  // * FIXED: Added user_id parameter
        "OldPassword123!",
        "NewPassword123!"
    );
    assert!(change_result.is_ok());
    // Password changed successfully
    
    // Test 2: Old password should not work anymore
    secure_executor.logout().unwrap();
    let old_auth = secure_executor.login("changeuser", "OldPassword123!");
    assert!(old_auth.is_err());
    // Old password correctly rejected
    
    // Test 3: New password should work
    let new_auth = secure_executor.login("changeuser", "NewPassword123!");
    assert!(new_auth.is_ok());
    // New password works correctly
    
    // Test 4: Try to change to same password (should fail)
    let same_password_result = secure_executor.change_password(
        &user_id,  // * FIXED: Added user_id parameter
        "NewPassword123!",
        "NewPassword123!"
    );
    assert!(same_password_result.is_err());
    // Correctly prevented password reuse
    
    // Test 5: Try to change with wrong old password
    let wrong_old_result = secure_executor.change_password(
        &user_id,  // * FIXED: Added user_id parameter
        "WrongOldPassword",
        "AnotherNewPassword123!"
    );
    assert!(wrong_old_result.is_err());
    // Correctly rejected wrong old password
}

#[test]
#[serial]
fn test_admin_password_reset() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_password_reset.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing admin password reset
    
    // Create admin and regular user
    secure_executor.create_admin_user("admin", "admin@example.com", "AdminPass123!").unwrap();
    let user_id = secure_executor.create_user(
        "resetuser",
        "reset@example.com",
        "UserPass123!",
        vec!["user".to_string()]
    ).unwrap();
    
    // Login as admin
    secure_executor.login("admin", "AdminPass123!").unwrap();
    
    // Test 1: Admin resets user password
    let reset_result = secure_executor.reset_password(&user_id, "NewResetPass123!");
    assert!(reset_result.is_ok());
    // Admin successfully reset user password
    
    // Test 2: Old password should not work
    secure_executor.logout().unwrap();
    let old_auth = secure_executor.login("resetuser", "UserPass123!");
    assert!(old_auth.is_err());
    // Old password correctly rejected after reset
    
    // Test 3: New password should work
    let new_auth = secure_executor.login("resetuser", "NewResetPass123!");
    assert!(new_auth.is_ok());
    // New password works after reset
    
    // Test 4: Regular user cannot reset passwords
    let user_reset_result = secure_executor.reset_password(&user_id, "AnotherPass123!");
    assert!(user_reset_result.is_err());
    // Regular user correctly blocked from resetting passwords
}

#[test]
#[serial]
fn test_temporary_password_generation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_temp_password.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing temporary password generation
    
    // Create admin and user
    secure_executor.create_admin_user("admin", "admin@example.com", "AdminPass123!").unwrap();
    let user_id = secure_executor.create_user(
        "tempuser",
        "temp@example.com",
        "UserPass123!",
        vec!["user".to_string()]
    ).unwrap();
    
    // Login as admin
    secure_executor.login("admin", "AdminPass123!").unwrap();
    
    // Test 1: Generate temporary password
    let temp_password_result = secure_executor.generate_and_set_temporary_password(&user_id);
    assert!(temp_password_result.is_ok());
    
    let temp_password = temp_password_result.unwrap();
    // Temporary password generated
    
    // Test 2: Temporary password should be strong
    assert!(temp_password.len() >= 16);
    assert!(temp_password.chars().any(|c| c.is_uppercase()));
    assert!(temp_password.chars().any(|c| c.is_lowercase()));
    assert!(temp_password.chars().any(|c| c.is_numeric()));
    assert!(temp_password.chars().any(|c| "!@#$%^&*".contains(c)));
    // Temporary password meets strength requirements
    
    // Test 3: Old password should not work
    secure_executor.logout().unwrap();
    let old_auth = secure_executor.login("tempuser", "UserPass123!");
    assert!(old_auth.is_err());
    println!("   * Old password correctly rejected");
    
    // Test 4: Temporary password should work
    let temp_auth = secure_executor.login("tempuser", &temp_password);
    assert!(temp_auth.is_ok());
    // Temporary password works correctly
}

#[test]
#[serial]
fn test_user_lock_unlock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_user_lock.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing user lock/unlock functionality
    
    // Create admin and user
    secure_executor.create_admin_user("admin", "admin@example.com", "AdminPass123!").unwrap();
    let user_id = secure_executor.create_user(
        "lockuser",
        "lock@example.com",
        "UserPass123!",
        vec!["user".to_string()]
    ).unwrap();
    
    // Test 1: User can initially login
    let initial_auth = secure_executor.login("lockuser", "UserPass123!");
    assert!(initial_auth.is_ok());
    // User can initially login
    secure_executor.logout().unwrap();
    
    // Test 2: Admin locks user
    secure_executor.login("admin", "AdminPass123!").unwrap();
    let lock_result = secure_executor.lock_user(&user_id);
    assert!(lock_result.is_ok());
    // Admin successfully locked user
    
    // Test 3: Locked user cannot login
    secure_executor.logout().unwrap();
    let locked_auth = secure_executor.login("lockuser", "UserPass123!");
    assert!(locked_auth.is_err());
    // Locked user correctly cannot login
    
    // Test 4: Admin unlocks user
    secure_executor.login("admin", "AdminPass123!").unwrap();
    let unlock_result = secure_executor.unlock_user(&user_id);
    assert!(unlock_result.is_ok());
    // Admin successfully unlocked user
    
    // Test 5: Unlocked user can login again
    secure_executor.logout().unwrap();
    let unlocked_auth = secure_executor.login("lockuser", "UserPass123!");
    assert!(unlocked_auth.is_ok());
    // Unlocked user can login again
}

#[test]
#[serial]
fn test_session_invalidation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_session_invalidation.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing session invalidation
    
    // Create admin and user
    secure_executor.create_admin_user("admin", "admin@example.com", "AdminPass123!").unwrap();
    let user_id = secure_executor.create_user(
        "sessionuser",
        "session@example.com",
        "UserPass123!",
        vec!["user".to_string()]
    ).unwrap();
    
    // Test 1: User logs in successfully
    let session_id = secure_executor.login("sessionuser", "UserPass123!").unwrap();
    assert!(!session_id.is_empty());
    // User logged in with session
    
    // Test 2: Admin invalidates user sessions
    secure_executor.logout().unwrap();
    secure_executor.login("admin", "AdminPass123!").unwrap();
    
    let invalidate_result = secure_executor.invalidate_sessions(&user_id);
    assert!(invalidate_result.is_ok());
    // Admin successfully invalidated user sessions
    
    // Test 3: User needs to login again (session should be invalid)
    secure_executor.logout().unwrap();
    let new_session = secure_executor.login("sessionuser", "UserPass123!");
    assert!(new_session.is_ok());
    // User can create new session after invalidation
}

#[test]
#[serial]
fn test_password_policy_validation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_password_policy.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing password policy validation
    
    // Test password strength requirements
    let test_cases = vec![
        ("123", false, "too short"),
        ("password", false, "too simple"),
        ("PASSWORD123", false, "no special chars"),
        ("password123!", false, "no uppercase"),
        ("PASSWORD123!", false, "no lowercase"),
        ("Password!", false, "no numbers"),
        ("Password123!", true, "meets all requirements"),
        ("MySecurePass456#", true, "strong password"),
        ("VeryLongPasswordThatExceeds128CharactersAndShouldFailValidationBecauseItIsTooLongForOurSecurityPolicy123456789012345678901234567890", false, "too long"),
    ];
    
    for (password, should_succeed, description) in test_cases {
        let result = secure_executor.create_user(
            &format!("user_{}", password.len()),
            &format!("test{}@example.com", password.len()),
            password,
            vec!["user".to_string()]
        );
        
        if should_succeed {
            assert!(result.is_ok(), "Password '{}' should succeed ({})", password, description);
            // Password correctly accepted
        } else {
            assert!(result.is_err(), "Password '{}' should fail ({})", password, description);
            // Password correctly rejected
        }
    }
}

#[test]
#[serial]
fn test_security_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_security_stats.db");
    let db = Arc::new(sled::open(db_path).unwrap());
    
    let query_executor = QueryExecutor::new(Arc::clone(&db), 100, 60);
    let policy_engine = Arc::new(PolicyEngine::new(Arc::clone(&db)));
    let trigger_system = Arc::new(TriggerSystem::new(Arc::clone(&db)));
    
    let secure_executor = SecureQueryExecutor::new(
        query_executor,
        policy_engine,
        trigger_system,
    );
    
    // Testing security statistics
    
    // Create users
    secure_executor.create_admin_user("admin", "admin@example.com", "AdminPass123!").unwrap();
    let user1_id = secure_executor.create_user("user1", "user1@example.com", "UserPass123!", vec!["user".to_string()]).unwrap();
    let _user2_id = secure_executor.create_user("user2", "user2@example.com", "UserPass456!", vec!["user".to_string()]).unwrap();
    
    // Login as admin
    secure_executor.login("admin", "AdminPass123!").unwrap();
    
    // Lock one user
    secure_executor.lock_user(&user1_id).unwrap();
    
    // Get security statistics
    let stats_result = secure_executor.get_security_stats();
    assert!(stats_result.is_ok());
    
    let stats = stats_result.unwrap();
    assert_eq!(stats.total_users, 3); // admin + 2 users
    assert_eq!(stats.strong_passwords, 3); // All passwords are strong with bcrypt
    assert_eq!(stats.weak_passwords, 0); // No weak passwords allowed
    assert_eq!(stats.locked_users, 1); // user1 is locked
    
    // Security statistics verified
}