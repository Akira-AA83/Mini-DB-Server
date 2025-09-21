/*
TicTacToe WASM Module
====================
WASM module containing all business logic for TicTacToe
Executed server-side to prevent cheating and ensure consistency
WASM sandbox for security
*/

use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};

// ================================
// Game State Structures
// ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameState {
    pub board: [u8; 9],         // 0=empty, 1=X, 2=O
    pub current_player: u8,     // 1=X, 2=O
    pub game_status: GameStatus,
    pub winner: Option<u8>,     // None, Some(1)=X, Some(2)=O
    pub move_count: u8,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum GameStatus {
    Waiting = 0,    // Waiting for players
    Playing = 1,    // Game in progress
    Won = 2,        // Someone won
    Draw = 3,       // Draw
    Abandoned = 4,  // Partita abbandonata
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveRequest {
    pub player: u8,      // 1=X, 2=O
    pub position: u8,    // 0-8 (grid position)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveResult {
    pub valid: bool,
    pub new_state: Option<GameState>,
    pub error_message: Option<String>,
}

// ================================
// Constants & Configuration
// ================================

const EMPTY: u8 = 0;
const PLAYER_X: u8 = 1;
const PLAYER_O: u8 = 2;

// Win patterns for TicTacToe (rows, columns, diagonals)
const WIN_PATTERNS: [[usize; 3]; 8] = [
    // Rows
    [0, 1, 2],
    [3, 4, 5],
    [6, 7, 8],
    // Columns
    [0, 3, 6],
    [1, 4, 7],
    [2, 5, 8],
    // Diagonals
    [0, 4, 8],
    [2, 4, 6],
];

// ================================
// WASM Interface Functions
// ================================

/// Initialize panic hook for better error messages
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

/// Create a new game state
#[wasm_bindgen]
pub fn create_new_game() -> String {
    let game_state = GameState {
        board: [EMPTY; 9],
        current_player: PLAYER_X,
        game_status: GameStatus::Waiting,
        winner: None,
        move_count: 0,
    };
    
    serde_json::to_string(&game_state).unwrap_or_else(|_| "{}".to_string())
}

/// Start a game (when there are 2 players)
#[wasm_bindgen]
pub fn start_game(state_json: &str) -> String {
    let mut game_state: GameState = match serde_json::from_str(state_json) {
        Ok(state) => state,
        Err(_) => return create_error_result("Invalid game state JSON"),
    };
    
    if game_state.game_status == GameStatus::Waiting {
        game_state.game_status = GameStatus::Playing;
        game_state.current_player = PLAYER_X; // X inizia sempre
    }
    
    serde_json::to_string(&game_state).unwrap_or_else(|_| create_error_result("Serialization error"))
}

/// Validate and execute a move
#[wasm_bindgen]
pub fn make_move(state_json: &str, move_json: &str) -> String {
    // Parse input
    let mut game_state: GameState = match serde_json::from_str(state_json) {
        Ok(state) => state,
        Err(_) => return create_move_result(false, None, Some("Invalid game state JSON")),
    };
    
    let move_request: MoveRequest = match serde_json::from_str(move_json) {
        Ok(req) => req,
        Err(_) => return create_move_result(false, None, Some("Invalid move request JSON")),
    };
    
    // Valida la mossa
    match validate_move(&game_state, &move_request) {
        Ok(_) => {
            // Esegui la mossa
            execute_move(&mut game_state, &move_request);
            create_move_result(true, Some(game_state), None)
        }
        Err(error) => create_move_result(false, None, Some(&error)),
    }
}

/// Check only if a move is valid (without executing it)
#[wasm_bindgen]
pub fn validate_move_only(state_json: &str, move_json: &str) -> String {
    let game_state: GameState = match serde_json::from_str(state_json) {
        Ok(state) => state,
        Err(_) => return serde_json::to_string(&false).unwrap(),
    };
    
    let move_request: MoveRequest = match serde_json::from_str(move_json) {
        Ok(req) => req,
        Err(_) => return serde_json::to_string(&false).unwrap(),
    };
    
    let is_valid = validate_move(&game_state, &move_request).is_ok();
    serde_json::to_string(&is_valid).unwrap()
}

/// Get the next player
#[wasm_bindgen]
pub fn get_next_player(current_player: u8) -> u8 {
    match current_player {
        PLAYER_X => PLAYER_O,
        PLAYER_O => PLAYER_X,
        _ => PLAYER_X, // Default to X if invalid
    }
}

/// Check if the game is finished and who won
#[wasm_bindgen]
pub fn check_game_end(state_json: &str) -> String {
    let game_state: GameState = match serde_json::from_str(state_json) {
        Ok(state) => state,
        Err(_) => return serde_json::to_string(&GameStatus::Playing).unwrap(),
    };
    
    let status = determine_game_status(&game_state.board);
    serde_json::to_string(&status).unwrap()
}

/// Debug: Print the board state in readable format
#[wasm_bindgen]
pub fn debug_board(state_json: &str) -> String {
    let game_state: GameState = match serde_json::from_str(state_json) {
        Ok(state) => state,
        Err(_) => return "Invalid state".to_string(),
    };
    
    format_board_debug(&game_state.board)
}

// ================================
// Core Game Logic (Internal)
// ================================

/// Validate a move according to TicTacToe rules
fn validate_move(game_state: &GameState, move_request: &MoveRequest) -> Result<(), String> {
    // 1. Il gioco deve essere in corso
    if game_state.game_status != GameStatus::Playing {
        return Err(format!("Game is not active (status: {:?})", game_state.game_status));
    }
    
    // 2. Deve essere il turno del giocatore giusto
    if move_request.player != game_state.current_player {
        return Err(format!("Not your turn! Current player: {}", game_state.current_player));
    }
    
    // 3. La posizione deve essere valida (0-8)
    if move_request.position > 8 {
        return Err(format!("Invalid position: {}. Must be 0-8", move_request.position));
    }
    
    // 4. La cella deve essere vuota
    if game_state.board[move_request.position as usize] != EMPTY {
        return Err(format!("Position {} is already occupied", move_request.position));
    }
    
    // 5. Il giocatore deve essere valido
    if move_request.player != PLAYER_X && move_request.player != PLAYER_O {
        return Err(format!("Invalid player: {}. Must be 1 (X) or 2 (O)", move_request.player));
    }
    
    Ok(())
}

/// Execute a move and update game state
fn execute_move(game_state: &mut GameState, move_request: &MoveRequest) {
    // Piazza il simbolo nella posizione
    game_state.board[move_request.position as usize] = move_request.player;
    game_state.move_count += 1;
    
    // Controlla se il gioco è finito
    let new_status = determine_game_status(&game_state.board);
    game_state.game_status = new_status;
    
    // Se qualcuno ha vinto, imposta il vincitore
    if new_status == GameStatus::Won {
        game_state.winner = Some(move_request.player);
    }
    
    // Se il gioco continua, cambia turno
    if new_status == GameStatus::Playing {
        game_state.current_player = get_next_player(move_request.player);
    }
}

/// Determina lo stato del gioco basandosi sulla board
fn determine_game_status(board: &[u8; 9]) -> GameStatus {
    // Controlla vittorie
    for pattern in &WIN_PATTERNS {
        let [a, b, c] = *pattern;
        if board[a] != EMPTY && board[a] == board[b] && board[b] == board[c] {
            return GameStatus::Won;
        }
    }
    
    // Controlla pareggio (board piena)
    if board.iter().all(|&cell| cell != EMPTY) {
        return GameStatus::Draw;
    }
    
    // Gioco in corso
    GameStatus::Playing
}

/// Controlla se un giocatore ha vinto
fn check_winner(board: &[u8; 9]) -> Option<u8> {
    for pattern in &WIN_PATTERNS {
        let [a, b, c] = *pattern;
        if board[a] != EMPTY && board[a] == board[b] && board[b] == board[c] {
            return Some(board[a]);
        }
    }
    None
}

/// Controlla se la board è piena
fn is_board_full(board: &[u8; 9]) -> bool {
    board.iter().all(|&cell| cell != EMPTY)
}

// ================================
// Helper Functions
// ================================

/// Crea un risultato di errore
fn create_error_result(message: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "error": message
    })).unwrap_or_else(|_| r#"{"error": "Unknown error"}"#.to_string())
}

/// Crea un risultato di mossa
fn create_move_result(valid: bool, state: Option<GameState>, error: Option<&str>) -> String {
    let result = MoveResult {
        valid,
        new_state: state,
        error_message: error.map(|s| s.to_string()),
    };
    
    serde_json::to_string(&result).unwrap_or_else(|_| {
        r#"{"valid": false, "error_message": "Serialization error"}"#.to_string()
    })
}

/// Formatta la board per debug
fn format_board_debug(board: &[u8; 9]) -> String {
    let symbols = [" ", "X", "O"];
    format!(
        " {} | {} | {} \n-----------\n {} | {} | {} \n-----------\n {} | {} | {} ",
        symbols[board[0] as usize], symbols[board[1] as usize], symbols[board[2] as usize],
        symbols[board[3] as usize], symbols[board[4] as usize], symbols[board[5] as usize],
        symbols[board[6] as usize], symbols[board[7] as usize], symbols[board[8] as usize]
    )
}

// ================================
// Tests (Rust-side)
// ================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new_game() {
        let game_json = create_new_game();
        let game_state: GameState = serde_json::from_str(&game_json).unwrap();
        
        assert_eq!(game_state.board, [EMPTY; 9]);
        assert_eq!(game_state.current_player, PLAYER_X);
        assert_eq!(game_state.game_status, GameStatus::Waiting);
        assert_eq!(game_state.winner, None);
        assert_eq!(game_state.move_count, 0);
    }

    #[test]
    fn test_validate_move_logic() {
        let mut game_state = GameState {
            board: [EMPTY; 9],
            current_player: PLAYER_X,
            game_status: GameStatus::Playing,
            winner: None,
            move_count: 0,
        };

        let valid_move = MoveRequest {
            player: PLAYER_X,
            position: 4, // Centro
        };

        // Mossa valida
        assert!(validate_move(&game_state, &valid_move).is_ok());

        // Occupa la posizione
        game_state.board[4] = PLAYER_X;

        // Mossa sulla stessa posizione deve fallire
        assert!(validate_move(&game_state, &valid_move).is_err());

        // Turno sbagliato
        let wrong_turn = MoveRequest {
            player: PLAYER_X, // Still X, but it should be O's turn
            position: 0,
        };
        game_state.current_player = PLAYER_O;
        assert!(validate_move(&game_state, &wrong_turn).is_err());
    }

    #[test]
    fn test_win_detection() {
        let winning_board = [
            PLAYER_X, PLAYER_X, PLAYER_X,  // Riga vincente
            PLAYER_O, PLAYER_O, EMPTY,
            EMPTY, EMPTY, EMPTY
        ];

        assert_eq!(determine_game_status(&winning_board), GameStatus::Won);
        assert_eq!(check_winner(&winning_board), Some(PLAYER_X));
    }

    #[test]
    fn test_draw_detection() {
        let draw_board = [
            PLAYER_X, PLAYER_O, PLAYER_X,
            PLAYER_O, PLAYER_O, PLAYER_X,
            PLAYER_O, PLAYER_X, PLAYER_O
        ];

        assert_eq!(determine_game_status(&draw_board), GameStatus::Draw);
        assert_eq!(check_winner(&draw_board), None);
        assert!(is_board_full(&draw_board));
    }

    #[test]
    fn test_next_player() {
        assert_eq!(get_next_player(PLAYER_X), PLAYER_O);
        assert_eq!(get_next_player(PLAYER_O), PLAYER_X);
        assert_eq!(get_next_player(99), PLAYER_X); // Invalid -> default to X
    }
}