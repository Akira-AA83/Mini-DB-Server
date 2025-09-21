# WASM Modules Directory

This directory contains compiled WASM modules. 

To build modules from source:

```bash
# Build TicTacToe module
cd user_modules/tictactoe
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/tictactoe.wasm ../../modules/

# Build Chat module  
cd ../chat
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/chat.wasm ../../modules/
```

The server will automatically load .wasm files from this directory.

