/*
WASM Engine - Mini-DB Modular Architecture
==========================================
Loads and manages WASM modules for external business logic
Provides secure sandbox for TicTacToe and other game modules
Immutable architecture: server core + external modules
WASM Memory Interface - Zero-copy data access optimization
*/

use wasmtime::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::Path;
use anyhow::Result;
use serde::{Serialize, Deserialize};

// ================================
// WASM Memory Interface Structures
// ================================

/// Memory layout for efficient server-WASM communication
#[repr(C)]
#[derive(Debug, Clone)]
pub struct WasmMemoryLayout {
    /// Input data buffer pointer (server -> WASM)
    pub input_ptr: u32,
    /// Input data length
    pub input_len: u32,
    /// Output data buffer pointer (WASM -> server)
    pub output_ptr: u32,
    /// Output data length
    pub output_len: u32,
    /// Error code (0 = success, non-zero = error)
    pub error_code: u32,
    /// Reserved for future use
    pub reserved: u32,
}

/// Efficient data serialization for WASM memory interface
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WasmDataPacket {
    /// Operation type (validate, process, etc.)
    pub operation: String,
    /// Target table name
    pub table: String,
    /// Row data as key-value pairs
    pub data: HashMap<String, String>,
    /// Additional metadata
    pub metadata: Option<HashMap<String, String>>,
}

const WASM_MEMORY_LAYOUT_SIZE: u32 = 24; // 6 * 4 bytes

/// WASM engine for managing business logic modules
pub struct WasmEngine {
    engine: Engine,
    modules: Arc<Mutex<HashMap<String, WasmModuleInstance>>>,
}

/// Loaded WASM module instance with memory interface optimizations
struct WasmModuleInstance {
    store: Store<()>,
    instance: Instance,
    memory: Option<Memory>,
    /// Pointer to memory layout in WASM linear memory
    layout_ptr: Option<u32>,
    /// Buffer allocator function in WASM module
    alloc_func: Option<TypedFunc<u32, u32>>,
    /// Buffer deallocator function in WASM module
    dealloc_func: Option<TypedFunc<(u32, u32), ()>>,
}

impl WasmEngine {
    /// Create a new WASM engine
    pub fn new() -> Result<Self> {
        let engine = Engine::default();
        
        Ok(Self {
            engine,
            modules: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Load a WASM module from file
    pub fn load_module(&self, module_name: &str, wasm_path: &str) -> Result<()> {
        if !Path::new(wasm_path).exists() {
            return Err(anyhow::anyhow!("WASM file not found: {}", wasm_path));
        }

        // Read the WASM file
        let wasm_bytes = std::fs::read(wasm_path)?;
        
        // Compile the module
        let module = Module::new(&self.engine, &wasm_bytes)?;
        
        // Create store and instance
        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &module, &[])?;
        
        // Get memory (if available)
        let memory = instance.get_memory(&mut store, "memory");
        
        // Setup memory interface functions (optional for compatibility)
        let alloc_func = instance.get_typed_func::<u32, u32>(&mut store, "wasm_alloc").ok();
        let dealloc_func = instance.get_typed_func::<(u32, u32), ()>(&mut store, "wasm_dealloc").ok();
        
        // Allocate layout in WASM memory if available
        let layout_ptr = if let (Some(memory), Some(alloc)) = (&memory, &alloc_func) {
            match alloc.call(&mut store, WASM_MEMORY_LAYOUT_SIZE) {
                Ok(ptr) => Some(ptr),
                Err(_) => None
            }
        } else {
            None
        };
        
        // Store the instance
        let wasm_instance = WasmModuleInstance {
            store,
            instance,
            memory,
            layout_ptr,
            alloc_func,
            dealloc_func,
        };
        
        self.modules.lock().unwrap().insert(module_name.to_string(), wasm_instance);
        
        Ok(())
    }

    /// Register a WASM module from bytes (legacy method)
    pub fn register_module(&self, name: &str, wasm_bytes: &[u8]) -> Result<(), String> {
        let module = Module::new(&self.engine, wasm_bytes).map_err(|e| e.to_string())?;
        
        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &module, &[]).map_err(|e| e.to_string())?;
        let memory = instance.get_memory(&mut store, "memory");
        
        let wasm_instance = WasmModuleInstance {
            store,
            instance,
            memory,
            layout_ptr: None,
            alloc_func: None,
            dealloc_func: None,
        };
        
        self.modules.lock().unwrap().insert(name.to_string(), wasm_instance);
        Ok(())
    }

    /// Call a WASM function with JSON parameters (legacy method)
    pub fn call_function(&self, module_name: &str, function_name: &str, args: &[String]) -> Result<String> {
        let mut modules = self.modules.lock().unwrap();
        let wasm_instance = modules.get_mut(module_name)
            .ok_or_else(|| anyhow::anyhow!("Module '{}' not found", module_name))?;
        
        // For now we only implement functions without parameters
        // TODO: Implement parameter passing via WASM memory
        let func = wasm_instance.instance
            .get_typed_func::<(), i32>(&mut wasm_instance.store, function_name)
            .map_err(|_| anyhow::anyhow!("Function '{}' not found in module '{}'", function_name, module_name))?;
        
        let result = func.call(&mut wasm_instance.store, ())?;
        
        // TODO: Implement reading result from WASM memory
        Ok(format!("{{\"wasm_result\": {}}}", result))
    }

    /// Optimized WASM function call using memory interface
    pub fn call_function_optimized(&self, module_name: &str, function_name: &str, data_packet: &WasmDataPacket) -> Result<String> {
        let mut modules = self.modules.lock().unwrap();
        let wasm_instance = modules.get_mut(module_name)
            .ok_or_else(|| anyhow::anyhow!("Module '{}' not found", module_name))?;
        
        // Check if memory interface is available
        let has_memory_interface = wasm_instance.memory.is_some() && 
                                   wasm_instance.layout_ptr.is_some() && 
                                   wasm_instance.alloc_func.is_some();
        
        if has_memory_interface {
            // Use optimized memory interface
            self.call_via_memory_interface(wasm_instance, function_name, data_packet)
        } else {
            // Fallback to JSON-based communication
            self.call_via_json_interface(wasm_instance, function_name, data_packet)
        }
    }

    /// Call WASM function via direct memory interface (zero-copy)
    fn call_via_memory_interface(
        &self,
        wasm_instance: &mut WasmModuleInstance,
        function_name: &str,
        data_packet: &WasmDataPacket
    ) -> Result<String> {
        // Extract necessary references safely
        let memory = wasm_instance.memory.as_ref().unwrap();
        let layout_ptr = wasm_instance.layout_ptr.unwrap();
        let alloc_func = wasm_instance.alloc_func.as_ref().unwrap();
        
        // Serialize input data to bytes
        let input_data = bincode::serialize(data_packet)
            .map_err(|e| anyhow::anyhow!("Failed to serialize input data: {}", e))?;
        
        // Allocate buffer in WASM memory for input
        let input_ptr = alloc_func.call(&mut wasm_instance.store, input_data.len() as u32)?;
        
        // Write input data to WASM memory
        let memory_data = memory.data_mut(&mut wasm_instance.store);
        memory_data[input_ptr as usize..(input_ptr as usize + input_data.len())]
            .copy_from_slice(&input_data);
        
        // Setup memory layout
        let layout = WasmMemoryLayout {
            input_ptr,
            input_len: input_data.len() as u32,
            output_ptr: 0,
            output_len: 0,
            error_code: 0,
            reserved: 0,
        };
        
        // Write layout to WASM memory
        let layout_bytes = unsafe {
            std::slice::from_raw_parts(
                &layout as *const WasmMemoryLayout as *const u8,
                WASM_MEMORY_LAYOUT_SIZE as usize
            )
        };
        memory_data[layout_ptr as usize..(layout_ptr as usize + WASM_MEMORY_LAYOUT_SIZE as usize)]
            .copy_from_slice(layout_bytes);
        
        // Call WASM function with layout pointer
        let func = wasm_instance.instance
            .get_typed_func::<u32, i32>(&mut wasm_instance.store, function_name)
            .map_err(|_| anyhow::anyhow!("Function '{}' not found", function_name))?;
        
        let wasm_result = func.call(&mut wasm_instance.store, layout_ptr)?;
        
        // Read updated layout from WASM memory
        let memory_data = memory.data(&wasm_instance.store);
        let updated_layout: WasmMemoryLayout = unsafe {
            std::ptr::read(
                memory_data[layout_ptr as usize..].as_ptr() as *const WasmMemoryLayout
            )
        };
        
        // Check for errors
        if updated_layout.error_code != 0 {
            return Err(anyhow::anyhow!("WASM function returned error code: {}", updated_layout.error_code));
        }
        
        // Read output data if available
        if updated_layout.output_len > 0 && updated_layout.output_ptr > 0 {
            let output_data = &memory_data[updated_layout.output_ptr as usize..(updated_layout.output_ptr as usize + updated_layout.output_len as usize)];
            
            // Try to deserialize as string first, fallback to JSON
            match std::str::from_utf8(output_data) {
                Ok(result_str) => Ok(result_str.to_string()),
                Err(_) => {
                    // Try to deserialize as structured data
                    match bincode::deserialize::<serde_json::Value>(output_data) {
                        Ok(json_value) => Ok(json_value.to_string()),
                        Err(_) => Ok(format!("{{\"wasm_result\": {}, \"raw_bytes\": {}}}", wasm_result, output_data.len()))
                    }
                }
            }
        } else {
            Ok(format!("{{\"wasm_result\": {}}}", wasm_result))
        }
    }

    /// Call WASM function via JSON interface (legacy fallback)
    fn call_via_json_interface(
        &self,
        wasm_instance: &mut WasmModuleInstance,
        function_name: &str,
        _data_packet: &WasmDataPacket
    ) -> Result<String> {
        // Legacy JSON-based call (simplified for compatibility)
        let func = wasm_instance.instance
            .get_typed_func::<(), i32>(&mut wasm_instance.store, function_name)
            .map_err(|_| anyhow::anyhow!("Function '{}' not found", function_name))?;
        
        let result = func.call(&mut wasm_instance.store, ())?;
        Ok(format!("{{\"wasm_result\": {}, \"interface\": \"json_fallback\"}}", result))
    }

    /// Execute an exported function from a WASM module (legacy method)
    pub fn execute_function(&mut self, module_name: &str, function_name: &str, param: i32) -> Result<i32, String> {
        let mut modules = self.modules.lock().unwrap();
        let wasm_instance = modules.get_mut(module_name)
            .ok_or_else(|| "Module not found".to_string())?;

        let function = wasm_instance.instance.get_typed_func::<i32, i32>(&mut wasm_instance.store, function_name)
            .map_err(|_| "Function not found".to_string())?;

        function.call(&mut wasm_instance.store, param).map_err(|e| e.to_string())
    }

    /// List all loaded modules
    pub fn list_modules(&self) -> Vec<String> {
        self.modules.lock().unwrap().keys().cloned().collect()
    }

    /// Check if a module is loaded
    pub fn is_module_loaded(&self, module_name: &str) -> bool {
        self.modules.lock().unwrap().contains_key(module_name)
    }

    /// Unload a module
    pub fn unload_module(&self, module_name: &str) -> Result<()> {
        let mut modules = self.modules.lock().unwrap();
        if modules.remove(module_name).is_some() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Module '{}' not found", module_name))
        }
    }
}

// Generic WASM engine - no game-specific code
// All game logic (TicTacToe, Chat, etc.) belongs in external modules