use wasmtime::{Engine, Module, Store, Instance};

#[test]
fn test_wasm_execution_integer() {
    let wasm_code = br#"
    (module
        (func $add (param $x i32) (param $y i32) (result i32)
            local.get $x
            local.get $y
            i32.add
        )
        (export "add_numbers" (func $add))
    )
    "#;

    let engine = Engine::default();
    let module = Module::new(&engine, wasm_code).expect("Errore nella compilazione WASM");
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[]).expect("Errore nell'istanza WASM");

    let add_func = instance
        .get_typed_func::<(i32, i32), i32>(&mut store, "add_numbers")
        .expect("Funzione non trovata");

    let result = add_func.call(&mut store, (2, 3)).expect("Errore nell'esecuzione WASM");
    assert_eq!(result, 5);
}

#[test]
fn test_wasm_execution_float() {
    let wasm_code = br#"
    (module
        (func $multiply (param $x f32) (param $y f32) (result f32)
            local.get $x
            local.get $y
            f32.mul
        )
        (export "multiply_floats" (func $multiply))
    )
    "#;

    let engine = Engine::default();
    let module = Module::new(&engine, wasm_code).expect("Errore nella compilazione WASM");
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[]).expect("Errore nell'istanza WASM");

    let multiply_func = instance
        .get_typed_func::<(f32, f32), f32>(&mut store, "multiply_floats")
        .expect("Funzione non trovata");

    let result = multiply_func.call(&mut store, (2.5, 4.0)).expect("Errore nell'esecuzione WASM");
    assert_eq!(result, 10.0);
}

#[test]
fn test_wasm_execution_boolean() {
    let wasm_code = br#"
    (module
        (func $is_greater (param $a i32) (param $b i32) (result i32)
            local.get $a
            local.get $b
            i32.gt_s
        )
        (export "is_greater" (func $is_greater))
    )
    "#;

    let engine = Engine::default();
    let module = Module::new(&engine, wasm_code).expect("Errore nella compilazione WASM");
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[]).expect("Errore nell'istanza WASM");

    let is_greater_func = instance
        .get_typed_func::<(i32, i32), i32>(&mut store, "is_greater")
        .expect("Funzione non trovata");

    let result = is_greater_func.call(&mut store, (10, 5)).expect("Errore nell'esecuzione WASM");
    assert_eq!(result, 1);
}

#[test]
fn test_wasm_execution_string() {
    let wasm_code = br#"
    (module
        (memory (export "memory") 1)
        (data (i32.const 0) "Hello, WASM!")
        (func $get_string (result i32)
            i32.const 0
        )
        (export "get_string" (func $get_string))
    )
    "#;

    let engine = Engine::default();
    let module = Module::new(&engine, wasm_code).expect("Errore nella compilazione WASM");
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[]).expect("Errore nell'istanza WASM");

    let memory = instance.get_memory(&mut store, "memory").expect("Memoria non trovata");
    let get_string_func = instance
        .get_typed_func::<(), i32>(&mut store, "get_string")
        .expect("Funzione non trovata");

    let string_ptr = get_string_func.call(&mut store, ()).expect("Errore nell'esecuzione WASM") as usize;
    let mem = memory.data(&store);
    
    let result_string = std::str::from_utf8(&mem[string_ptr..string_ptr + 12]).expect("Errore nella lettura della stringa");
    
    assert_eq!(result_string, "Hello, WASM!");
}
