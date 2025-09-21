use mini_db_server::sync::SyncServer;
use tokio_tungstenite::connect_async;
use tokio::net::TcpListener;
use url::Url;
use std::time::Duration;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message;
use tokio::sync::OnceCell;

static TEST_SERVER_URL: OnceCell<String> = OnceCell::const_new();

async fn get_test_server_url() -> &'static str {
    TEST_SERVER_URL
        .get_or_init(|| async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = format!("ws://{}", listener.local_addr().unwrap());

            tokio::spawn(async move {
                let server = SyncServer::new("test_db", 100, 60);
                server.start_with_listener(listener).await;
            });

            tokio::time::sleep(Duration::from_millis(500)).await;
            addr
        })
        .await
}

#[tokio::test]
async fn test_sync_insert() {
    let server_url = get_test_server_url().await;
    let (ws_stream, _) = connect_async(Url::parse(server_url).unwrap()).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // * Iscrizione alla tabella "users" prima di inviare la query
    println!("📡 Iscrizione alla tabella users...");
    write.send(Message::Text("SUBSCRIBE users".to_string())).await.unwrap();

    // * Attesa della conferma dell'iscrizione
    if let Some(Ok(msg)) = read.next().await {
        assert!(msg.to_string().contains("ACK: SUBSCRIBE users"));
    } else {
        panic!("❌ Nessuna conferma di iscrizione ricevuta!");
    }

    // * Invio della query INSERT
    println!("📩 Invio INSERT...");
    write.send(Message::Text("INSERT INTO users (id, name) VALUES ('1', 'Alice')".to_string())).await.unwrap();

    // * Attesa della notifica di sincronizzazione
    if let Some(Ok(msg)) = read.next().await {
        let response = msg.to_string();
        println!("* Risposta ricevuta: {:?}", response); // Aggiunto per debug
        assert!(response.contains("Inserito record in users"));
    } else {
        panic!("❌ Nessuna risposta ricevuta!");
    }
    
}

#[tokio::test]
async fn test_sync_update() {
    let server_url = get_test_server_url().await;
    let (ws_stream, _) = connect_async(Url::parse(server_url).unwrap()).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // * Iscrizione alla tabella "users"
    println!("📡 Iscrizione alla tabella users...");
    write.send(Message::Text("SUBSCRIBE users".to_string())).await.unwrap();

    if let Some(Ok(msg)) = read.next().await {
        assert!(msg.to_string().contains("ACK: SUBSCRIBE users"));
    } else {
        panic!("❌ Nessuna conferma di iscrizione ricevuta!");
    }

    // * Invio della query UPDATE
    println!("📩 Invio UPDATE...");
    write.send(Message::Text("UPDATE users SET name = 'Bob' WHERE id = '1'".to_string())).await.unwrap();

    if let Some(Ok(msg)) = read.next().await {
        let response = msg.to_string();
        println!("* Risposta ricevuta: {:?}", response); // Aggiunto per debug
        assert!(response.contains("Aggiornati record in users"));
    } else {
        panic!("❌ Nessuna risposta ricevuta!");
    }
    
}

#[tokio::test]
async fn test_sync_delete() {
    let server_url = get_test_server_url().await;
    let (ws_stream, _) = connect_async(Url::parse(server_url).unwrap()).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // * Iscrizione alla tabella "users"
    println!("📡 Iscrizione alla tabella users...");
    write.send(Message::Text("SUBSCRIBE users".to_string())).await.unwrap();

    if let Some(Ok(msg)) = read.next().await {
        assert!(msg.to_string().contains("ACK: SUBSCRIBE users"));
    } else {
        panic!("❌ Nessuna conferma di iscrizione ricevuta!");
    }

    // * Invio della query DELETE
    println!("📩 Invio DELETE...");
    write.send(Message::Text("DELETE FROM users WHERE id = '1'".to_string())).await.unwrap();

    if let Some(Ok(msg)) = read.next().await {
        let response = msg.to_string();
        println!("* Risposta ricevuta: {}", response);
        assert!(response.contains("Cancellati record in users"));
    } else {
        panic!("❌ Nessuna risposta ricevuta!");
    }
}
