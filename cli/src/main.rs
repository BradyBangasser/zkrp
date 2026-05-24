use libghost::behavior::ClientBehavior;
use libghost::context::ZRPContext;
use libghost::handler::{EventHandler, ZRPEvent};
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tracing::info;

// ── Auth server response shape ────────────────────────────────────────────────
// Replace with your actual auth server response type
#[allow(dead_code)]
struct RelayInfo {
    addr: String, // e.g. "/ip4/1.2.3.4/tcp/9000/p2p/12D3KooW..."
    peer_id: String,
}

/// Query auth server for relay info.
/// Currently falls back to RELAY_ADDR env var.
/// Replace the body with a real HTTP call when your auth server is ready:
///
/// ```
/// let resp = reqwest::get("https://your-auth-server/relay")
///     .await?
///     .json::<RelayInfo>()
///     .await?;
/// return Ok(resp.addr);
/// ```
async fn fetch_relay_addr() -> Result<String, Box<dyn std::error::Error>> {
    // TODO: replace with auth server query
    // Example:
    // let resp = reqwest::Client::new()
    //     .get("https://auth.yourapp.com/v1/relay")
    //     .bearer_auth(token)
    //     .send()
    //     .await?
    //     .json::<serde_json::Value>()
    //     .await?;
    // return Ok(format!(
    //     "/ip4/{}/tcp/{}/p2p/{}",
    //     resp["ip"], resp["port"], resp["peer_id"]
    // ));

    Ok(std::env::var("RELAY_ADDR").unwrap_or_else(|_| "/ip4/127.0.0.1/tcp/9000".to_string()))
}

#[derive(Clone)]
struct CliState {
    active_topic: Arc<Mutex<String>>,
    topics: Arc<Mutex<HashMap<String, String>>>,
}

impl CliState {
    fn new(default_topic: &str) -> Self {
        let mut topics = HashMap::new();
        topics.insert(default_topic.to_string(), "broadcast".to_string());
        Self {
            active_topic: Arc::new(Mutex::new(default_topic.to_string())),
            topics: Arc::new(Mutex::new(topics)),
        }
    }
}

#[allow(unused)]
struct CliHandler {
    state: CliState,
}

impl EventHandler for CliHandler {
    fn handle(&self, event: &ZRPEvent) -> bool {
        match event {
            ZRPEvent::Message { payload, .. } => {
                let text = String::from_utf8_lossy(payload);
                println!("\r\x1b[K[msg] {}", text);
            }
            ZRPEvent::PeerConnected { peer_id, .. } => {
                println!("\r\x1b[K[+] peer connected: {}", peer_id);
            }
            ZRPEvent::PeerDisconnected { peer_id, reason } => {
                println!("\r\x1b[K[-] peer disconnected: {} ({:?})", peer_id, reason);
            }
            ZRPEvent::ConnectionStatus(status) => {
                println!("\r\x1b[K[~] connection: {:?}", status);
            }
            ZRPEvent::MessageSendFailed { reason, .. } => {
                println!("\r\x1b[K[!] send failed: {:?}", reason);
            }
            _ => {}
        }
        true
    }
}

enum Command {
    /// /join <topic> [name]  — subscribe to a topic
    Join(String, Option<String>),
    /// /leave <topic>        — unsubscribe from a topic
    Leave(String),
    /// /switch <topic>       — switch active topic
    Switch(String),
    /// /topics               — list subscribed topics
    Topics,
    /// /send <file>          — placeholder for file/image upload
    Send(String),
    /// /help
    Help,
    /// Plain message text
    Message(String),
}

fn parse_command(input: &str) -> Command {
    let input = input.trim();
    if let Some(rest) = input.strip_prefix("/join ") {
        let mut parts = rest.splitn(2, ' ');
        let topic = parts.next().unwrap_or("").to_string();
        let name = parts.next().map(|s| s.to_string());
        Command::Join(topic, name)
    } else if let Some(rest) = input.strip_prefix("/leave ") {
        Command::Leave(rest.trim().to_string())
    } else if let Some(rest) = input.strip_prefix("/switch ") {
        Command::Switch(rest.trim().to_string())
    } else if input == "/topics" {
        Command::Topics
    } else if let Some(rest) = input.strip_prefix("/send ") {
        Command::Send(rest.trim().to_string())
    } else if input == "/help" {
        Command::Help
    } else {
        Command::Message(input.to_string())
    }
}

fn print_help() {
    println!("Commands:");
    println!("  /join <topic> [name]  — subscribe to a topic");
    println!("  /leave <topic>        — unsubscribe from a topic");
    println!("  /switch <topic>       — switch active topic");
    println!("  /topics               — list subscribed topics");
    println!("  /send <path>          — send a file (coming soon)");
    println!("  /help                 — show this help");
    println!("  <text>                — send message on active topic");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    */

    let relay_addr = fetch_relay_addr().await?;
    info!("Using relay: {}", relay_addr);

    let identity = NodeIdentity::generate();
    let config = TransportConfig::with_ports(0, 0);

    let broadcast_topic = "penis".to_string(); // TODO: replace with SHA3 hash
    let state = CliState::new(&broadcast_topic);

    let mut ctx = ZRPContext::default();
    ctx.register_handler(
        "cli",
        CliHandler {
            state: state.clone(),
        },
    )
    .await;

    let handle = ctx
        .start(identity, vec![relay_addr], config, |key, relay_client| {
            ClientBehavior::new(key.public(), relay_client, key)
        })
        .await?;

    handle.subscribe(broadcast_topic.clone()).await;
    println!("Subscribed to broadcast. Type /help for commands.");
    println!("Active topic: broadcast ({})", broadcast_topic);

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    None => break, // EOF
                    Some(input) if input.trim().is_empty() => continue,
                    Some(input) => {
                        match parse_command(&input) {
                            Command::Message(text) => {
                                let topic = state.active_topic.lock().await.clone();
                                handle.publish(topic, text.into_bytes()).await;
                            }

                            Command::Join(topic, name) => {
                                let display = name.unwrap_or_else(|| topic.clone());
                                handle.subscribe(topic.clone()).await;
                                state.topics.lock().await
                                    .insert(topic.clone(), display.clone());
                                println!("Joined topic '{}' as '{}'", topic, display);
                            }

                            Command::Leave(topic) => {
                                handle.unsubscribe(topic.clone()).await;
                                state.topics.lock().await.remove(&topic);
                                let mut active = state.active_topic.lock().await;
                                if *active == topic {
                                    *active = broadcast_topic.clone();
                                    println!("Left '{}', switched to broadcast", topic);
                                } else {
                                    println!("Left topic '{}'", topic);
                                }
                            }

                            Command::Switch(topic) => {
                                let topics = state.topics.lock().await;
                                if topics.contains_key(&topic) {
                                    drop(topics);
                                    *state.active_topic.lock().await = topic.clone();
                                    println!("Switched to topic '{}'", topic);
                                } else {
                                    println!("Not subscribed to '{}'. Use /join first.", topic);
                                }
                            }

                            Command::Topics => {
                                let active = state.active_topic.lock().await.clone();
                                let topics = state.topics.lock().await;
                                println!("Subscribed topics:");
                                for (hash, name) in topics.iter() {
                                    let marker = if *hash == active { "*" } else { " " };
                                    println!("  {} {} ({})", marker, name, &hash[..8]);
                                }
                            }

                            Command::Send(path) => {
                                // TODO: implement file/image upload
                                // Steps when ready:
                                // 1. Read file bytes
                                // 2. Encode with image/file codec
                                // 3. For large files: shard and store in distributed storage
                                // 4. Publish a FileRef message with content hash + retrieval info
                                println!("File upload not yet implemented: {}", path);
                                println!("(Coming soon: sharded distributed storage)");
                            }

                            Command::Help => print_help(),
                        }
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("\nShutting down...");
                break;
            }
        }
    }

    handle.shutdown().await;
    Ok(())
}
