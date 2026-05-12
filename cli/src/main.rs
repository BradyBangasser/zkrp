use std::time::Duration;

use libghost::behavior::ClientBehavior;
use libghost::node::MeshNode as CoreNode;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let identity = NodeIdentity::generate();
    let relay_addr =
        std::env::var("RELAY_ADDR").unwrap_or_else(|_| "/ip4/127.0.0.1/tcp/9000".to_string());
    let config = TransportConfig::with_ports(0, 0);

    let mut node = CoreNode::start(identity, relay_addr, config, |key, relay_client| {
        ClientBehavior::new(key.public(), relay_client, key)
    })
    .await?;

    node.subscribe("ghost/test/v1").await?;

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut tick = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            result = lines.next_line() => {
                if let Some(line) = result? {
                    if line == "stop" {
                        break;
                    }
                    info!("Sending message");
                node.send_message("ghost/test/v1", line.into_bytes()).await?;
                }
            }
            _ = tick.tick() => {
                for msg in node.drain_messages() {
                        println!("MSG: {}", String::from_utf8(msg.payload)?);
                }
            }
        }
    }

    Ok(())
}
