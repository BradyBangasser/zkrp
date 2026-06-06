use libghost::behavior::ClientBehavior;
use libghost::context::{ZRPContext, ZRPHandle};
use libghost::handler::{EventHandler, ZRPEvent};
use libghost::relay::RelayClient;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::info;

#[derive(Clone)]
struct Profile {
    name: String,
    college: String,
    year: String,
    bio: String,
}

impl Profile {
    fn generate() -> Self {
        let names = [
            "Alex Chen",
            "Jordan Smith",
            "Taylor Brooks",
            "Casey Rivera",
            "Morgan Lee",
            "Riley Johnson",
            "Quinn Davis",
            "Avery Wilson",
            "Blake Martinez",
            "Cameron Thompson",
        ];
        let colleges = [
            "Iowa State",
            "University of Iowa",
            "Drake University",
            "UNI",
            "Grinnell College",
        ];
        let years = ["Freshman", "Sophomore", "Junior", "Senior"];
        let bios = [
            "here for a good time",
            "coffee addict ☕ | pre-law",
            "greek life + intramurals",
            "music nerd 🎶",
            "just here to vibe",
        ];

        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
            .hash(&mut h);
        let seed = h.finish() as usize;

        Self {
            name: names[seed % names.len()].to_string(),
            college: colleges[seed % colleges.len()].to_string(),
            year: years[seed % years.len()].to_string(),
            bio: bios[seed % bios.len()].to_string(),
        }
    }

    fn display(&self) {
        println!("  Name:    {}", self.name);
        println!("  College: {}", self.college);
        println!("  Year:    {}", self.year);
        println!("  Bio:     {}", self.bio);
    }
}

struct CliArgs {
    relay: Option<String>,
    name: Option<String>,
    college: Option<String>,
    year: Option<String>,
    bio: Option<String>,
    generate_profile: bool,
    autolike: bool,
    autoreply: bool,
}

fn parse_args() -> Result<CliArgs, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cli = CliArgs {
        relay: None,
        name: None,
        college: None,
        year: None,
        bio: None,
        generate_profile: false,
        autolike: false,
        autoreply: false,
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--autolike" => cli.autolike = true,
            "--autoreply" => cli.autoreply = true,
            "--generate-profile" => cli.generate_profile = true,
            "--name" => {
                i += 1;
                cli.name = args
                    .get(i)
                    .cloned()
                    .ok_or("--name requires a value")?
                    .into();
            }
            "--college" => {
                i += 1;
                cli.college = args
                    .get(i)
                    .cloned()
                    .ok_or("--college requires a value")?
                    .into();
            }
            "--year" => {
                i += 1;
                cli.year = args
                    .get(i)
                    .cloned()
                    .ok_or("--year requires a value")?
                    .into();
            }
            "--bio" => {
                i += 1;
                cli.bio = args.get(i).cloned().ok_or("--bio requires a value")?.into();
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            arg if !arg.starts_with("--") => {
                cli.relay = Some(arg.to_string());
            }
            arg => return Err(format!("Unknown option: {}", arg)),
        }
        i += 1;
    }

    Ok(cli)
}

fn print_usage() {
    println!("Usage: ghost-cli [OPTIONS] [RELAY]");
    println!();
    println!("Arguments:");
    println!("  [RELAY]              IP, domain, or full multiaddr of relay");
    println!("                       e.g. 1.2.3.4  or  relay.example.com:9001");
    println!("                       Peer ID is fetched automatically via gRPC");
    println!();
    println!("Options:");
    println!("  --autolike           Auto-like all connecting peers");
    println!("  --autoreply          Auto-reply to incoming messages");
    println!("  --generate-profile   Generate a random profile");
    println!("  --name <NAME>        Your display name");
    println!("  --college <COLLEGE>  Your college");
    println!("  --year <YEAR>        Your year (Freshman/Sophomore/Junior/Senior)");
    println!("  --bio <BIO>          Your bio");
    println!("  --help               Show this help");
}

fn parse_relay_host(input: &str) -> (String, u16) {
    let input = input.trim().trim_start_matches('/');
    if let Some(colon) = input.rfind(':') {
        let port = input[colon + 1..].parse::<u16>().unwrap_or(9001);
        (input[..colon].to_string(), port)
    } else {
        (input.to_string(), 9001)
    }
}

fn grpc_addr(host: &str, port: u16) -> String {
    format!("http://{}:{}", host, port)
}

fn build_multiaddr(host: &str, mesh_port: u16, peer_id: &str) -> String {
    let is_ipv4 = host.chars().all(|c| c.is_ascii_digit() || c == '.');
    if is_ipv4 {
        format!("/ip4/{}/tcp/{}/p2p/{}", host, mesh_port, peer_id)
    } else {
        format!("/dns4/{}/tcp/{}/p2p/{}", host, mesh_port, peer_id)
    }
}

async fn resolve_relay(input: &str) -> String {
    if input.starts_with('/') {
        return input.to_string();
    }

    let (host, grpc_port) = parse_relay_host(input);
    let addr = grpc_addr(&host, grpc_port);

    info!("Fetching relay info via gRPC: {}", addr);

    match RelayClient::connect(&addr).await {
        Ok(mut client) => match client.list_relays().await {
            Ok(relays) if !relays.is_empty() => {
                let relay = &relays[0];
                if relay.multiaddr.starts_with('/') {
                    info!("Resolved relay: {}", relay.multiaddr);
                    return relay.multiaddr.clone();
                }
                let multiaddr = build_multiaddr(&host, 9000, &relay.peer_id);
                info!("Resolved relay: {}", multiaddr);
                multiaddr
            }
            Ok(_) => {
                tracing::warn!("gRPC returned no relays, falling back to plain multiaddr");
                build_multiaddr_no_peer(&host)
            }
            Err(e) => {
                tracing::warn!("gRPC list_relays failed: {} — falling back", e);
                build_multiaddr_no_peer(&host)
            }
        },
        Err(e) => {
            tracing::warn!("gRPC connect failed: {} — falling back", e);
            build_multiaddr_no_peer(&host)
        }
    }
}

fn build_multiaddr_no_peer(host: &str) -> String {
    let is_ipv4 = host.chars().all(|c| c.is_ascii_digit() || c == '.');
    if is_ipv4 {
        format!("/ip4/{}/tcp/9000", host)
    } else {
        format!("/dns4/{}/tcp/9000", host)
    }
}

#[derive(Clone)]
struct CliState {
    active_topic: Arc<Mutex<String>>,
    topics: Arc<Mutex<HashMap<String, String>>>,
    profile: Arc<Mutex<Profile>>,
    auto_reply: Arc<Mutex<bool>>,
    auto_like: Arc<Mutex<bool>>,
    liked_peers: Arc<Mutex<Vec<String>>>,
    message_log: Arc<Mutex<Vec<(String, String)>>>,
}

use libghost::context::SwarmCommand;

impl CliState {
    fn new(default_topic: &str, profile: Profile, auto_like: bool, auto_reply: bool) -> Self {
        let mut topics = HashMap::new();
        topics.insert(default_topic.to_string(), "broadcast".to_string());
        Self {
            active_topic: Arc::new(Mutex::new(default_topic.to_string())),
            topics: Arc::new(Mutex::new(topics)),
            profile: Arc::new(Mutex::new(profile)),
            auto_reply: Arc::new(Mutex::new(auto_reply)),
            auto_like: Arc::new(Mutex::new(auto_like)),
            liked_peers: Arc::new(Mutex::new(Vec::new())),
            message_log: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

struct CliHandler {
    state: CliState,
}

impl EventHandler for CliHandler {
    fn handle(&self, event: &ZRPEvent, tx: tokio::sync::mpsc::Sender<SwarmCommand>) -> bool {
        match event {
            ZRPEvent::Message {
                peer_id, payload, ..
            } => {
                let text = String::from_utf8_lossy(payload).to_string();
                let peer = peer_id.to_string();
                let short = peer[..peer.len().min(12)].to_string();

                println!("\r\x1b[K\x1b[33m[{}…]\x1b[0m {}", short, text);

                let state = self.state.clone();
                let peer2 = peer.clone();
                let text2 = text.clone();
                tokio::spawn(async move {
                    state.message_log.lock().await.push((peer2.clone(), text2));

                    if *state.auto_reply.lock().await {
                        let profile = state.profile.lock().await.clone();
                        let reply = format!(
                            "Hey! I'm {} from {} ({}). {}",
                            profile.name, profile.college, profile.year, profile.bio
                        );
                        let reply_topic = format!("fratrat/v1/dm/{}", peer2);
                        let _ = ZRPHandle::send(tx.clone(), reply_topic, reply.into_bytes()).await;
                        println!("\r\x1b[K\x1b[90m[auto-replied]\x1b[0m");
                    }
                });

                print!("> ");
                let _ = std::io::stdout().flush();
            }

            ZRPEvent::PeerConnected { peer_id, .. } => {
                let peer = peer_id.to_string();
                let short = peer[..peer.len().min(12)].to_string();
                println!("\r\x1b[K\x1b[32m[+]\x1b[0m peer connected: {}…", short);

                let state = self.state.clone();
                let peer2 = peer.clone();
                tokio::spawn(async move {
                    // Auto-like
                    if *state.auto_like.lock().await {
                        state.liked_peers.lock().await.push(peer2.clone());
                        println!(
                            "\r\x1b[K\x1b[35m[♥ auto-liked]\x1b[0m {}…",
                            &peer2[..peer2.len().min(12)]
                        );
                    }

                    // Announce our profile
                    let profile = state.profile.lock().await.clone();
                    let announcement = format!(
                        "PROFILE:{}|{}|{}|{}",
                        profile.name, profile.college, profile.year, profile.bio
                    );
                    let _ = ZRPHandle::send(
                        tx.clone(),
                        "fratrat/v1/profiles".to_string(),
                        announcement.into_bytes(),
                    )
                    .await;
                });

                print!("> ");
                let _ = std::io::stdout().flush();
            }

            ZRPEvent::PeerDisconnected { peer_id, reason } => {
                let peer = peer_id.to_string();
                println!(
                    "\r\x1b[K\x1b[31m[-]\x1b[0m peer disconnected: {}… ({:?})",
                    &peer[..peer.len().min(12)],
                    reason
                );
                print!("> ");
                let _ = std::io::stdout().flush();
            }

            ZRPEvent::ConnectionStatus(status) => {
                println!("\r\x1b[K\x1b[36m[~]\x1b[0m connection: {:?}", status);
                print!("> ");
                let _ = std::io::stdout().flush();
            }

            ZRPEvent::MessageSendFailed { reason, .. } => {
                println!("\r\x1b[K\x1b[31m[!]\x1b[0m send failed: {:?}", reason);
                print!("> ");
                let _ = std::io::stdout().flush();
            }

            _ => {}
        }
        true
    }
}

const COMMANDS: &[&str] = &[
    "/join",
    "/leave",
    "/switch",
    "/topics",
    "/send",
    "/profile",
    "/autoreply",
    "/autolike",
    "/likes",
    "/messages",
    "/help",
    "/quit",
];

fn tab_complete(partial: &str, topics: &HashMap<String, String>) -> Option<String> {
    if partial.starts_with('/') {
        let matches: Vec<&str> = COMMANDS
            .iter()
            .filter(|c| c.starts_with(partial))
            .copied()
            .collect();

        match matches.len() {
            0 => None,
            1 => Some(format!("{} ", matches[0])),
            _ => {
                println!();
                for m in &matches {
                    print!("  {}  ", m);
                }
                println!();
                None
            }
        }
    } else if partial.starts_with("/switch ") || partial.starts_with("/leave ") {
        let prefix = partial.split(' ').next().unwrap_or("");
        let topic_part = partial.split(' ').nth(1).unwrap_or("");
        let matches: Vec<&String> = topics
            .keys()
            .filter(|t| t.starts_with(topic_part))
            .collect();

        match matches.len() {
            0 => None,
            1 => Some(format!("{} {} ", prefix, matches[0])),
            _ => {
                println!();
                for m in &matches {
                    print!("  {}  ", m);
                }
                println!();
                None
            }
        }
    } else {
        None
    }
}

enum Command {
    Join(String, Option<String>),
    Leave(String),
    Switch(String),
    Topics,
    Send(String),
    Profile(Option<String>),
    AutoReply(Option<bool>),
    AutoLike(Option<bool>),
    Likes,
    Messages,
    Help,
    Quit,
    Message(String),
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Some(true),
        "off" | "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn parse_command(input: &str) -> Command {
    let input = input.trim();

    if let Some(rest) = input.strip_prefix("/join ") {
        let mut p = rest.splitn(2, ' ');
        Command::Join(p.next().unwrap_or("").into(), p.next().map(Into::into))
    } else if let Some(rest) = input.strip_prefix("/leave ") {
        Command::Leave(rest.trim().into())
    } else if let Some(rest) = input.strip_prefix("/switch ") {
        Command::Switch(rest.trim().into())
    } else if input == "/topics" {
        Command::Topics
    } else if let Some(rest) = input.strip_prefix("/send ") {
        Command::Send(rest.trim().into())
    } else if input == "/profile" {
        Command::Profile(None)
    } else if let Some(rest) = input.strip_prefix("/profile ") {
        Command::Profile(Some(rest.trim().into()))
    } else if input == "/autoreply" {
        Command::AutoReply(None)
    } else if let Some(rest) = input.strip_prefix("/autoreply ") {
        Command::AutoReply(parse_bool(rest.trim()))
    } else if input == "/autolike" {
        Command::AutoLike(None)
    } else if let Some(rest) = input.strip_prefix("/autolike ") {
        Command::AutoLike(parse_bool(rest.trim()))
    } else if input == "/likes" {
        Command::Likes
    } else if input == "/messages" {
        Command::Messages
    } else if input == "/help" {
        Command::Help
    } else if input == "/quit" || input == "/exit" {
        Command::Quit
    } else {
        Command::Message(input.into())
    }
}

fn print_help() {
    println!("\x1b[1mCommands:\x1b[0m");
    println!("  /join <topic> [name]   subscribe to a topic");
    println!("  /leave <topic>         unsubscribe from a topic");
    println!("  /switch <topic>        switch active topic");
    println!("  /topics                list subscribed topics");
    println!("  /profile               show your profile");
    println!("  /profile <name>        set your display name");
    println!("  /autoreply [on|off]    auto-reply to incoming messages");
    println!("  /autolike  [on|off]    auto-like every new peer");
    println!("  /likes                 show liked peers");
    println!("  /messages              show recent messages");
    println!("  /send <path>           send a file (coming soon)");
    println!("  /help                  show this help");
    println!("  /quit                  exit");
    println!("  <text>                 send on active topic");
    println!();
    println!("  \x1b[90mTab\x1b[0m completes commands and topic names.");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = parse_args().unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        print_usage();
        std::process::exit(1);
    });

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let relay_input: String = cli
        .relay
        .or_else(|| Some(std::env::var("RELAY_ADDR").unwrap_or("127.0.0.1".into())))
        .unwrap();

    let relay_addr = resolve_relay(&relay_input).await;
    info!("Using relay: {}", relay_addr);

    let profile = if cli.generate_profile || cli.name.is_none() {
        let mut p = Profile::generate();
        if let Some(name) = cli.name {
            p.name = name;
        }
        if let Some(college) = cli.college {
            p.college = college;
        }
        if let Some(year) = cli.year {
            p.year = year;
        }
        if let Some(bio) = cli.bio {
            p.bio = bio;
        }
        p
    } else {
        Profile {
            name: cli.name.unwrap_or_default(),
            college: cli.college.unwrap_or_default(),
            year: cli.year.unwrap_or_default(),
            bio: cli.bio.unwrap_or_default(),
        }
    };

    println!("\x1b[1mFratRat CLI\x1b[0m — your profile:");
    profile.display();
    if cli.autolike {
        println!("  \x1b[35m♥ auto-like ON\x1b[0m");
    }
    if cli.autoreply {
        println!("  \x1b[36m↩ auto-reply ON\x1b[0m");
    }
    println!();

    let identity = NodeIdentity::generate();
    let config = TransportConfig::with_ports(0, 0);
    let broadcast_topic = "fratrat/v1/broadcast".to_string();

    let state = CliState::new(&broadcast_topic, profile, cli.autolike, cli.autoreply);

    let mut ctx = ZRPContext::default();
    ctx.register_handler(
        "cli",
        CliHandler {
            state: state.clone(),
        },
    )
    .await;

    let handle = ctx
        .start(
            identity,
            Some(vec![relay_addr]),
            None,
            config,
            |key, relay_client| ClientBehavior::new(key.public(), relay_client, key),
        )
        .await?;

    handle.subscribe(broadcast_topic.clone()).await;
    handle.subscribe("fratrat/v1/profiles".to_string()).await;

    println!("Connected. Active topic: \x1b[1mbroadcast\x1b[0m");
    println!("Type \x1b[1m/help\x1b[0m for commands, Tab to autocomplete.\n");

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    loop {
        print!("> ");
        std::io::stdout().flush().ok();

        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    None                                     => break,
                    Some(input) if input.trim().is_empty()   => continue,

                    Some(input) if input.contains('\t') => {
                        let partial = input.trim_end_matches('\t').trim();
                        let topics  = state.topics.lock().await.clone();
                        if let Some(completed) = tab_complete(partial, &topics) {
                            println!("→ {}", completed.trim());
                        }
                    }

                    Some(input) => {
                        match parse_command(&input) {

                            Command::Message(text) => {
                                let topic = state.active_topic.lock().await.clone();
                                handle.publish(
                                    topic,
                                    text.into_bytes(),
                                ).await;
                            }

                            Command::Join(topic, name) => {
                                let display = name.unwrap_or_else(|| topic.clone());
                                handle.subscribe(topic.clone()).await;
                                state.topics.lock().await.insert(topic.clone(), display.clone());
                                println!("Joined '{}' as '{}'", topic, display);
                            }

                            Command::Leave(topic) => {
                                handle.unsubscribe(topic.clone()).await;
                                state.topics.lock().await.remove(&topic);
                                let mut active = state.active_topic.lock().await;
                                if *active == topic {
                                    *active = broadcast_topic.clone();
                                    println!("Left '{}', switched to broadcast", topic);
                                } else {
                                    println!("Left '{}'", topic);
                                }
                            }

                            Command::Switch(topic) => {
                                let topics = state.topics.lock().await;
                                if topics.contains_key(&topic) {
                                    drop(topics);
                                    *state.active_topic.lock().await = topic.clone();
                                    let display = state.topics.lock().await
                                        .get(&topic).cloned().unwrap_or(topic.clone());
                                    println!("Switched to '{}'", display);
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
                                    println!(
                                        "  {} {} ({}…)",
                                        marker, name,
                                        &hash[..hash.len().min(16)]
                                    );
                                }
                            }

                            Command::Profile(new_name) => {
                                if let Some(name) = new_name {
                                    state.profile.lock().await.name = name.clone();
                                    println!("Name updated to '{}'", name);
                                } else {
                                    state.profile.lock().await.display();
                                }
                            }

                            Command::AutoReply(val) => {
                                let mut ar = state.auto_reply.lock().await;
                                match val {
                                    Some(v) => {
                                        *ar = v;
                                        println!("Auto-reply: \x1b[1m{}\x1b[0m",
                                            if v { "ON" } else { "OFF" });
                                    }
                                    None => println!("Auto-reply is \x1b[1m{}\x1b[0m",
                                        if *ar { "ON" } else { "OFF" }),
                                }
                            }

                            Command::AutoLike(val) => {
                                let mut al = state.auto_like.lock().await;
                                match val {
                                    Some(v) => {
                                        *al = v;
                                        println!("Auto-like: \x1b[1m{}\x1b[0m",
                                            if v { "ON" } else { "OFF" });
                                    }
                                    None => println!("Auto-like is \x1b[1m{}\x1b[0m",
                                        if *al { "ON" } else { "OFF" }),
                                }
                            }

                            Command::Likes => {
                                let liked = state.liked_peers.lock().await;
                                if liked.is_empty() {
                                    println!("No liked peers yet.");
                                } else {
                                    println!("Liked peers ({}):", liked.len());
                                    for peer in liked.iter() {
                                        println!("  ♥ {}…", &peer[..peer.len().min(20)]);
                                    }
                                }
                            }

                            Command::Messages => {
                                let log = state.message_log.lock().await;
                                if log.is_empty() {
                                    println!("No messages yet.");
                                } else {
                                    println!("Recent messages ({}):", log.len());
                                    for (peer, msg) in log.iter().rev().take(20) {
                                        println!(
                                            "  \x1b[33m{}…\x1b[0m {}",
                                            &peer[..peer.len().min(12)], msg
                                        );
                                    }
                                }
                            }

                            Command::Send(path) => {
                                println!("File upload not yet implemented: {}", path);
                            }

                            Command::Help => print_help(),

                            Command::Quit => {
                                println!("Shutting down...");
                                break;
                            }
                        }
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    println!("\nShutting down...");
    match timeout(Duration::from_secs(2), handle.shutdown()).await {
        Ok(_) => println!("Successfully shutdown"),
        Err(_) => println!("Operation timed out"),
    }

    std::process::exit(0);

    Ok(())
}
