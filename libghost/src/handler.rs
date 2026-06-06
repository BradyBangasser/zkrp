use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::context::SwarmCommand;

#[derive(Debug, Clone)]
pub enum ZRPEvent {
    PeerConnected {
        peer_id: libp2p::PeerId, // ephemeral, already public
        addr: libp2p::Multiaddr,
    },
    PeerDisconnected {
        peer_id: libp2p::PeerId,
        reason: DisconnectReason,
    },
    RelayChanged {
        relay_addr: String,
    },
    ConnectionStatus(ConnectionStatus),

    Message {
        conversation: ConversationId,
        peer_id: libp2p::PeerId,
        content_type: u16,
        payload: Vec<u8>,
    },
    MessageSendFailed {
        conversation: ConversationId,
        reason: SendFailReason,
    },
    DecryptionFailed {
        conversation: ConversationId,
        reason: DecryptFailReason,
    },
    UnknownContentType {
        content_type_id: u16,
        raw_bytes: Vec<u8>,
    },

    SessionEstablished {
        conversation: ConversationId,
    },
    SessionBroken {
        conversation: ConversationId,
        reason: SessionBreakReason,
    },

    GroupInviteReceived {
        group_id: GroupId,
        group_name: String,
    },
    GroupMemberJoined {
        group_id: GroupId,
    },
    GroupMemberLeft {
        group_id: GroupId,
    },

    PrekeyBundleExpiring {
        keys_remaining: u32,
    },
}

pub type ConversationId = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupId(pub [u8; 32]); // random 32 bytes

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub identity_key: Vec<u8>,    // Ed25519 public key
    pub pq_identity_key: Vec<u8>, // Dilithium3 public key
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connecting,
    Connected {
        relay: libp2p::PeerId,
    },
    Degraded {
        relay: libp2p::PeerId,
        reason: String,
    },
    Disconnected,
}

#[derive(Debug, Clone)]
pub enum SendFailReason {
    NoSession,
    NoPeersSubscribed,
    EncryptionFailed,
    NetworkError(String),
}

#[derive(Debug, Clone)]
pub enum DisconnectReason {
    Clean,
    Timeout,
    ProtocolError(String),
    RelayLost,
}

#[derive(Debug, Clone)]
pub enum SessionBreakReason {
    TooManySkippedMessages,
    InvalidRatchetState,
    PrekeyExhausted,
    SignatureVerificationFailed,
}

#[derive(Debug, Clone)]
pub enum DecryptFailReason {
    UnknownSession,
    InvalidSignature,
    InvalidCiphertext,
    ReplayDetected,
    MessageIndexTooOld,
}

#[allow(unused)]
pub trait EventHandler: Send + Sync + 'static {
    fn handle(&self, event: &ZRPEvent, mut handle: Sender<SwarmCommand>) -> bool {
        true
    }

    fn filter(&self, event: &ZRPEvent) -> bool {
        true
    }
}

#[derive(Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn EventHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: impl Into<String>, handler: impl EventHandler + 'static) {
        self.handlers.insert(name.into(), Arc::new(handler));
    }

    pub fn remove(&mut self, name: &str) {
        self.handlers.remove(name);
    }

    pub async fn dispatch(&mut self, event: ZRPEvent, crypt_tx: Sender<SwarmCommand>) {
        let event = Arc::new(event);
        let mut to_remove = vec![];

        let mut join_set = tokio::task::JoinSet::new();

        for (name, handler) in &self.handlers {
            if !handler.filter(&event) {
                continue;
            }
            let h = handler.clone();
            let e = event.clone();
            let n = name.clone();
            let tx = crypt_tx.clone();
            join_set.spawn(async move {
                let keep = h.handle(&e, tx);
                (n, keep)
            });
        }

        while let Some(Ok((name, keep))) = join_set.join_next().await {
            if !keep {
                to_remove.push(name);
            }
        }

        for name in to_remove {
            self.handlers.remove(&name);
        }
    }
}
