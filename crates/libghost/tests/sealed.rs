//! Sealed topics through a real relay. Start the local stack
//! (scripts/local-up.sh), then from the repository root:
//!
//!   ZKRP_E2E_GRPC=http://127.0.0.1:9001 \
//!   DEV_ISSUER_KEY=$PWD/deploy/local/secrets/issuer-key \
//!   cargo test -p libghost --test sealed -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use libghost::behavior::ClientBehavior;
use libghost::context::{SwarmCommand, ZRPContext, ZRPHandle};
use libghost::handler::{EventHandler, ZRPEvent};
use libghost::identity::NodeIdentity;
use libghost::store::{GhostStore, MemoryStore};
use libghost::transport::TransportConfig;
use libp2p::PeerId;
use tokio::sync::mpsc;

type Received = (String, PeerId, Vec<u8>);

struct Collect(mpsc::UnboundedSender<Received>);

impl EventHandler for Collect {
    fn handle(&self, event: &ZRPEvent, _tx: mpsc::Sender<SwarmCommand>) -> bool {
        if let ZRPEvent::Message { conversation, peer_id, payload, .. } = event {
            let _ = self.0.send((conversation.to_string(), *peer_id, payload.clone()));
        }
        true
    }
}

/// A credential from the local stack's throwaway issuer key.
fn dev_credential(store: &dyn GhostStore) {
    let path = std::env::var("DEV_ISSUER_KEY").expect("DEV_ISSUER_KEY");
    let key = std::fs::read_to_string(path).expect("read DEV_ISSUER_KEY");
    let sk = zkcred::IssuerSecretKey::from_bytes(&hex::decode(key.trim()).unwrap()).unwrap();
    let params = libghost::admit::params(zkcred::DEFAULT_PARAMS_LABEL);
    let epoch_secs = 2_592_000;
    let epoch = zkcred::epoch_at(libghost::admit::now_unix(), epoch_secs);
    let mut rng = zkcred::rand::rngs::OsRng;
    let (pending, req) = zkcred::request(&params, b"dev", &mut rng);
    let sig = zkcred::issue(&params, &sk, &req, b"dev", epoch, &mut rng).unwrap();
    let cred = zkcred::finalize(&params, pending, &sig, epoch, &sk.public_key(&params)).unwrap();
    libghost::admit::save_credential(store, &cred, params.label(), epoch_secs);
}

async fn node(sealed_only: bool) -> (ZRPHandle, mpsc::UnboundedReceiver<Received>, PeerId) {
    let grpc = std::env::var("ZKRP_E2E_GRPC").expect("ZKRP_E2E_GRPC");
    let store: Arc<dyn GhostStore> = Arc::new(MemoryStore::new());
    dev_credential(store.as_ref());
    let mut ctx = ZRPContext::with_store(Arc::clone(&store));
    ctx.seals().require(sealed_only);
    let (tx, rx) = mpsc::unbounded_channel();
    ctx.register_handler("test", Collect(tx)).await;
    let identity = NodeIdentity::generate();
    let account = identity.peer_id;
    let handle = ctx
        .start(
            identity,
            None,
            Some(vec![grpc]),
            TransportConfig::with_ports(0, 0),
            |key, relay| ClientBehavior::new(key.public(), relay, key),
        )
        .await
        .expect("start");
    (handle, rx, account)
}

#[tokio::test]
#[ignore]
async fn sealed_topic_reaches_key_holders_only() {
    let topic = format!("e2e/{}", hex::encode(NodeIdentity::generate_conversation_key()));
    let key: [u8; 32] = NodeIdentity::generate_conversation_key().try_into().unwrap();

    let (alice, _, alice_account) = node(true).await;
    let (bob, mut bob_rx, _) = node(true).await;
    let (eve, mut eve_rx, _) = node(false).await;
    for h in [&alice, &bob] {
        h.subscribe_sealed(topic.clone(), key).await;
        h.seals().set_hello_topics(vec![topic.clone()]);
    }
    eve.subscribe(topic.clone()).await;

    // Sealed-only: a publish to a topic without a key never leaves.
    alice.publish(format!("{topic}/open"), b"in the clear".to_vec()).await;

    // Gossip meshes take a few heartbeats to form; repeat until it lands.
    let got = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            alice.publish_typed(topic.clone(), b"secret".to_vec(), 0x0042).await;
            if let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(3), bob_rx.recv()).await {
                break m;
            }
        }
    })
    .await
    .expect("bob never received the sealed message");

    assert_eq!(got.0, topic);
    assert_eq!(got.1, alice_account, "attributed to the signing account");
    assert_eq!(got.2, b"secret");
    // Eve is on the topic but has no key: nothing opens for her.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(eve_rx.try_recv().is_err(), "a node without the key received something");

    for h in [alice, bob, eve] {
        h.shutdown().await;
    }
}
