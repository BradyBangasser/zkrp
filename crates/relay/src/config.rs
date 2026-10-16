use std::path::PathBuf;

fn credential(name: &str) -> Option<String> {
    if let Ok(dir) = std::env::var("CREDENTIALS_DIRECTORY") {
        let path = std::path::Path::new(&dir).join(name);
        if let Ok(val) = std::fs::read_to_string(&path) {
            return Some(val.trim().to_string());
        }
    }
    std::env::var(name.to_uppercase().replace('-', "_")).ok()
}

/// Export a systemd credential as an env var for the AWS SDK. When neither is
/// present, the SDK's default chain applies (EC2 instance role, ~/.aws, ...).
fn load_cred_into_env(var: &str, cred: &str) {
    if std::env::var(var).is_ok() {
        return;
    }
    if let Some(val) = credential(cred) {
        // SAFETY: called from `load_config` before any other thread is spawned.
        unsafe {
            std::env::set_var(var, &val);
        }
    }
}

fn required(var: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| panic!("{var} not set"))
}

fn list(var: &str, default: &str) -> Vec<String> {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// How the relay checks credentials. `None` (ADMISSION=off) admits everyone,
/// for local development only.
#[derive(Clone)]
pub struct AdmissionConfig {
    pub params_label: String,
    pub issuer_public_key: Vec<u8>,
    pub epoch_secs: u64,
}

fn admission() -> Option<AdmissionConfig> {
    if std::env::var("ADMISSION").as_deref() == Ok("off") {
        tracing::warn!("ADMISSION=off: relay accepts peers without credentials");
        return None;
    }
    let key = required("ISSUER_PUBLIC_KEY");
    Some(AdmissionConfig {
        params_label: std::env::var("PARAMS_LABEL")
            .unwrap_or_else(|_| zkcred::DEFAULT_PARAMS_LABEL.to_string()),
        issuer_public_key: hex::decode(key.trim()).expect("ISSUER_PUBLIC_KEY is not hex"),
        epoch_secs: std::env::var("EPOCH_SECS")
            .unwrap_or_else(|_| "2592000".to_string())
            .parse()
            .expect("EPOCH_SECS is not a number"),
    })
}

/// Everything the relay needs, read once at startup. Missing required values
/// panic here, before the swarm or gRPC server exist, so a misconfigured
/// container fails fast instead of coming up half-alive.
#[derive(Clone)]
pub struct RelayConfig {
    pub blob_bucket: String,
    pub log_bucket: String,
    pub mailbox_bucket: String,
    pub cf_domain: String,
    /// Mesh names advertised in Health / ListRelays. Display only.
    pub meshes: Vec<String>,
    /// Region label advertised to clients, e.g. "us-central".
    pub region: String,
    /// Where the relay's libp2p keypair lives. A stable keypair means a stable
    /// peer id, so `/p2p/<id>` addresses cached by clients survive restarts.
    pub identity_path: PathBuf,
    /// Postgres for the shared relay registry. Optional: without it the relay
    /// only ever advertises itself.
    pub database_url: Option<String>,
    pub admission: Option<AdmissionConfig>,
    /// This relay's public gRPC URL, advertised in ListRelays and the
    /// registry. PUBLIC_GRPC_URL, else https://$RELAY_DOMAIN.
    pub public_grpc_url: String,
    /// Served to clients by GetNetworkConfig, so apps don't hard-code it.
    pub auth_url: String,
}

pub fn load_config() -> RelayConfig {
    load_cred_into_env("AWS_ACCESS_KEY_ID", "aws-key-id");
    load_cred_into_env("AWS_SECRET_ACCESS_KEY", "aws-secret");
    required("AWS_REGION");

    let blob_bucket = required("BLOB_BUCKET");
    RelayConfig {
        log_bucket: required("LOG_BUCKET"),
        mailbox_bucket: std::env::var("MAILBOX_BUCKET").unwrap_or_else(|_| blob_bucket.clone()),
        blob_bucket,
        cf_domain: required("CF_DOMAIN"),
        meshes: list("RELAY_MESHES", "zkrp"),
        region: std::env::var("REGION").unwrap_or_else(|_| "local".to_string()),
        identity_path: std::env::var("IDENTITY_PATH")
            .unwrap_or_else(|_| "relay_identity.key".to_string())
            .into(),
        database_url: credential("database-url").filter(|s| !s.is_empty()),
        public_grpc_url: std::env::var("PUBLIC_GRPC_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("RELAY_DOMAIN")
                    .ok()
                    .filter(|d| !d.is_empty())
                    .map(|d| format!("https://{d}"))
            })
            .unwrap_or_default(),
        auth_url: std::env::var("AUTH_URL").unwrap_or_default(),
        admission: admission(),
    }
}
