fn credential(name: &str) -> String {
    if let Ok(dir) = std::env::var("CREDENTIALS_DIRECTORY") {
        let path = std::path::Path::new(&dir).join(name);
        if let Ok(val) = std::fs::read_to_string(&path) {
            return val.trim().to_string();
        }
    }
    std::env::var(name.to_uppercase().replace('-', "_")).unwrap_or_else(|_| {
        panic!(
            "credential '{}' not found in CREDENTIALS_DIRECTORY or env",
            name
        )
    })
}

fn expect_or_load_cred(var: &str, cred: &str) {
    if std::env::var(var).is_err() {
        let cred = credential(cred);

        unsafe {
            std::env::set_var(var, &cred);
        }
    }
}

pub async fn load_config() -> RelayConfig {
    expect_or_load_cred("AWS_ACCESS_KEY_ID", "aws-key-id");
    expect_or_load_cred("AWS_SECRET_ACCESS_KEY", "aws-secret");

    RelayConfig {
        region: std::env::var("AWS_REGION").expect("AWS_REGION not set"),
        blob_bucket: std::env::var("BLOB_BUCKET").expect("BLOB_BUCKET not set"),
        log_bucket: std::env::var("LOG_BUCKET").expect("LOG_BUCKET not set"),
        cf_domain: std::env::var("CF_DOMAIN").expect("CF_DOMAIN not set"),
    }
}

#[derive(Clone)]
pub struct RelayConfig {
    pub region: String,
    pub blob_bucket: String,
    pub log_bucket: String,
    pub cf_domain: String,
}
