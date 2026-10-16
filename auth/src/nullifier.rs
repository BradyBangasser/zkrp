//! The nullifier set: the only thing this service persists.
//!
//! nullifier = HMAC(pepper, domain || epoch || slot || email). The pepper lives
//! in KMS in production and never leaves it, so a stolen database can't be
//! brute-forced against a list of student emails. Each email gets a few slots
//! per epoch (reinstall, second device) and no more.

use std::sync::Arc;

use sqlx::PgPool;

const DOMAIN: &[u8] = b"zkrp/nullifier/v1";
pub const SLOTS_PER_EPOCH: u8 = 3;

#[async_trait::async_trait]
pub trait Pepper: Send + Sync {
    async fn mac(&self, message: &[u8]) -> anyhow::Result<Vec<u8>>;
}

/// HMAC-SHA256 inside AWS KMS.
pub struct KmsPepper {
    pub client: aws_sdk_kms::Client,
    pub key_id: String,
}

#[async_trait::async_trait]
impl Pepper for KmsPepper {
    async fn mac(&self, message: &[u8]) -> anyhow::Result<Vec<u8>> {
        let out = self
            .client
            .generate_mac()
            .key_id(&self.key_id)
            .mac_algorithm(aws_sdk_kms::types::MacAlgorithmSpec::HmacSha256)
            .message(aws_sdk_kms::primitives::Blob::new(message))
            .send()
            .await?;
        Ok(out
            .mac()
            .ok_or_else(|| anyhow::anyhow!("KMS returned no MAC"))?
            .as_ref()
            .to_vec())
    }
}

/// Local HMAC key, for development and self-hosting without KMS.
pub struct LocalPepper(pub ring::hmac::Key);

#[async_trait::async_trait]
impl Pepper for LocalPepper {
    async fn mac(&self, message: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(ring::hmac::sign(&self.0, message).as_ref().to_vec())
    }
}

pub struct NullifierSet {
    pub pool: PgPool,
    pub pepper: Arc<dyn Pepper>,
}

fn message(email: &str, epoch: u64, slot: u8) -> Vec<u8> {
    let mut m = DOMAIN.to_vec();
    m.extend_from_slice(&epoch.to_be_bytes());
    m.push(slot);
    m.extend_from_slice(email.as_bytes());
    m
}

impl NullifierSet {
    /// Claim a free slot for `email` in `epoch`. `Ok(false)` when every slot is
    /// taken. Uses only INSERT (see db/auth.sql: the role cannot SELECT).
    pub async fn claim(&self, email: &str, epoch: u64) -> anyhow::Result<bool> {
        for slot in 0..SLOTS_PER_EPOCH {
            let n = self.pepper.mac(&message(email, epoch, slot)).await?;
            let inserted = sqlx::query("INSERT INTO nullifier (n) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(n)
                .execute(&self.pool)
                .await?
                .rows_affected();
            if inserted == 1 {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::message;

    #[test]
    fn slots_and_epochs_separate_nullifiers() {
        assert_ne!(message("a@x.edu", 1, 0), message("a@x.edu", 1, 1));
        assert_ne!(message("a@x.edu", 1, 0), message("a@x.edu", 2, 0));
    }
}
