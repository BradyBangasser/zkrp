//! Apple App Attest *attestation* verification, statelessly.
//!
//! Follows Apple's "Validating apps that connect to your server", steps 1–9,
//! except that nothing is stored: the attested key id would be a stable
//! per-device identifier, and this service keeps none. Each enrollment
//! attests a fresh key instead.
//!
//! https://developer.apple.com/documentation/devicecheck/validating-apps-that-connect-to-your-server

use ciborium::value::Value;
use ring::digest::{SHA256, digest};
use x509_parser::prelude::*;

const ROOT_PEM: &[u8] = include_bytes!("../assets/Apple_App_Attestation_Root_CA.pem");
/// Apple's nonce extension.
const NONCE_OID: &str = "1.2.840.113635.100.8.2";
const AAGUID_PRODUCTION: &[u8; 16] = b"appattest\0\0\0\0\0\0\0";
const AAGUID_DEVELOPMENT: &[u8; 16] = b"appattestdevelop";

#[derive(Debug, thiserror::Error)]
pub enum AttestError {
    #[error("malformed attestation: {0}")]
    Malformed(&'static str),
    #[error("certificate chain invalid: {0}")]
    Chain(&'static str),
    #[error("attestation does not match: {0}")]
    Mismatch(&'static str),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Environment {
    Production,
    Development,
    /// No verification at all. Debug builds only; see `AppAttest::new`.
    Disabled,
}

pub struct AppAttest {
    root_der: Vec<u8>,
    app_id_hash: [u8; 32],
    env: Environment,
}

impl AppAttest {
    /// `app_id` is "<TEAM ID>.<bundle id>".
    pub fn new(app_id: &str, env: Environment) -> Self {
        assert!(
            env != Environment::Disabled || cfg!(debug_assertions),
            "App Attest cannot be disabled in a release build"
        );
        let (_, pem) = parse_x509_pem(ROOT_PEM).expect("bundled Apple root CA");
        Self {
            root_der: pem.contents,
            app_id_hash: digest(&SHA256, app_id.as_bytes()).as_ref().try_into().unwrap(),
            env,
        }
    }

    pub fn verify(&self, attestation: &[u8], client_data_hash: &[u8; 32]) -> Result<(), AttestError> {
        if self.env == Environment::Disabled {
            return Ok(());
        }
        let parsed = Attestation::parse(attestation)?;

        // 1. Chain: leaf <- intermediate <- Apple App Attestation Root CA.
        let (_, root) = X509Certificate::from_der(&self.root_der).expect("bundled root parses");
        let (_, leaf) = X509Certificate::from_der(&parsed.leaf)
            .map_err(|_| AttestError::Malformed("leaf certificate"))?;
        let (_, inter) = X509Certificate::from_der(&parsed.intermediate)
            .map_err(|_| AttestError::Malformed("intermediate certificate"))?;
        inter
            .verify_signature(Some(root.public_key()))
            .map_err(|_| AttestError::Chain("intermediate not signed by Apple root"))?;
        leaf.verify_signature(Some(inter.public_key()))
            .map_err(|_| AttestError::Chain("leaf not signed by intermediate"))?;
        if !leaf.validity().is_valid() || !inter.validity().is_valid() {
            return Err(AttestError::Chain("certificate outside validity period"));
        }

        // 2–4. nonce = SHA256(authData || clientDataHash) must equal the value
        // in the leaf's nonce extension: SEQUENCE { [1] { OCTET STRING } }.
        let mut buf = parsed.auth_data.clone();
        buf.extend_from_slice(client_data_hash);
        let nonce = digest(&SHA256, &buf);
        let ext = leaf
            .extensions()
            .iter()
            .find(|e| e.oid.to_id_string() == NONCE_OID)
            .ok_or(AttestError::Mismatch("missing nonce extension"))?;
        let mut expected = vec![0x30, 0x24, 0xA1, 0x22, 0x04, 0x20];
        expected.extend_from_slice(nonce.as_ref());
        if ext.value != expected.as_slice() {
            return Err(AttestError::Mismatch("nonce"));
        }

        // 5. key id = SHA256(leaf public key, uncompressed point).
        let key_id = digest(&SHA256, &leaf.public_key().subject_public_key.data);

        // 6–9. authenticator data.
        let ad = AuthData::parse(&parsed.auth_data)?;
        if ad.rp_id_hash != self.app_id_hash {
            return Err(AttestError::Mismatch("app id"));
        }
        if ad.sign_count != 0 {
            return Err(AttestError::Mismatch("counter must be 0"));
        }
        let aaguid = match self.env {
            Environment::Production => AAGUID_PRODUCTION,
            _ => AAGUID_DEVELOPMENT,
        };
        if ad.aaguid != *aaguid {
            return Err(AttestError::Mismatch("environment (aaguid)"));
        }
        if ad.credential_id != key_id.as_ref() {
            return Err(AttestError::Mismatch("credential id"));
        }
        Ok(())
    }
}

struct Attestation {
    leaf: Vec<u8>,
    intermediate: Vec<u8>,
    auth_data: Vec<u8>,
}

fn map_get<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .map(|(_, v)| v)
}

impl Attestation {
    fn parse(bytes: &[u8]) -> Result<Self, AttestError> {
        let value: Value =
            ciborium::from_reader(bytes).map_err(|_| AttestError::Malformed("cbor"))?;
        let top = value.as_map().ok_or(AttestError::Malformed("not a map"))?;
        if map_get(top, "fmt").and_then(Value::as_text) != Some("apple-appattest") {
            return Err(AttestError::Malformed("fmt"));
        }
        let stmt = map_get(top, "attStmt")
            .and_then(Value::as_map)
            .ok_or(AttestError::Malformed("attStmt"))?;
        let x5c = map_get(stmt, "x5c")
            .and_then(Value::as_array)
            .ok_or(AttestError::Malformed("x5c"))?;
        let cert = |i: usize| {
            x5c.get(i)
                .and_then(Value::as_bytes)
                .cloned()
                .ok_or(AttestError::Malformed("x5c entry"))
        };
        let auth_data = map_get(top, "authData")
            .and_then(Value::as_bytes)
            .cloned()
            .ok_or(AttestError::Malformed("authData"))?;
        Ok(Self {
            leaf: cert(0)?,
            intermediate: cert(1)?,
            auth_data,
        })
    }
}

struct AuthData<'a> {
    rp_id_hash: [u8; 32],
    sign_count: u32,
    aaguid: [u8; 16],
    credential_id: &'a [u8],
}

impl<'a> AuthData<'a> {
    /// rpIdHash(32) | flags(1) | signCount(4) | aaguid(16) | credIdLen(2) | credId | ...
    fn parse(b: &'a [u8]) -> Result<Self, AttestError> {
        const MIN: usize = 32 + 1 + 4 + 16 + 2;
        if b.len() < MIN {
            return Err(AttestError::Malformed("authData too short"));
        }
        let id_len = u16::from_be_bytes([b[53], b[54]]) as usize;
        let credential_id = b
            .get(55..55 + id_len)
            .ok_or(AttestError::Malformed("credential id"))?;
        Ok(Self {
            rp_id_hash: b[..32].try_into().unwrap(),
            sign_count: u32::from_be_bytes(b[33..37].try_into().unwrap()),
            aaguid: b[37..53].try_into().unwrap(),
            credential_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_root_parses() {
        let a = AppAttest::new("TEAMID.com.example.app", Environment::Production);
        let (_, root) = X509Certificate::from_der(&a.root_der).unwrap();
        assert!(root.subject().to_string().contains("Apple App Attestation Root CA"));
    }

    #[test]
    fn rejects_garbage() {
        let a = AppAttest::new("TEAMID.com.example.app", Environment::Production);
        assert!(matches!(a.verify(b"not cbor", &[0; 32]), Err(AttestError::Malformed(_))));
    }

    #[test]
    fn auth_data_layout() {
        let mut b = vec![7u8; 32];
        b.push(0x40);
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(AAGUID_DEVELOPMENT);
        b.extend_from_slice(&3u16.to_be_bytes());
        b.extend_from_slice(&[1, 2, 3]);
        let ad = AuthData::parse(&b).unwrap();
        assert_eq!(ad.rp_id_hash, [7u8; 32]);
        assert_eq!(ad.sign_count, 0);
        assert_eq!(&ad.aaguid, AAGUID_DEVELOPMENT);
        assert_eq!(ad.credential_id, &[1, 2, 3]);
    }
}
