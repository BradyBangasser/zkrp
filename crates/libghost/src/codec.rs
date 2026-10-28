use std::any::Any;
use std::error::Error;

use zeroize::Zeroize;

// Content types below 0xF000 belong to applications. libghost's own protocol
// messages live at 0xF000 and up and are never delivered to app handlers, so
// they must not overlap the app range (they used to, and swallowed likes).
pub const CONTENT_TYPE_TEXT: u16 = 0x0001;
/// First content type reserved for libghost.
pub const RESERVED_CONTENT_TYPES: u16 = 0xF000;
pub const CONTENT_TYPE_HELLO: u16 = 0xF001;
/// An anonymous outer message carrying a sealed one (see `crate::sealed`).
pub const CONTENT_TYPE_SEALED: u16 = 0xF002;
pub const CONTENT_TYPE_CHUNK_STORE: u16 = 0xF050;
pub const CONTENT_TYPE_CHUNK_STORE_ACK: u16 = 0xF051;
pub const CONTENT_TYPE_CHUNK_REQUEST: u16 = 0xF052;
pub const CONTENT_TYPE_CHUNK_RESPONSE: u16 = 0xF053;
pub const CONTENT_TYPE_BLOB_MANIFEST: u16 = 0xF054;

type EncoderFn = Box<dyn Fn(&dyn Any) -> Result<Vec<u8>, Box<dyn Error>> + Send + Sync>;
type DecoderFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, Box<dyn Error>> + Send + Sync>;

pub struct Codec {
    pub encode: EncoderFn,
    pub decode: DecoderFn,
    pub id: String,
}

impl Codec {
    pub fn text() -> Self {
        Self {
            id: "text".to_string(),
            encode: Box::new(|any| {
                let s = any.downcast_ref::<String>().ok_or("expected String")?;
                Ok(s.as_bytes().to_vec())
            }),
            decode: Box::new(|bytes| {
                let s = String::from_utf8(bytes.to_vec())?;
                Ok(Box::new(s))
            }),
        }
    }
}

impl std::fmt::Debug for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("Codec {}", self.id))
            .finish_non_exhaustive()
    }
}

impl Zeroize for Codec {
    fn zeroize(&mut self) {
        self.id.zeroize();
    }
}

impl Drop for Codec {
    fn drop(&mut self) {
        self.zeroize();
    }
}
