use std::any::Any;
use std::error::Error;

use zeroize::{Zeroize, ZeroizeOnDrop};

type EncoderFn = Box<dyn Fn(&dyn Any) -> Result<Vec<u8>, Box<dyn Error>> + Send + Sync>;

type DecoderFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, Box<dyn Error>> + Send + Sync>;

pub struct Codec {
    pub encode: EncoderFn,
    pub decode: DecoderFn,
    pub id: String,
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
