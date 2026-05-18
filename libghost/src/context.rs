use std::collections::HashMap;

use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::codec::Codec;

#[derive(Debug)]
pub struct ZRPContext {
    codecs: HashMap<String, Codec>,
}

impl ZRPContext {
    pub fn new() -> Self {
        Self {
            codecs: HashMap::new(),
        }
    }
}

impl Zeroize for ZRPContext {
    fn zeroize(&mut self) {
        for (_, codec) in self.codecs.iter_mut() {
            codec.zeroize();
        }
        self.codecs.clear();
    }
}

impl Drop for ZRPContext {
    fn drop(&mut self) {
        self.zeroize();
    }
}
