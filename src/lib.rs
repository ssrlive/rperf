pub mod args;
pub mod client;
mod error;
pub(crate) mod protocol;
pub mod server;
pub(crate) mod stream;
pub(crate) mod utils;

pub use error::{BoxResult, Error, Result};

/// a global token generator
pub(crate) fn get_global_token() -> mio::Token {
    mio::Token(TOKEN_SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
}
static TOKEN_SEED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
