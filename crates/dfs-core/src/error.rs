//! Error types for the distributed file system core.
//!
//! All errors in the core layer implement [`std::error::Error`] via
//! [`thiserror`] and are designed to be forwarded through the client
//! and node layers with minimal transformation.

use std::fmt;

/// Centralized error type for all `dfs-core` operations.
///
/// Errors are categorized by their source layer:
/// - `Encoding` / `Decoding`: erasure coding failures.
/// - `InsufficientShards`: not enough shards to reconstruct.
/// - `ChecksumMismatch`: data corruption detected.
/// - `Ring*`: membership and placement errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    /// Invalid erasure coding configuration (e.g., k=0 or k+m > 255).
    InvalidErasureConfig { k: u8, m: u8 },

    /// The reed-solomon encoder failed (usually a library bug or out-of-memory).
    Encoding(String),

    /// The reed-solomon decoder failed (not enough parity to reconstruct).
    Decoding(String),

    /// Insufficient shards available for reconstruction.
    InsufficientShards { have: usize, need: usize },

    /// Data corruption: the stored data does not match its checksum.
    ChecksumMismatch { expected: [u8; 32], actual: [u8; 32] },

    /// Storage I/O error (delegated from the filesystem layer).
    StorageIo(String),

    /// Serialization or deserialization error (bincode, JSON, etc.).
    Serialization(String),

    /// Ring has no nodes (all nodes have failed or been removed).
    NoNodesLeft,

    /// Too few healthy nodes to satisfy placement requirements.
    InsufficientNodes { needed: usize, available: usize },
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidErasureConfig { k, m } => {
                write!(f, "invalid erasure coding config: k={k}, m={m}")
            }
            Self::Encoding(msg) => write!(f, "encoding failed: {msg}"),
            Self::Decoding(msg) => write!(f, "decoding failed: {msg}"),
            Self::InsufficientShards { have, need } => {
                write!(f, "insufficient shards: have {have}, need {need}")
            }
            Self::ChecksumMismatch { expected, actual } => {
                write!(f, "checksum mismatch: expected {}, got {}",
                       hex::encode(expected), hex::encode(actual))
            }
            Self::StorageIo(msg) => write!(f, "storage I/O error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Self::NoNodesLeft => write!(f, "ring has no nodes left"),
            Self::InsufficientNodes { needed, available } => {
                write!(f, "need {needed} nodes but only {available} available")
            }
        }
    }
}

impl std::error::Error for CoreError {}

impl From<CoreError> for String {
    fn from(e: CoreError) -> Self {
        e.to_string()
    }
}

/// Shorthand for `Result` with [`CoreError`].
pub type Result<T> = std::result::Result<T, CoreError>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn display_formats() {
        let s = CoreError::InvalidErasureConfig { k: 1, m: 2 }.to_string();
        assert!(s.contains("k=1"));
        assert!(s.contains("m=2"));

        let s = CoreError::InsufficientShards { have: 2, need: 3 }.to_string();
        assert!(s.contains("have 2"));
        assert!(s.contains("need 3"));

        let s = CoreError::ChecksumMismatch {
            expected: [0xAA; 32],
            actual: [0xBB; 32],
        }
        .to_string();
        assert!(s.contains("checksum mismatch"));
    }

    #[test]
    fn error_is_std_error() {
        fn _takes_error(_: &dyn std::error::Error) {}
        let e = CoreError::NoNodesLeft;
        _takes_error(&e);
    }

    #[test]
    fn partial_eq_works() {
        assert_eq!(CoreError::NoNodesLeft, CoreError::NoNodesLeft);
        assert_ne!(
            CoreError::InsufficientShards { have: 2, need: 3 },
            CoreError::InsufficientShards { have: 3, need: 3 },
        );
    }
}