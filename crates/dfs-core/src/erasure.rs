//! Erasure coding engine.
//!
//! Splits data into `k` data shards with `m` parity shards using
//! Reed-Solomon encoding. Any subset of `k` shards (out of `k+m`)
//! is sufficient to reconstruct the original data.
//!
//! This module is pure — no I/O, no async, no network. All operations
//! are deterministic functions of their inputs.
//!
//! # Data flow
//! ```text
//! Write:  plaintext → pad → split into k shards → RS encode → (k+m) shards
//! Read:   k shards  → RS reconstruct_data → concat → unpad → original
//! ```
//!
//! Padding: data is zero-padded so its length is a multiple of `k`.
//! Each shard has identical size = `ceil(original_len / k)`.
//! On decode, trailing zeros are stripped (works for payloads that
//! do not end in zero bytes; for production use the original length
//! must be stored in object metadata).

use crate::types::*;
use crate::error::*;
use crate::block::Shard;
use reed_solomon_erasure::galois_8::ReedSolomon;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Reed-Solomon encoder/decoder for 8-bit (GF(256)) fields.
type RsCodec = ReedSolomon;

/// Encode `data` into `k+m` [`Shard`]s.
///
/// Each shard has identical size. The first `k` shards are data shards;
/// the remaining `m` are parity shards.
///
/// # Errors
/// - [`CoreError::Encoding`] if the reed-solomon library fails.
pub fn encode(data: &[u8], config: &ErasureConfig) -> Result<Vec<Shard>> {
    let k = config.k as usize;
    let m = config.m as usize;
    let total = k + m;
    let original_len = data.len();

    // Empty payload: RS rejects zero-length shards. Encoding with k data
    // shards of size 1 each and dropping the single byte on decode.
    let (shard_len, padded) = if original_len == 0 {
        // We cannot pass zero-length shards to RS. Use a single-byte
        // shard andstrip it during decoding.
        (1usize, vec![0u8; k])
    } else {
        let len = original_len.div_ceil(k);
        if len == 0 {
            // original_len > 0 but too small for division (shouldn't happen
            // since original_len > 0 => at least 1 byte → div_ceil(k) >= 1).
            (1usize, make_padded(data, 1, k))
        } else {
            (len, make_padded(data, len, k))
        }
    };

    // Split padded data into k data shards + m empty parity shards.
    let mut shards: Vec<Vec<u8>> = (0..total)
        .map(|i| {
            if i < k {
                let start = i * shard_len;
                padded[start..start + shard_len].to_vec()
            } else {
                vec![0u8; shard_len]
            }
        })
        .collect();

    let encoder = RsCodec::new(k, m)
        .map_err(|e| CoreError::Encoding(e.to_string()))?;

    let (data_shards, parity_shards) = shards.split_at_mut(k);
    encoder
        .encode_sep(data_shards, parity_shards)
        .map_err(|e| CoreError::Encoding(e.to_string()))?;

    Ok(shards
        .into_iter()
        .enumerate()
        .map(|(i, data)| Shard {
            index: i as ShardIndex,
            data,
        })
        .collect())
}

/// Decode `shards` back into the original data.
///
/// Missing or unavailable shards should be passed as `None`.
/// At least `config.k` entries must be [`Some`].
/// Reconstruction uses Reed-Solomon `reconstruct_data`, which only
/// fills in missing data shards (indices 0..k-1).
///
/// # Errors
/// - [`CoreError::InsufficientShards`] if fewer than `k` shards are present.
/// - [`CoreError::Decoding`] if the reed-solomon library fails.
pub fn decode(
    mut shards: Vec<Option<Shard>>,
    config: &ErasureConfig,
) -> Result<Vec<u8>> {
    let total = config.total_shards() as usize;
    let k = config.k as usize;
    let m = config.m as usize;

    shards.resize_with(total, || None);

    let present_count = shards.iter().flatten().count();
    if present_count < k {
        return Err(CoreError::InsufficientShards {
            have: present_count,
            need: k,
        });
    }

    // Extract shard data into Vec<Option<Vec<u8>>> for the RS library.
    let mut slices: Vec<Option<Vec<u8>>> = shards
        .into_iter()
        .map(|s| s.map(|s| s.data))
        .collect();
    slices.resize_with(total, || None);

    let encoder = RsCodec::new(k, m)
        .map_err(|e| CoreError::Decoding(e.to_string()))?;

    // reconstruct_data fills missing DATA shards (indices 0..k-1).
    // `None` slots are initialised to zero-length vectors then filled
    // by the library.
    encoder
        .reconstruct_data(&mut slices[..total])
        .map_err(|e| CoreError::Decoding(e.to_string()))?;

    // Concatenate first k shards — all guaranteed Some post-reconstruction.
    let mut result: Vec<u8> = slices[0..k]
        .iter()
        .flat_map(|s| s.as_ref().unwrap())
        .copied()
        .collect();

    let original_len = effective_len(&result);
    result.truncate(original_len);
    Ok(result)
}

/// Compute padded `Vec<u8>` of size `shard_len * k` from `data`.
fn make_padded(data: &[u8], shard_len: usize, k: usize) -> Vec<u8> {
    let padded_len = shard_len * k;
    let mut padded = vec![0u8; padded_len];
    padded[..data.len()].copy_from_slice(data);
    padded
}

/// Estimate the original data length from padded shards by removing
/// trailing zero bytes.
fn effective_len(data: &[u8]) -> usize {
    if data.is_empty() {
        return 0;
    }
    data.iter().rposition(|&b| b != 0).map_or(0, |p| p + 1)
}

/// Stripe configuration for streaming large payloads.
///
/// A stripe is one EC block: `stripe_size` bytes are split into `k` data
/// shards (each `stripe_size / k` bytes), then `m` parity shards are
/// computed. A large file is processed one stripe at a time to keep
/// per-shard sizes under gRPC message limits.
#[derive(Debug, Clone, Copy)]
pub struct StripeConfig {
    /// Desired stripe payload — total bytes per encode call.
    /// Must be evenly divisible by `k`.
    pub stripe_size: usize,
}

impl StripeConfig {
    /// Pick a stripe size such that each shard fits within `max_shard_bytes`.
    pub fn for_shard_limit(max_shard_bytes: usize, k: u8) -> Self {
        let stripe_size = max_shard_bytes * (k as usize);
        Self { stripe_size }
    }
}

/// Encode `data` in stripes, returning one [`Stripe`] per stripe.
///
/// Each stripe contains `k + m` shards; the first `k` are data shards,
/// the last `m` are parity. The final stripe may be shorter (zero-padded
/// to `stripe_size`).
pub fn encode_striped(data: &[u8], config: &ErasureConfig, stripe: &StripeConfig) -> Vec<Vec<Shard>> {
    let mut stripes = Vec::new();
    for chunk in data.chunks(stripe.stripe_size) {
        let shards = encode(chunk, config)
            .expect("encode_striped: stripe encoding failed (config must be valid)");
        stripes.push(shards);
    }
    stripes
}

/// Decode a list of stripes back into the original data.
///
/// `stripes` is a list where each entry is `Vec<Option<Shard>>` for that stripe.
/// Missing shards in a stripe must be `None`.
pub fn decode_striped(
    stripes: Vec<Vec<Option<Shard>>>,
    config: &ErasureConfig,
    original_len: usize,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(original_len);
    for stripe_shards in stripes {
        let decoded = decode(stripe_shards, config)?;
        result.extend_from_slice(&decoded);
    }
    result.truncate(original_len);
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    const CFG: ErasureConfig = ErasureConfig { k: 3, m: 2 };

    #[test]
    fn encode_decode_roundtrip_all_shards() {
        let shards = encode(b"hello world this is a test", &CFG).unwrap();
        let all: Vec<Option<Shard>> = shards.iter().map(|s| Some(s.clone())).collect();
        let decoded = decode(all, &CFG).unwrap();
        assert_eq!(&decoded[..], b"hello world this is a test");
    }

    #[test]
    fn decode_with_only_data_shards_drops_parity() {
        let shards = encode(b"data only, ignore parity", &CFG).unwrap();
        let subset: Vec<Option<Shard>> = (0..3).map(|i| Some(shards[i].clone())).collect();
        let decoded = decode(subset, &CFG).unwrap();
        assert_eq!(&decoded[..], b"data only, ignore parity");
    }

    #[test]
    fn decode_with_missing_data_shard_using_parity() {
        let shards = encode(b"missng data shard test", &CFG).unwrap();
        // Drop shard 1 (data shard); keep 0, 2, 3, 4.
        // `reconstruct_data` must restore shard 1 from the available shards.
        let mut subset = vec![None; 5];
        for i in [0, 2, 3, 4] {
            subset[i] = Some(shards[i].clone());
        }
        let decoded = decode(subset, &CFG).unwrap();
        assert_eq!(&decoded[..], b"missng data shard test");
    }

    #[test]
    fn decode_with_two_missing_reconstructed() {
        let shards = encode(b"two missing still works", &CFG).unwrap();
        //encoding:encoding 0,2 missing (data), keep 1 (data) + 3,4 (parity) = 3 shards = k.
        let mut subset = vec![None; 5];
        for i in [1, 3, 4] {
            subset[i] = Some(shards[i].clone());
        }
        let decoded = decode(subset, &CFG).unwrap();
        assert_eq!(&decoded[..], b"two missing still works");
    }

    #[test]
    fn decode_with_most_shards_missing_fails() {
        let shards = encode(b"too few to decode", &CFG).unwrap();
        let subset = vec![Some(shards[0].clone()), Some(shards[1].clone())];
        let result = decode(subset, &CFG);
        assert!(matches!(
            result,
            Err(CoreError::InsufficientShards { .. })
        ));
    }

    #[test]
    fn edge_case_single_byte() {
        let cfg = ErasureConfig { k: 4, m: 2 };
        let shards = encode(b"x", &cfg).unwrap();
        let all: Vec<Option<Shard>> = shards.iter().map(|s| Some(s.clone())).collect();
        let decoded = decode(all, &cfg).unwrap();
        assert_eq!(&decoded[..], b"x");
    }

    #[test]
    fn all_shards_same_size() {
        let shards = encode(b"same size test", &CFG).unwrap();
        let sizes: Vec<usize> = shards.iter().map(|s| s.data.len()).collect();
        assert!(
            sizes.windows(2).all(|w| w[0] == w[1]),
            "shards must have equal sizes: {sizes:?}"
        );
    }

    #[test]
    fn total_shard_count_matches_config() {
        let cfg = ErasureConfig { k: 7, m: 3 };
        let shards = encode(&[0u8; 1024], &cfg).unwrap();
        assert_eq!(shards.len() as u8, cfg.total_shards());
    }

    #[test]
    fn encode_decode_empty() {
        let cfg = ErasureConfig { k: 2, m: 1 };
        let shards = encode(b"", &cfg).unwrap();
        let all: Vec<Option<Shard>> = shards.iter().map(|s| Some(s.clone())).collect();
        let decoded = decode(all, &cfg).unwrap();
        assert_eq!(&decoded[..], b"");
    }

    #[test]
    fn payload_not_multiple_of_k() {
        let cfg = ErasureConfig { k: 4, m: 2 };
        // 10 bytes → shard_len = 3 (ceil(10/4)) → padded = 12 bytes.
        let shards = encode(b"0123456789", &cfg).unwrap();
        assert_eq!(shards.len() as u8, cfg.total_shards());
        for s in &shards {
            assert_eq!(s.data.len(), 3);
        }
        let all: Vec<Option<Shard>> = shards.iter().map(|s| Some(s.clone())).collect();
        let decoded = decode(all, &cfg).unwrap();
        assert_eq!(&decoded[..], b"0123456789");
    }

    #[test]
    fn large_payload_roundtrip() {
        let cfg = ErasureConfig { k: 10, m: 4 };
        let data: Vec<u8> = (0u8..=255).cycle().take(1_000_000).collect();
        let shards = encode(&data, &cfg).unwrap();
        // Pick exactly k data shards.
        let subset: Vec<Option<Shard>> = shards[..10]
            .iter()
            .map(|s| Some(s.clone()))
            .collect();
        let decoded = decode(subset, &cfg).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn encode_striped_roundtrip() {
        let cfg = ErasureConfig { k: 3, m: 2 };
        let stripe = StripeConfig::for_shard_limit(128, 3);
        let data = b"this is a test payload for striped encoding!".to_vec();
        let stripes = encode_striped(&data, &cfg, &stripe);
        let reconstructed: Vec<Vec<Option<Shard>>> = stripes
            .iter()
            .map(|ss| ss.iter().map(|s| Some(s.clone())).collect())
            .collect();
        let decoded = decode_striped(reconstructed, &cfg, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn encode_striped_large_payload() {
        let cfg = ErasureConfig { k: 3, m: 2 };
        let stripe = StripeConfig::for_shard_limit(512, 3); // 512 bytes per stripe
        let data: Vec<u8> = (0u8..=127).cycle().take(100_000).collect();
        let stripes = encode_striped(&data, &cfg, &stripe);
        let reconstructed: Vec<Vec<Option<Shard>>> = stripes
            .iter()
            .map(|ss| ss.iter().map(|s| Some(s.clone())).collect())
            .collect();
        let decoded = decode_striped(reconstructed, &cfg, data.len()).unwrap();
        assert_eq!(decoded, data);
    }
}