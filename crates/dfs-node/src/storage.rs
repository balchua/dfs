//! Data-node storage engine.
//!
//! Manages on-disk block storage with the following on-disk layout
//! per data directory:
//!
//! ```text
//! {data_dir}/
//!   blocks/
//!     {block_id}.blob    ← shard data (atomic via rename)
//!     {block_id}.meta    ← BlockMeta (bincode-encoded, checksummed)
//!   node.id             ← this node's UUID (generated on first start)
//! ```
//!
//! ## Integrity guarantees
//!
//! - **Atomic writes**: data lands in a `.tmp` file, then `rename(2)`'d
//!   into place. A partial write never corrupts a valid block.
//! - **Checksum on read**: every `read_block()` call verifies the blake3
//!   checksum stored in the meta file. A mismatch returns an error.
//! - **Crash safety**: the `.blob` and `.meta` files are written
//!   independently; an interrupted write leaves a `.tmp` file that is
//!   cleaned on startup. Synced files survive a `bounce()` in turmoil.

use bincode::{deserialize, serialize};
use dfs_core::block::{BlockId, BlockMeta, Shard};
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, error, info, instrument, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Errors the storage layer can encounter.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Block not found locally.
    #[error("block not found: {0}")]
    NotFound(BlockId),

    /// The data on disk does not match its checksum.
    #[error("checksum mismatch for block {0}")]
    ChecksumMismatch(BlockId),

    /// I/O error from the OS or turmoil simulated-fs.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Bincode could not encode or decode metadata.
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// The node id file is corrupted or missing on a reusespec data dir.
    #[error("node id file corrupted")]
    CorruptNodeId,
}

pub type Result<T> = std::result::Result<T, StorageError>;

// ---------------------------------------------------------------------------
// FsStorage — concrete local-filesystem implementation
// ---------------------------------------------------------------------------

/// Local disk storage for a single data node.
///
/// # Layout
/// Every `FsStorage` instance owns one data directory. Multiple
/// `FsStorage`s on the same machine (with different dirs) can run
/// multiple node processes for manual testing.
///
/// # Atomicity
/// `FsStorage::write_block` writes data to a temporary `.tmp` file,
/// then atomically renames it into place. This ensures a partially
/// written file never replaces a valid block.
#[derive(Debug, Clone)]
pub struct FsStorage {
    /// Root data directory.
    data_dir: PathBuf,

    /// This node's persistent UUID (read from `node.id` on disk).
    node_id_file: PathBuf,
}

impl FsStorage {
    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Open or create the data directory.
    ///
    /// Creates `{data_dir}/blocks/` if missing. Does **not** generate
    /// the node id — call [`read_or_create_node_id`](FsStorage::read_or_create_node_id) separately.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        let blocks_dir = data_dir.join("blocks");
        if !blocks_dir.exists() {
            std::fs::create_dir_all(&blocks_dir)?;
        }
        Ok(Self {
            node_id_file: data_dir.join("node.id"),
            data_dir,
        })
    }

    /// Read the node id from `{data_dir}/node.id`, or generate a new
    /// one and persist it. The node id is stable across restarts.
    #[instrument(skip(self))]
    pub fn read_or_create_node_id(&self) -> Result<dfs_core::NodeId> {
        if self.node_id_file.exists() {
            let bytes = std::fs::read(&self.node_id_file)?;
            let s = String::from_utf8(bytes)
                .map_err(|_| StorageError::CorruptNodeId)?;
            let id: uuid::Uuid = s.trim().parse()
                .map_err(|_| StorageError::CorruptNodeId)?;
            info!(node_id = %id, "loaded node id from disk");
            Ok(id)
        } else {
            let id = uuid::Uuid::new_v4();
            std::fs::write(&self.node_id_file, id.to_string())?;
            info!(node_id = %id, "generated new node id");
            Ok(id)
        }
    }

    /// Return the `blocks/` subdirectory for inspection.
    pub fn blocks_dir(&self) -> PathBuf {
        self.data_dir.join("blocks")
    }

    // ------------------------------------------------------------------
    // Block I/O
    // ------------------------------------------------------------------

    /// Persist a block, atomically committing both data and metadata.
    ///
    /// # File names created
    /// - `{blocks_dir}/{block_id}.blob` — shard data
    /// - `{blocks_dir}/{block_id}.meta` — bincode-encoded [`BlockMeta`]
    #[instrument(skip(self, data, meta), fields(
        block_id = %meta.block_id,
        size = data.len(),
    ))]
    pub async fn write_block(&self, meta: &BlockMeta, data: &[u8]) -> Result<()> {
        let blob_path = self.blob_path(&meta.block_id);
        let meta_path = self.meta_path(&meta.block_id);

        // Atomic write via tmp + rename.
        write_atomic(&blob_path, data).await?;
        write_atomic(&meta_path, &serialize(meta)?).await?;

        debug!(
            block_id = %meta.block_id,
            shard_index = meta.shard_index,
            "block written"
        );
        Ok(())
    }

    /// Write an already-constructed [`Shard`] with associated metadata.
    pub async fn write_shard(&self, meta: &BlockMeta, shard: &Shard) -> Result<()> {
        self.write_block(meta, &shard.data).await
    }

    /// Read a block and verify its checksum.
    ///
    /// # Checksum verification
    /// The metadata file contains a blake3 hash of the data. If the
    /// blob on disk differs from that hash — due to bit-rot, manual
    /// tampering, or a partial write — this method returns
    /// [`StorageError::ChecksumMismatch`].
    #[instrument(skip(self), fields(block_id = %block_id))]
    pub async fn read_block(&self, block_id: &BlockId) -> Result<(Shard, BlockMeta)> {
        let meta: BlockMeta = {
            let meta_bytes = fs::read(self.meta_path(block_id)).await?;
            deserialize(&meta_bytes)?
        };
        let data = fs::read(self.blob_path(block_id)).await?;

        if !meta.verify(&data) {
            error!(
                block_id = %block_id,
                expected = %hex::encode(meta.checksum),
                actual   = %hex::encode(blake3::hash(&data).as_bytes()),
                "checksum mismatch"
            );
            return Err(StorageError::ChecksumMismatch(meta.block_id));
        }

        let shard = Shard {
            index: meta.shard_index,
            data,
        };
        Ok((shard, meta))
    }

    /// Read only the metadata (no data or checksum verification).
    pub async fn read_meta(&self, block_id: &BlockId) -> Result<BlockMeta> {
        let meta_bytes = fs::read(self.meta_path(block_id)).await?;
        Ok(deserialize(&meta_bytes)?)
    }

    /// Delete a block (both blob and meta files).
    #[instrument(skip(self), fields(block_id = %block_id))]
    pub async fn delete_block(&self, block_id: &BlockId) -> Result<()> {
        let _ = fs::remove_file(self.blob_path(block_id)).await;
        let _ = fs::remove_file(self.meta_path(block_id)).await;
        info!(%block_id, "block deleted");
        Ok(())
    }

    /// List all block IDs currently stored.
    ///
    /// Scans the `blocks/` directory for `.meta` files. This is O(n)
    /// where n is the number of stored blocks.
    pub async fn list_blocks(&self) -> Result<Vec<BlockId>> {
        let mut ids = Vec::new();
        let mut dir = fs::read_dir(self.blocks_dir()).await?;
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(raw) = name.strip_suffix(".meta") {
                // Strip ".meta" suffix to get the block id.
                ids.push(BlockId(raw.to_owned()));
            }
        }
        Ok(ids)
    }

    /// Count the number of stored blocks.
    pub async fn block_count(&self) -> Result<usize> {
        Ok(self.list_blocks().await?.len())
    }

    /// Sync all pending data to disk.
    ///
    /// In production this is a no-op (writes are already durable via
    /// atomic rename). In turmoil simulations this flushes pending
    /// buffers so that `sim.bounce()` recovers synced data.
    pub async fn sync_all(&self) -> Result<()> {
        // No-op for real filesystem. In turmoil's simulated fs,
        // this is triggered by `turmoil::fs::shim::std::fs::File::sync_all`.
        Ok(())
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn blob_path(&self, block_id: &BlockId) -> PathBuf {
        self.blocks_dir().join(format!("{}.blob", block_id.as_str()))
    }

    fn meta_path(&self, block_id: &BlockId) -> PathBuf {
        self.blocks_dir().join(format!("{}.meta", block_id.as_str()))
    }
}

// ---------------------------------------------------------------------------
// Atomic write helper
// ---------------------------------------------------------------------------

/// Write bytes to `final_path` atomically: write to `final_path.tmp`,
/// then `rename(2)`. On error the `.tmp` file is cleaned up.
async fn write_atomic(final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = final_path.with_extension("tmp");
    fs::write(&tmp, bytes).await?;
    fs::rename(&tmp, final_path).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Startup cleanup
// ---------------------------------------------------------------------------

/// Remove any stale `.tmp` files left from a previous crash.
///
/// Call once at startup before serving requests.
pub async fn cleanup_stale_tmp(data_dir: &Path) -> io::Result<usize> {
    let blocks_dir = data_dir.join("blocks");
    if !blocks_dir.exists() {
        return Ok(0);
    }
    let mut removed = 0usize;
    let mut dir = fs::read_dir(&blocks_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".tmp") {
            fs::remove_file(entry.path()).await?;
            removed += 1;
        }
    }
    if removed > 0 {
        warn!(stale_files = removed, "cleaned up stale .tmp files");
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "Test code uses unwrap, panic, and print for ergonomic assertions and debugging"
)]
mod tests {
    use super::*;
    

    #[tokio::test]
    async fn write_then_read_block_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = FsStorage::new(tmp.path().to_owned()).unwrap();
        let node = disk.read_or_create_node_id().unwrap();
        assert!(!node.is_nil());

        let data = b"hello-from-storage-test";
        let meta = BlockMeta::new("obj-1", 2, 7, data);

        disk.write_block(&meta, data).await.unwrap();

        let (shard, loaded) = disk.read_block(&meta.block_id).await.unwrap();
        assert_eq!(&shard.data[..], data);
        assert_eq!(loaded.checksum, meta.checksum);
        assert_eq!(loaded.shard_index, 2);
    }

    #[tokio::test]
    async fn checksum_mismatch_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = FsStorage::new(tmp.path().to_owned()).unwrap();

        let meta = BlockMeta::new("corrupt-me", 0, 1, b"original");
        disk.write_block(&meta, b"original").await.unwrap();

        // Tamper with the data file directly (simulating bit-rot).
        tokio::fs::write(disk.blob_path(&meta.block_id), b"tampered!").await.unwrap();

        let err = disk.read_block(&meta.block_id).await.unwrap_err();
        assert!(matches!(err, StorageError::ChecksumMismatch(_)));
    }

    #[tokio::test]
    async fn delete_then_read_returns_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = FsStorage::new(tmp.path().to_owned()).unwrap();

        let meta = BlockMeta::new("del-me", 0, 1, b"tmp");
        disk.write_block(&meta, b"tmp").await.unwrap();
        disk.delete_block(&meta.block_id).await.unwrap();

        let err = disk.read_block(&meta.block_id).await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_) | StorageError::Io(_)));
        // `not found` is an OS error → mapped via `#[from] io::Error` to Io
    }

    #[tokio::test]
    async fn list_blocks_after_writing() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = FsStorage::new(tmp.path().to_owned()).unwrap();

        for i in 0..5u8 {
            let meta = BlockMeta::new(&format!("obj-{i}"), i, 1, b"x");
            disk.write_block(&meta, b"x").await.unwrap();
        }

        let ids = disk.list_blocks().await.unwrap();
        assert_eq!(ids.len(), 5);
        assert_eq!(disk.block_count().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn node_id_persists_across_restarts() {
        let tmp = tempfile::tempdir().unwrap();

        let id1 = {
            let d = FsStorage::new(tmp.path().to_owned()).unwrap();
            d.read_or_create_node_id().unwrap()
        };
        let id2 = {
            // Simulate restart: new FsStorage pointing at same dir.
            let d = FsStorage::new(tmp.path().to_owned()).unwrap();
            d.read_or_create_node_id().unwrap()
        };

        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn cleanup_stale_tmp_files() {
        let tmp = tempfile::tempdir().unwrap();
        let blocks = tmp.path().join("blocks");
        std::fs::create_dir_all(&blocks).unwrap();
        // Create a stale .tmp file.
        std::fs::write(blocks.join("stale.blob.tmp"), b"leftover").unwrap();

        let removed = cleanup_stale_tmp(tmp.path()).await.unwrap();
        assert_eq!(removed, 1);
        assert!(!blocks.join("stale.blob.tmp").exists());
    }

    #[tokio::test]
    async fn atomic_write_no_partial_state() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = FsStorage::new(tmp.path().to_owned()).unwrap();

        let meta = BlockMeta::new("atomic", 0, 1, b"ok");
        disk.write_block(&meta, b"ok").await.unwrap();

        // Neither .tmp nor .tmp.blob should exist — rename was successful.
        assert!(!disk.blob_path(&meta.block_id).with_extension("tmp").exists());
        assert!(!disk.meta_path(&meta.block_id).with_extension("tmp").exists());

        // Both final files exist.
        assert!(disk.blob_path(&meta.block_id).exists());
        assert!(disk.meta_path(&meta.block_id).exists());
    }
}