//! Wire protocol for inter-node communication.
//!
//! All messages are length-prefixed frames over a raw TCP stream.
//! Format (big-endian):
//!
//! ```text
//! ┌─────────┬──────────┬───────────┐
//! │ len:u32 │ kind:u8  │ payload   │
//! │ (BE)    │          │ (bincode) │
//! └─────────┴──────────┴───────────┘
//! ```
//!
//! Messages ≤ 2^32-1 bytes are supported (4 GiB frame limit — enough
//! for a few MB shard at a time; larger payloads would need streaming).
//!
//! # Wire format verification
//!
//! The raw bytes can be inspected with `xxd` or `hexdump` against a
//! TCP capture. The `kind` byte uniquely identifies the request/response
//! type.

use bincode::{deserialize, serialize};
use dfs_core::block::{BlockId, BlockMeta, Shard};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, trace};

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Every message sent between nodes or between client → node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    // ---- Block CRUD (client → node, peer → peer) ----
    /// Store a shard with its metadata. Returns [`Ack::Ok`] or
    /// [`Ack::Error`].
    PutBlock {
        shard: Shard,
        meta: BlockMeta,
    },

    /// Request a shard by ID. Returns [`ResponseBlock`].
    GetBlock {
        block_id: BlockId,
    },

    /// Delete a block. Returns [`Ack::Ok`].
    DelBlock {
        block_id: BlockId,
    },

    /// List all block IDs on this node.
    ListBlocks,

    /// Request block count only (lightweight).
    BlockCount,

    /// Request block metadata only (no data transfer).
    StatBlock {
        block_id: BlockId,
    },

    // ---- Ring / membership (node ↔ node) ----
    /// Join the cluster. The source node tells the target its own id
    /// and asks for the current ring.
    Join {
        node_id: dfs_core::NodeId,
        listen_addr: String,
    },

    /// Response to [`Message::Join`]: the current ring.
    JoinAck {
        ring: dfs_core::Ring,
    },

    /// Gossip message for SWIM membership.
    Gossip(String),

    /// Ping for liveness check.
    Ping,

    // ---- Responses ----
    Pong,
    Ok,
    Error(String),
    BlockList(Vec<BlockId>),
    BlockCount(u64),
    BlockData {
        shard: Shard,
        meta: BlockMeta,
    },
    BlockMeta(BlockMeta),
}

// ---------------------------------------------------------------------------
// Kind bytes — one per variant, makes hexdump legible
// ---------------------------------------------------------------------------

const KIND_PUT_BLOCK: u8 = 0x01;
const KIND_GET_BLOCK: u8 = 0x02;
const KIND_DEL_BLOCK: u8 = 0x03;
const KIND_LIST_BLOCKS: u8 = 0x04;
const KIND_BLOCK_COUNT: u8 = 0x05;
const KIND_STAT_BLOCK: u8 = 0x06;
const KIND_JOIN: u8 = 0x10;
const KIND_JOIN_ACK: u8 = 0x11;
const KIND_GOSSIP: u8 = 0x12;
const KIND_PING: u8 = 0x20;
const KIND_PONG: u8 = 0x21;
const KIND_RESP_OK: u8 = 0x81;
const KIND_RESP_ERR: u8 = 0x82;
const KIND_RESP_BLOCK_LIST: u8 = 0x83;
const KIND_RESP_BLOCK_COUNT: u8 = 0x84;
const KIND_RESP_BLOCK_DATA: u8 = 0x85;
const KIND_RESP_BLOCK_META: u8 = 0x86;

fn kind_byte(msg: &Message) -> u8 {
    match msg {
        Message::PutBlock { .. } => KIND_PUT_BLOCK,
        Message::GetBlock { .. } => KIND_GET_BLOCK,
        Message::DelBlock { .. } => KIND_DEL_BLOCK,
        Message::ListBlocks => KIND_LIST_BLOCKS,
        Message::BlockCount => KIND_BLOCK_COUNT,
        Message::StatBlock { .. } => KIND_STAT_BLOCK,
        Message::Join { .. } => KIND_JOIN,
        Message::JoinAck { .. } => KIND_JOIN_ACK,
        Message::Gossip(_) => KIND_GOSSIP,
        Message::Ping => KIND_PING,
        Message::Pong => KIND_PONG,
        Message::Ok => KIND_RESP_OK,
        Message::Error(_) => KIND_RESP_ERR,
        Message::BlockList(_) => KIND_RESP_BLOCK_LIST,
        Message::BlockCount(_) => KIND_RESP_BLOCK_COUNT,
        Message::BlockData { .. } => KIND_RESP_BLOCK_DATA,
        Message::BlockMeta(_) => KIND_RESP_BLOCK_META,
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// A framed message ready to be serialized onto a TCP stream.
#[derive(Debug)]
pub struct Frame(Vec<u8>);

/// Errors during framing or transmission.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("incomplete frame: expected {expected} bytes, got {got}")]
    Incomplete { expected: usize, got: usize },
    #[error("unknown kind byte: {0:#04x}")]
    UnknownKind(u8),
}

/// Create a length-prefixed frame from a [`Message`].
pub fn frame(msg: &Message) -> Result<Frame, WireError> {
    let payload = serialize(msg)?;
    let total = 1 + payload.len(); // kind byte + bincode body
    let mut frame = Vec::with_capacity(4 + total);
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    frame.push(kind_byte(msg));
    frame.extend_from_slice(&payload);
    Ok(Frame(frame))
}

impl Frame {
    /// Write this frame to an async writer (length + kind + payload).
    pub async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), WireError> {
        writer.write_all(&self.0).await?;
        writer.flush().await?;
        Ok(())
    }
}

/// Read a single length-prefixed frame from an async reader.
pub async fn read_frame_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Message, WireError> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let total = u32::from_be_bytes(header) as usize;

    if total == 0 || total > 512 * 1024 * 1024 {
        // Reject frames > 512 MiB (single shard limit).
        return Err(WireError::Incomplete { expected: total, got: 0 });
    }

    let mut buf = vec![0u8; total];
    reader.read_exact(&mut buf).await?;
    // TODO: verify kind byte matches deserialized enum
    // _kind = buf[0]
    let msg: Message = deserialize(&buf[1..])?;
    Ok(msg)
}

// ---------------------------------------------------------------------------
// Convenience: send one message, read one response
// ---------------------------------------------------------------------------

/// Send a request message and return the response message.
///
///This is a **blocking-in-async** operation: send the request, then
///block until exactly one response frame is fully received.
pub async fn request_response(stream: &mut TcpStream, req: &Message) -> Result<Message, WireError> {
    let frame = frame(req)?;
    frame.write_to(&mut BufWriter::new(&mut *stream)).await?;
    let resp = read_frame_from(&mut BufReader::new(&mut *stream)).await?;
    trace!(?req, ?resp, "request → response");
    Ok(resp)
}

///alen Send a message without waiting for a response.
pub async fn send(stream: &mut TcpStream, msg: &Message) -> Result<(), WireError> {
    frame(msg)?.write_to(&mut BufWriter::new(&mut *stream)).await?;
    Ok(())
}

/// Read exactly one incoming message.
pub async fn recv(stream: &mut TcpStream) -> Result<Message, WireError> {
    read_frame_from(&mut BufReader::new(&mut *stream)).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    /// Encode → decode round-trip without actual socket.
    #[test]
    fn frame_roundtrip() {
        let msg = Message::PutBlock {
            shard: Shard { index: 2, data: b"test data".to_vec() },
            meta: BlockMeta::new("obj", 2, 3, b"test data"),
        };
        let f = frame(&msg).unwrap();
        // parse the frame
        let len = u32::from_be_bytes(f.0[..4].try_into().unwrap()) as usize;
        assert_eq!(len, f.0.len() - 4);
        let _kind = f.0[4];
        let decoded: Message = bincode::deserialize(&f.0[5..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn empty_put_block() {
        let msg = Message::PutBlock {
            shard: Shard { index: 0, data: vec![0u8; 0] },
            meta: BlockMeta::new("empty", 0, 1, b""),
        };
        let f = frame(&msg).unwrap();
        assert!(f.0.len() > 5);
    }
}