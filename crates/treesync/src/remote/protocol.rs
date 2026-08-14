//! The wire format spoken between treesync and an agent on the target host.
//!
//! # Shape
//!
//! Strictly request/response over one duplex stream, driven by the client. The
//! transport is a single SSH pipe with one peer at each end, so there is
//! nothing to route and no need for request ids: the reply to a request is the
//! next frame the agent sends. [`apply`](crate::sink::apply) executes a plan in
//! order anyway, and that order is load-bearing, because a directory has to
//! exist before a file lands in it, so pipelining would buy little and cost the
//! ordering guarantee.
//!
//! # Framing
//!
//! Every frame is a little-endian `u32` byte count followed by that many bytes
//! of [bincode]. Self-delimiting, so the reader never has to guess where a
//! message ends, and a corrupt stream fails at the frame boundary rather than
//! being interpreted as a shorter valid message.
//!
//! # Compression
//!
//! The count's high bit says whether the payload is zstd-compressed. It is free
//! to use: `MAX_FRAME` keeps a length far below 2^31, so the header stays
//! four bytes and an uncompressed frame is byte-identical to what it always
//! was.
//!
//! Compression lives here rather than at any one call site so that every
//! payload benefits from one implementation: file content, and an
//! [`Response::Index`] whose repeated path prefixes compress especially well.
//! It is applied only above `COMPRESS_THRESHOLD` and only when the result is
//! actually smaller, because feeding already-compressed content to a
//! compressor returns it slightly larger.
//!
//! # Paths are bytes
//!
//! Not strings. A Unix path is a sequence of non-NUL bytes, and treating one as
//! UTF-8 loses files that are perfectly legal on disk, which, for a mirroring
//! tool, means silently failing to mirror them. [`Vec<u8>`] round-trips
//! whatever the filesystem actually held.
//!
//! # File content
//!
//! Streamed as a series of [`Chunk`] frames rather than declared up front. The
//! obvious alternative, a length in the request header followed by that many
//! raw bytes, desynchronises the whole stream if the file changes size between
//! being stat'd and being read, which for a tree under active write is not a
//! rare case. A self-delimiting chunk sequence cannot desynchronise, and it
//! gives a reader that hits an error mid-file somewhere to say so: see
//! [`Chunk::Abort`].

use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::reconcile::{Entry, Filter, Index, IndexOptions, Metadata, Preserve, Scope, Verify};
use crate::remote::delta::{self, BlockSig, Signature};

/// Bumped whenever a change would make two versions misread each other.
///
/// Checked in the opening handshake, so a mismatch is one clear error at
/// startup rather than a decode failure partway through a transfer. It is also
/// what makes shipping the agent safe to repeat: an agent already on the host
/// that answers with a different version is replaced rather than trusted.
pub const PROTOCOL_VERSION: u32 = 2;

/// How much file content one [`Chunk::Data`] frame carries.
///
/// Sized for the transport rather than for memory: SSH moves data in its own
/// 32 KiB-ish channel windows, so smaller chunks add round trips without
/// saving anything, and larger ones stop paying for themselves.
pub const CHUNK_SIZE: usize = 256 * 1024;

/// Largest frame that will be read.
///
/// A guard against a corrupt or truncated stream turning a garbage length into
/// a multi-gigabyte allocation, not a security boundary against the agent.
/// the agent is a binary this client shipped to a host the operator chose.
///
/// The one message that can genuinely approach it is an [`Response::Index`] of
/// a very large tree, at roughly a hundred bytes per entry. Sixty-four
/// mebibytes is on the order of half a million files; past that the index
/// wants streaming rather than a bigger number here.
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Payloads at or above this are put through zstd.
///
/// Below it the compressor's own framing costs more than it saves, and every
/// small control frame would pay for a round of work that cannot help it.
const COMPRESS_THRESHOLD: usize = 4 * 1024;

/// The zstd level frames are compressed at.
///
/// Three is the knee of the curve for this payload: JSON content still
/// compresses several-fold, and the compressor keeps up with a link rather than
/// replacing it as the bottleneck.
const COMPRESS_LEVEL: i32 = 3;

/// Set in the length prefix when the payload that follows is compressed.
const COMPRESSED_FLAG: u32 = 1 << 31;

/// A request from the client to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Opens the session. Must be the first frame.
    Hello {
        version: u32,
    },

    /// Reports what the target holds within `scope`.
    Index {
        scope: WireScope,
        /// Patterns, not a compiled matcher: the agent rebuilds the filter so
        /// both trees are indexed under identical exclusions.
        exclude: Vec<String>,
        verify: WireVerify,
    },

    CreateDir {
        path: WirePath,
    },

    /// Opens a file transfer. Followed by [`Chunk`] frames.
    WriteFile {
        path: WirePath,
    },

    CreateSymlink {
        path: WirePath,
        target: WirePath,
    },

    Remove {
        path: WirePath,
    },

    Rename {
        from: WirePath,
        to: WirePath,
    },

    SetMetadata {
        path: WirePath,
        metadata: WireMetadata,
        preserve: WirePreserve,
    },

    /// Ends the session cleanly. The agent replies [`Response::Ok`] and exits.
    Goodbye,

    // Added in protocol 2, below the variants protocol 1 already had. See the
    // note on [`Response`]: a variant's position *is* its encoding, so
    // inserting one renumbers the rest.
    //
    // Nothing depends on this ordering in the way `Response` does, since an
    // old agent rejects the handshake before it ever decodes one of these, but
    // keeping the rule uniform means it does not have to be re-derived.
    /// Asks the agent to describe what it already holds at `path`, block by
    /// block, so the client can send only what differs.
    ///
    /// The signature is computed on the target, which is the point: the bytes
    /// being described never cross the link, only ~20 bytes per block do.
    Signature {
        path: WirePath,
        block_size: u32,
    },

    /// Opens a delta transfer. Followed by [`Token`] frames.
    ///
    /// Separate from [`Request::WriteFile`] rather than a mode of it: a patch
    /// reads the target's existing file, and a request that can do that is
    /// worth being able to see in a log as the distinct thing it is.
    ///
    /// `resume_from` is how many bytes of the reconstruction the agent already
    /// holds from an interrupted attempt, and so where this stream picks up.
    /// Zero starts clean, discarding whatever was there.
    PatchFile {
        path: WirePath,
        resume_from: u64,
    },

    /// Asks how much of an interrupted transfer survived, and what it hashes
    /// to.
    ///
    /// The hash is the point. Resuming onto bytes that are merely *present*
    /// would mean trusting that they are the ones this file has now; the client
    /// hashes the same prefix of its own source and only continues if the two
    /// agree. That turns resumption from an assumption into a check.
    ResumeState {
        path: WirePath,
    },
}

/// One frame of a file transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Chunk {
    /// Content, in order. Never empty: an empty file sends no `Data` at all.
    Data(Vec<u8>),

    /// Ends the transfer: stamp `mtime` and move the file into place.
    ///
    /// The timestamp is sent at the end rather than in the header because it
    /// is read from the source *after* the content, matching what a local copy
    /// does. A file rewritten during the transfer therefore arrives stamped
    /// with the newer mtime and is caught by the next pass instead of looking
    /// settled.
    Commit { mtime: WireTime },

    /// Ends the transfer without publishing anything.
    ///
    /// Sent when the client cannot finish reading the source. The file was
    /// removed mid-copy, or turned unreadable. The agent discards its
    /// temporary and reports the reason, so a half-file is never renamed over
    /// good content on the target.
    Abort { reason: String },
}

/// One frame of a delta transfer.
///
/// The stream reconstructs the file as a sequence of "reuse what you already
/// have" and "here are bytes you do not". Ordering is the whole content of the
/// message, since token *n* describes the bytes that follow token *n-1*'s, so
/// stream, like the rest of the protocol, is strictly sequential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Token {
    /// Reuse `len` bytes of the target's existing file, starting at `offset`.
    ///
    /// A byte range rather than a block index and count, for two reasons. It
    /// coalesces an unchanged region into one token, so a 1.5 GB run is one of
    /// these and not twenty-four thousand, and it can be split at *any* byte,
    /// which is what makes a transfer resumable from wherever it stopped
    /// rather than only at a block boundary. It also means the agent never
    /// needs to know the block size to interpret a stream.
    Copy { offset: u64, len: u64 },

    /// Bytes the target does not have. Never empty.
    Literal(Vec<u8>),

    /// Ends the transfer: stamp `mtime` and publish.
    ///
    /// `hash` is BLAKE3 of the *whole* source file, computed by the client as
    /// it read it. The agent hashes what it reconstructed and refuses to
    /// publish on a mismatch, which makes the transfer verified end to end
    /// instead of merely delivered. That covers a bad block read back from the
    /// target's own copy, a bug in reconstruction, and corruption on disk.
    /// Corruption *in flight* is already caught by SSH's transport MAC.
    Commit {
        mtime: WireTime,
        hash: [u8; blake3::OUT_LEN],
    },

    /// Ends the transfer without publishing anything.
    Abort { reason: String },
}

/// A reply from the agent to the client.
///
/// # New variants go at the end
///
/// bincode encodes a variant as its position, so inserting one renumbers
/// everything below it. That matters for exactly one exchange: a new client
/// meeting an agent still running an old build. The old agent answers the
/// handshake with `Error`, and if `Error` has moved the client cannot decode
/// the very frame that explains the problem. It reports a mangled message
/// instead of "this agent speaks protocol 1". Keeping the original order means
/// the version mismatch always reads correctly, and the agent is then replaced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Hello {
        version: u32,
        /// The agent's own build, for logs. Never trusted for compatibility;
        /// that is `version`'s job.
        build: String,
    },
    Ok,
    Index(WireIndex),
    Error {
        kind: WireErrorKind,
        message: String,
    },

    // Added in protocol 2, below the variants protocol 1 already had.
    Signature(WireSignature),

    /// What an interrupted transfer left behind: how many bytes, and their
    /// BLAKE3. `bytes` of zero means there is nothing to resume onto.
    ResumeState {
        bytes: u64,
        hash: [u8; blake3::OUT_LEN],
    },
}

/// A path, as the bytes the filesystem actually holds.
///
/// A newtype rather than a bare `Vec<u8>` so the conversions live in one place
/// and a plain byte buffer cannot be passed where a path belongs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WirePath(pub Vec<u8>);

impl WirePath {
    pub fn new(path: &Path) -> Self {
        Self(path.as_os_str().as_bytes().to_vec())
    }

    pub fn into_path(self) -> PathBuf {
        PathBuf::from(OsString::from_vec(self.0))
    }
}

/// A `SystemTime` as an offset from the Unix epoch.
///
/// Signed, because a timestamp before 1970 is unusual but entirely
/// representable on disk, and clamping one to the epoch would make the file
/// differ on every pass and be copied forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTime {
    pub secs: i64,
    pub nanos: u32,
}

impl WireTime {
    pub fn new(time: SystemTime) -> Self {
        let nanos = match time.duration_since(UNIX_EPOCH) {
            Ok(since) => since.as_nanos() as i128,
            Err(before) => -(before.duration().as_nanos() as i128),
        };

        Self {
            secs: (nanos.div_euclid(1_000_000_000)) as i64,
            // Euclidean, so the remainder is never negative and the pair always
            // reads as "this many whole seconds, plus this far into the next".
            nanos: (nanos.rem_euclid(1_000_000_000)) as u32,
        }
    }

    pub fn into_system_time(self) -> SystemTime {
        let total = self.secs as i128 * 1_000_000_000 + self.nanos as i128;

        if total >= 0 {
            UNIX_EPOCH + Duration::from_nanos(total as u64)
        } else {
            UNIX_EPOCH - Duration::from_nanos((-total) as u64)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireScope {
    Paths(Vec<WirePath>),
    Subtree(WirePath),
}

impl WireScope {
    pub fn new(scope: &Scope) -> Self {
        match scope {
            Scope::Paths(paths) => {
                Self::Paths(paths.iter().map(|path| WirePath::new(path)).collect())
            }
            Scope::Subtree(prefix) => Self::Subtree(WirePath::new(prefix)),
        }
    }

    pub fn into_scope(self) -> Scope {
        match self {
            Self::Paths(paths) => {
                Scope::Paths(paths.into_iter().map(WirePath::into_path).collect())
            }
            Self::Subtree(prefix) => Scope::Subtree(prefix.into_path()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireVerify {
    Quick,
    Checksum,
}

impl WireVerify {
    pub fn new(verify: Verify) -> Self {
        match verify {
            Verify::Quick => Self::Quick,
            Verify::Checksum => Self::Checksum,
        }
    }

    pub fn into_verify(self) -> Verify {
        match self {
            Self::Quick => Verify::Quick,
            Self::Checksum => Verify::Checksum,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WirePreserve {
    pub mode: bool,
    pub ownership: bool,
}

impl WirePreserve {
    pub fn new(preserve: Preserve) -> Self {
        Self {
            mode: preserve.mode,
            ownership: preserve.ownership,
        }
    }

    pub fn into_preserve(self) -> Preserve {
        Preserve {
            mode: self.mode,
            ownership: self.ownership,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireMetadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl WireMetadata {
    pub fn new(metadata: &Metadata) -> Self {
        Self {
            mode: metadata.mode,
            uid: metadata.uid,
            gid: metadata.gid,
        }
    }

    pub fn into_metadata(self) -> Metadata {
        Metadata {
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireEntry {
    File {
        size: u64,
        mtime: WireTime,
        /// Present only under [`Verify::Checksum`]. Raw bytes rather than the
        /// hex form, which would be twice the size for no benefit.
        hash: Option<[u8; blake3::OUT_LEN]>,
        meta: WireMetadata,
    },
    Dir {
        meta: WireMetadata,
    },
    Symlink {
        target: WirePath,
    },
}

impl WireEntry {
    pub fn new(entry: &Entry) -> Self {
        match entry {
            Entry::File {
                size,
                mtime,
                hash,
                meta,
            } => Self::File {
                size: *size,
                mtime: WireTime::new(*mtime),
                hash: hash.map(|hash| *hash.as_bytes()),
                meta: WireMetadata::new(meta),
            },
            Entry::Dir { meta } => Self::Dir {
                meta: WireMetadata::new(meta),
            },
            Entry::Symlink { target } => Self::Symlink {
                target: WirePath::new(target),
            },
        }
    }

    pub fn into_entry(self) -> Entry {
        match self {
            Self::File {
                size,
                mtime,
                hash,
                meta,
            } => Entry::File {
                size,
                mtime: mtime.into_system_time(),
                hash: hash.map(blake3::Hash::from_bytes),
                meta: meta.into_metadata(),
            },
            Self::Dir { meta } => Entry::Dir {
                meta: meta.into_metadata(),
            },
            Self::Symlink { target } => Entry::Symlink {
                target: target.into_path(),
            },
        }
    }
}

/// A block signature of the target's existing file.
///
/// Flat pairs rather than a struct per block: at roughly twenty bytes each and
/// tens of thousands of blocks for a large file, this is the one message whose
/// encoding overhead is worth caring about.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireSignature {
    pub block_size: u32,
    /// Weak rolling checksum paired with a truncated BLAKE3.
    pub blocks: Vec<(u32, [u8; delta::STRONG_LEN])>,
}

impl WireSignature {
    pub fn new(signature: &Signature) -> Self {
        Self {
            block_size: signature.block_size,
            blocks: signature
                .blocks
                .iter()
                .map(|block| (block.weak, block.strong))
                .collect(),
        }
    }

    pub fn into_signature(self) -> Signature {
        Signature {
            block_size: self.block_size,
            blocks: self
                .blocks
                .into_iter()
                .map(|(weak, strong)| BlockSig { weak, strong })
                .collect(),
        }
    }
}

/// An index, flattened to a list.
///
/// A list rather than a map: it is built and consumed in one pass on each side,
/// so the hashing a map would do on the wire is work neither end needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireIndex {
    pub entries: Vec<(WirePath, WireEntry)>,
}

impl WireIndex {
    pub fn new(index: &Index) -> Self {
        Self {
            entries: index
                .iter()
                .map(|(path, entry)| (WirePath::new(path), WireEntry::new(entry)))
                .collect(),
        }
    }

    pub fn into_index(self) -> Index {
        let mut index = Index::new();

        for (path, entry) in self.entries {
            index.insert(path.into_path(), entry.into_entry());
        }

        index
    }
}

/// The categories of [`Error`] that survive a trip over the wire.
///
/// Deliberately not the whole enum: [`Error::Io`] carries an
/// `io::Error` whose `ErrorKind` is the only part a caller branches on, and
/// that is what [`WireErrorKind::Io`] preserves. The point is that a caller can
/// still tell "the file was not there" from "the file could not be read" after
/// the error crossed a process boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireErrorKind {
    NotFound,
    PermissionDenied,
    AlreadyExists,
    InvalidPath,
    Config,
    Io,
    Unsupported,
    Internal,
}

impl WireErrorKind {
    pub fn of(error: &Error) -> Self {
        match error {
            Error::NotFound(_) => Self::NotFound,
            Error::PermissionDenied(_) => Self::PermissionDenied,
            Error::AlreadyExists(_) => Self::AlreadyExists,
            Error::InvalidPath(_) => Self::InvalidPath,
            Error::Config(_) => Self::Config,
            Error::Io(_) => Self::Io,
            Error::Unsupported(_) => Self::Unsupported,
            Error::Internal(_) => Self::Internal,
        }
    }

    /// Rebuilds an error, tagging the message so a log makes clear which side
    /// of the connection actually failed.
    pub fn into_error(self, message: String) -> Error {
        let message = format!("agent: {message}");

        match self {
            Self::NotFound => Error::NotFound(message),
            Self::PermissionDenied => Error::PermissionDenied(message),
            Self::AlreadyExists => Error::AlreadyExists(message),
            Self::InvalidPath => Error::InvalidPath(message),
            Self::Config => Error::Config(message),
            Self::Io => Error::Io(std::io::Error::other(message)),
            Self::Unsupported => Error::Unsupported(message),
            Self::Internal => Error::Internal(message),
        }
    }
}

impl Response {
    /// Turns an agent-side failure into the frame that reports it.
    pub fn from_error(error: &Error) -> Self {
        Self::Error {
            kind: WireErrorKind::of(error),
            message: error.to_string(),
        }
    }

    /// Collapses an `Error` response back into an `Err`, leaving the rest.
    pub fn into_result(self) -> Result<Self> {
        match self {
            Self::Error { kind, message } => Err(kind.into_error(message)),
            other => Ok(other),
        }
    }
}

/// Rebuilds index options from what the wire carries.
pub fn index_options(exclude: &[String], verify: WireVerify) -> Result<IndexOptions> {
    Ok(IndexOptions {
        filter: Filter::new(exclude)?,
        verify: verify.into_verify(),
    })
}

fn codec() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Writes one length-prefixed frame and flushes it.
///
/// The flush is not optional: a request sitting in a buffer while the client
/// waits for its reply is a deadlock, and both ends of this protocol take
/// turns.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let framed = encode_frame(value)?;

    send_frame(writer, &framed).await
}

/// Turns a value into the bytes of one frame, header included.
///
/// Split out from [`write_frame`] so a caller can tell "this message cannot be
/// sent" from "the connection is gone". The two need opposite handling: the
/// first is a reply that has to be replaced with one explaining itself, and the
/// second means there is nobody left to explain anything to.
///
/// Nothing is written here, so a value refused by this function has put no bytes
/// on the wire and the stream is still exactly where it was.
pub fn encode_frame<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let payload = bincode::serde::encode_to_vec(value, codec())
        .map_err(|err| Error::Internal(format!("encoding a frame: {err}")))?;

    if payload.len() > MAX_FRAME {
        return Err(Error::Internal(format!(
            "a message of {} bytes exceeds the {MAX_FRAME} byte frame limit. \
             The one message that reaches this is the index of a very large \
             tree, at roughly a hundred bytes per entry; narrow it with \
             `exclude` or split the tree across several syncs",
            payload.len()
        )));
    }

    // The length is checked against the limit before compression, not after:
    // the limit exists to bound what either side has to hold in memory, and
    // that is the decoded size on both ends.
    let (body, compressed) = match compress(&payload) {
        Some(smaller) => (smaller, true),
        None => (payload, false),
    };

    let header = body.len() as u32 | if compressed { COMPRESSED_FLAG } else { 0 };

    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&header.to_le_bytes());
    framed.extend_from_slice(&body);

    Ok(framed)
}

/// Writes bytes produced by [`encode_frame`] and flushes them.
///
/// The flush is not optional: a request sitting in a buffer while the client
/// waits for its reply is a deadlock, and both ends of this protocol take turns.
pub async fn send_frame<W>(writer: &mut W, framed: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(framed).await.map_err(Error::from)?;
    writer.flush().await.map_err(Error::from)?;

    Ok(())
}

/// Compresses a payload, or declines to.
///
/// Returns `None` when the payload is too small to be worth it or when zstd
/// made it bigger, which is what happens to content that is already compressed.
/// Sending that would make the wire strictly worse than not trying.
///
/// A compressor failure is also `None` rather than an error: the frame is still
/// perfectly sendable uncompressed, and failing a transfer over an optimisation
/// would be the wrong trade.
fn compress(payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() < COMPRESS_THRESHOLD {
        return None;
    }

    let compressed = zstd::bulk::compress(payload, COMPRESS_LEVEL).ok()?;

    (compressed.len() < payload.len()).then_some(compressed)
}

/// Reads one length-prefixed frame.
///
/// A clean end of stream is `Ok(None)`, not an error: it is how the agent
/// observes the client hanging up, which is a normal way for a session to end.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; 4];

    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(Error::from(err)),
    }

    let header = u32::from_le_bytes(header);
    let compressed = header & COMPRESSED_FLAG != 0;
    let length = (header & !COMPRESSED_FLAG) as usize;

    if length > MAX_FRAME {
        return Err(Error::Internal(format!(
            "peer announced a {length} byte frame, over the {MAX_FRAME} byte limit; \
             the stream is corrupt or the peer is not an agent"
        )));
    }

    let mut body = vec![0u8; length];
    // Distinct from the header case above: a stream that ends *inside* a frame
    // is a truncated message, not a clean hangup.
    reader.read_exact(&mut body).await.map_err(|err| {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Internal(format!("stream ended {length} bytes into a frame"))
        } else {
            Error::from(err)
        }
    })?;

    let payload = if compressed {
        // Bounded explicitly rather than trusting the frame. A compressed
        // length says nothing about what it expands to, so a corrupt or
        // truncated stream could otherwise turn a few bytes into an unbounded
        // allocation, the one hazard compression adds to this framing.
        zstd::bulk::decompress(&body, MAX_FRAME)
            .map_err(|err| Error::Internal(format!("decompressing a frame: {err}")))?
    } else {
        body
    };

    let (value, _) = bincode::serde::decode_from_slice(&payload, codec())
        .map_err(|err| Error::Internal(format!("decoding a frame: {err}")))?;

    Ok(Some(value))
}

/// Reads a frame, treating a clean end of stream as a failure.
///
/// For the points where a reply is required. The peer going away mid-exchange
/// is an error there, not a hangup.
pub async fn expect_frame<R, T>(reader: &mut R, expecting: &str) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_frame(reader).await?.ok_or_else(|| {
        Error::Internal(format!(
            "the connection closed while waiting for {expecting}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip_time(time: SystemTime) -> SystemTime {
        WireTime::new(time).into_system_time()
    }

    #[test]
    fn a_timestamp_survives_the_round_trip() {
        let time = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);

        assert_eq!(round_trip_time(time), time);
    }

    #[test]
    fn the_epoch_survives_the_round_trip() {
        assert_eq!(round_trip_time(UNIX_EPOCH), UNIX_EPOCH);
    }

    #[test]
    fn a_pre_epoch_timestamp_survives_the_round_trip() {
        // Legal on disk, and clamping it to the epoch would make the file look
        // different on every pass and be copied forever.
        let time = UNIX_EPOCH - Duration::new(86_400, 0) + Duration::from_nanos(500);

        assert_eq!(round_trip_time(time), time);
    }

    #[test]
    fn a_path_that_is_not_utf8_survives_the_round_trip() {
        // Perfectly legal on disk. Routing paths through String would drop this
        // file from the mirror without ever reporting it.
        let raw = OsString::from_vec(vec![b'a', 0xff, 0xfe, b'/', b'b']);
        let path = PathBuf::from(raw);

        assert_eq!(WirePath::new(&path).into_path(), path);
    }

    #[test]
    fn a_path_containing_a_newline_survives_the_round_trip() {
        let path = PathBuf::from("dir/two\nlines.txt");

        assert_eq!(WirePath::new(&path).into_path(), path);
    }

    #[test]
    fn an_index_survives_the_round_trip() {
        let mut index = Index::new();
        index.insert(
            "a.txt",
            Entry::File {
                size: 42,
                mtime: UNIX_EPOCH + Duration::from_secs(99),
                hash: Some(blake3::hash(b"contents")),
                meta: Metadata {
                    mode: 0o644,
                    uid: 1,
                    gid: 2,
                },
            },
        );
        index.insert(
            "sub",
            Entry::Dir {
                meta: Metadata {
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                },
            },
        );
        index.insert(
            "link",
            Entry::Symlink {
                target: PathBuf::from("/elsewhere"),
            },
        );

        assert_eq!(WireIndex::new(&index).into_index(), index);
    }

    #[tokio::test]
    async fn a_frame_survives_the_round_trip() {
        let mut buffer = Vec::new();
        let request = Request::CreateDir {
            path: WirePath::new(Path::new("a/b")),
        };

        write_frame(&mut buffer, &request).await.expect("write");

        let mut reader = Cursor::new(buffer);
        let decoded: Option<Request> = read_frame(&mut reader).await.expect("read");

        assert_eq!(decoded, Some(request));
    }

    #[tokio::test]
    async fn frames_are_read_back_in_order() {
        let mut buffer = Vec::new();

        for name in ["a", "b", "c"] {
            write_frame(
                &mut buffer,
                &Request::Remove {
                    path: WirePath::new(Path::new(name)),
                },
            )
            .await
            .expect("write");
        }

        let mut reader = Cursor::new(buffer);
        let mut seen = Vec::new();

        while let Some(Request::Remove { path }) = read_frame(&mut reader).await.expect("read") {
            seen.push(String::from_utf8(path.0).expect("ascii"));
        }

        assert_eq!(seen, vec!["a", "b", "c"]);
    }

    /// The compression flag and on-wire length, read straight off a frame.
    fn frame_header(buffer: &[u8]) -> (bool, usize) {
        let header = u32::from_le_bytes(buffer[..4].try_into().expect("a four byte header"));

        (
            header & COMPRESSED_FLAG != 0,
            (header & !COMPRESSED_FLAG) as usize,
        )
    }

    #[tokio::test]
    async fn a_compressible_frame_is_compressed_and_survives_it() {
        // Shaped like the payload this exists for.
        let content = br#"{"id":1,"name":"treesync","values":[0,0,0,0,0,0,0,0]}"#.repeat(2000);
        let chunk = Chunk::Data(content.clone());

        let mut buffer = Vec::new();
        write_frame(&mut buffer, &chunk).await.expect("write");

        let (compressed, length) = frame_header(&buffer);

        assert!(compressed, "repetitive content should compress");
        assert!(
            length < content.len() / 2,
            "expected a real saving; {length} bytes on the wire from {}",
            content.len()
        );

        let mut reader = Cursor::new(buffer);
        let decoded: Option<Chunk> = read_frame(&mut reader).await.expect("read");

        assert_eq!(decoded, Some(chunk), "compression must be lossless");
    }

    #[tokio::test]
    async fn an_incompressible_frame_is_sent_as_it_is() {
        // Hash output, so genuinely incompressible. Stands in for content that
        // arrives already compressed, which an unconditional compressor would
        // quietly make *larger*, and at these file sizes that is the common
        // case rather than the exotic one.
        let noise: Vec<u8> = (0..1024u32)
            .flat_map(|i| *blake3::hash(&i.to_le_bytes()).as_bytes())
            .collect();
        let chunk = Chunk::Data(noise.clone());

        let mut buffer = Vec::new();
        write_frame(&mut buffer, &chunk).await.expect("write");

        let (compressed, length) = frame_header(&buffer);

        assert!(
            !compressed,
            "incompressible content must be left alone, not sent expanded"
        );
        assert!(
            length >= noise.len(),
            "the payload should have gone out whole"
        );

        let mut reader = Cursor::new(buffer);
        let decoded: Option<Chunk> = read_frame(&mut reader).await.expect("read");

        assert_eq!(decoded, Some(chunk));
    }

    #[tokio::test]
    async fn a_small_frame_is_not_compressed() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, &Request::Goodbye)
            .await
            .expect("write");

        let (compressed, _) = frame_header(&buffer);

        assert!(
            !compressed,
            "a control frame is far too small for compression to pay"
        );
    }

    #[tokio::test]
    async fn a_clean_end_of_stream_is_not_an_error() {
        let mut reader = Cursor::new(Vec::new());

        let frame: Option<Request> = read_frame(&mut reader).await.expect("read");

        assert!(frame.is_none(), "a hangup is how a session normally ends");
    }

    #[tokio::test]
    async fn a_truncated_frame_is_an_error() {
        let mut buffer = Vec::new();
        write_frame(
            &mut buffer,
            &Request::Remove {
                path: WirePath::new(Path::new("a")),
            },
        )
        .await
        .expect("write");

        buffer.truncate(buffer.len() - 1);
        let mut reader = Cursor::new(buffer);

        let result: Result<Option<Request>> = read_frame(&mut reader).await;

        assert!(
            result.is_err(),
            "a half-message must not be mistaken for a hangup"
        );
    }

    #[tokio::test]
    async fn an_absurd_length_is_refused_before_allocating() {
        let mut buffer = u32::MAX.to_le_bytes().to_vec();
        buffer.extend_from_slice(b"nonsense");
        let mut reader = Cursor::new(buffer);

        let result: Result<Option<Request>> = read_frame(&mut reader).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_frame_of_the_wrong_type_costs_one_frame_and_not_the_stream() {
        // A request where a chunk belongs, which is what a desynchronised or
        // malformed peer produces. Whether it decodes as some other chunk or
        // fails to decode at all is not the point: the length prefix says how
        // far this message runs, so exactly one message is spent either way and
        // the one behind it is untouched.
        let mut buffer = Vec::new();
        write_frame(
            &mut buffer,
            &Request::Remove {
                path: WirePath::new(Path::new("a")),
            },
        )
        .await
        .expect("write");
        write_frame(
            &mut buffer,
            &Chunk::Commit {
                mtime: WireTime::new(UNIX_EPOCH),
            },
        )
        .await
        .expect("write");

        let mut reader = Cursor::new(buffer);

        // Read as the wrong type. Discarded: any outcome is acceptable here.
        let _: Result<Option<Chunk>> = read_frame(&mut reader).await;

        let next: Option<Chunk> = read_frame(&mut reader)
            .await
            .expect("the frame behind a bad one has to still be readable");

        assert_eq!(
            next,
            Some(Chunk::Commit {
                mtime: WireTime::new(UNIX_EPOCH)
            }),
            "self-delimiting framing is what keeps one bad message from costing \
             the session; without it the reader would resume mid-message and \
             every frame after it would be garbage"
        );
    }

    #[tokio::test]
    async fn a_frame_that_expands_past_the_limit_is_refused() {
        // Compression is the one thing that lets a small message become a large
        // allocation: the length on the wire says nothing about what it expands
        // to. A few kilobytes of zeros unpack to more than the limit, and the
        // bound passed to the decompressor is the only thing between a peer and
        // this process's memory.
        let bomb =
            zstd::bulk::compress(&vec![0u8; MAX_FRAME + 1], COMPRESS_LEVEL).expect("compress");

        assert!(
            bomb.len() < 1024 * 1024,
            "the point is that the wire cost is nothing like the memory cost; \
             got {} bytes on the wire",
            bomb.len()
        );

        let header = bomb.len() as u32 | COMPRESSED_FLAG;
        let mut buffer = header.to_le_bytes().to_vec();
        buffer.extend_from_slice(&bomb);

        let mut reader = Cursor::new(buffer);
        let result: Result<Option<Chunk>> = read_frame(&mut reader).await;

        assert!(
            result.is_err(),
            "a frame that unpacks past the limit has to be refused rather than \
             allocated"
        );
    }

    #[test]
    fn a_message_too_large_to_send_is_refused_before_anything_is_written() {
        // The property the agent's reply path depends on: a value this refuses
        // has put no bytes on the wire, so the stream is still exactly where it
        // was and an error frame can go out in its place.
        let mut index = WireIndex::default();

        // A handful of very large paths rather than the half a million ordinary
        // entries it would otherwise take, which is the same encoded size and
        // builds in an instant.
        for _ in 0..70 {
            index.entries.push((
                WirePath(vec![b'a'; 1024 * 1024]),
                WireEntry::Dir {
                    meta: WireMetadata {
                        mode: 0o755,
                        uid: 0,
                        gid: 0,
                    },
                },
            ));
        }

        let result = encode_frame(&Response::Index(index));

        assert!(
            result.is_err(),
            "a message over the limit has to be refused"
        );
        assert!(
            result.unwrap_err().to_string().contains("exclude"),
            "and the error has to say what an operator can do about it"
        );
    }

    #[test]
    fn an_error_keeps_its_category_across_the_wire() {
        let original = Error::PermissionDenied("/srv/app/secret".to_string());

        let response = Response::from_error(&original);
        let rebuilt = response.into_result().expect_err("should stay an error");

        assert!(
            matches!(rebuilt, Error::PermissionDenied(_)),
            "a caller has to still be able to tell why it failed; got {rebuilt:?}"
        );
        assert!(
            rebuilt.to_string().contains("/srv/app/secret"),
            "the detail must survive too: {rebuilt}"
        );
    }

    #[test]
    fn an_error_says_which_side_failed() {
        let response = Response::from_error(&Error::NotFound("/gone".to_string()));

        let rebuilt = response.into_result().expect_err("should stay an error");

        assert!(rebuilt.to_string().contains("agent:"), "{rebuilt}");
    }

    #[test]
    fn the_error_variant_keeps_the_position_protocol_1_gave_it() {
        // The one piece of cross-version compatibility that matters. A current
        // client meeting an agent from an older build has to be able to decode
        // the frame that explains the mismatch, and bincode encodes a variant
        // as its position, so `Error` moving turns a clear "this agent speaks
        // protocol 1" into an unintelligible decode failure, on the exact path
        // an operator most needs a straight answer.
        let encoded = bincode::serde::encode_to_vec(
            Response::Error {
                kind: WireErrorKind::Unsupported,
                message: "wrong version".to_string(),
            },
            codec(),
        )
        .expect("encode");

        assert_eq!(
            encoded[0], 3,
            "Error must stay where protocol 1 put it; add new variants at the end"
        );
    }

    #[test]
    fn a_non_error_response_passes_through() {
        assert_eq!(Response::Ok.into_result().expect("ok"), Response::Ok);
    }
}
