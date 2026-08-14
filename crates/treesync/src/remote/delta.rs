//! Rolling-checksum delta, so a changed file costs its changes.
//!
//! # Why rolling, and not fixed blocks
//!
//! The obvious scheme, hashing block *n* on each side and sending the ones
//! that differ, fails on the workload this exists for. Editing a value in a large JSON
//! document usually changes its *length*, which shifts every byte after it. A
//! fixed-block comparison then reports every block from the edit onward as
//! different, and a one-line change costs the whole file.
//!
//! A rolling checksum is what makes the shifted case work. The window advances
//! a byte at a time, so a block the target already holds is found wherever it
//! now sits in the source, aligned or not.
//!
//! # Shape
//!
//! Two halves, on opposite ends of the link:
//!
//! - The **agent** builds a [`Signature`] of the file it already has: a weak
//!   rolling checksum and a truncated BLAKE3 per block. Only the signature
//!   crosses the link, around twenty bytes per block, against the block itself.
//! - The **client** slides a window over its source with [`Matcher`], and emits
//!   a [`Token`] stream of "reuse the block you already have" and "here are
//!   bytes you do not".
//!
//! # Two checksums, not one
//!
//! The weak checksum is what makes this affordable: it updates in constant time
//! per byte, so scanning a 1.5 GB source is one pass rather than one hash per
//! offset. It is also only sixteen bits per half and collides readily, so a
//! weak hit is a *candidate*, confirmed against the strong hash before anything
//! is reused. The weak checksum decides where to look; the strong one decides
//! whether it is really there.
//!
//! # Memory
//!
//! Everything here streams. The matcher holds a window plus a bounded
//! read-ahead, under a megabyte regardless of file size, because the point of
//! this module is files far larger than memory.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Error, Result};
use crate::remote::protocol::Token;

/// Bytes of BLAKE3 kept per block.
///
/// Sixteen rather than thirty-two: the signature is the one message whose size
/// scales with the file, and 128 bits is far past what distinguishing tens of
/// thousands of blocks needs.
pub const STRONG_LEN: usize = 16;

/// Bounds on the block size a signature will use.
const MIN_BLOCK: u64 = 16 * 1024;
const MAX_BLOCK: u64 = 128 * 1024;

/// How much source the matcher keeps buffered beyond its window.
const READ_AHEAD: usize = 256 * 1024;

/// How much unmatched data accumulates before it goes out as one literal.
const LITERAL_FLUSH: usize = 256 * 1024;

/// The block size to describe a file of `len` bytes with.
///
/// Square root of the length, which balances the two costs pulling against each
/// other: smaller blocks find more matches in a scattered edit, larger ones
/// make the signature cheaper to send. Clamped at both ends so a tiny file does
/// not get a pathologically small block and a huge one does not get a block so
/// coarse that a small edit dirties a large region.
pub fn block_size_for(len: u64) -> u32 {
    let root = (len as f64).sqrt() as u64;

    root.next_power_of_two().clamp(MIN_BLOCK, MAX_BLOCK) as u32
}

/// When a sink should send a delta rather than the whole file.
///
/// Lives here rather than in the config so the remote half does not have to
/// depend back on the config module that already depends on it. The TOML shape
/// converts into this, the same way a `[sync.target]` block converts into an
/// [`SshTarget`](crate::remote::SshTarget).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    pub enabled: bool,
    /// Files smaller than this are sent whole.
    pub min_size: u64,
    /// Fixed block size, or `None` to derive it with [`block_size_for`].
    pub block_size: Option<u32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: true,
            min_size: 1024 * 1024,
            block_size: None,
        }
    }
}

impl Options {
    /// The block size to use for a file of `len` bytes.
    pub fn block_size(&self, len: u64) -> u32 {
        self.block_size.unwrap_or_else(|| block_size_for(len))
    }
}

/// One block of a file, as the two checksums that identify it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSig {
    pub weak: u32,
    pub strong: [u8; STRONG_LEN],
}

/// What a target already holds, block by block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Signature {
    pub block_size: u32,
    pub blocks: Vec<BlockSig>,
}

/// rsync's weak checksum: cheap, and updatable in constant time per byte.
///
/// Two sixteen-bit halves, a plain sum of the window and a position-weighted
/// sum, packed into one word. Weak enough that a hit means "look closer", which
/// is exactly how [`Matcher`] uses it.
#[derive(Debug, Clone, Copy)]
pub struct Rolling {
    a: u32,
    b: u32,
    len: u32,
}

impl Rolling {
    /// Computes the checksum of a window from scratch.
    pub fn new(window: &[u8]) -> Self {
        let len = window.len() as u32;
        let mut a: u32 = 0;
        let mut b: u32 = 0;

        for (offset, &byte) in window.iter().enumerate() {
            a = a.wrapping_add(u32::from(byte));
            b = b.wrapping_add((len - offset as u32).wrapping_mul(u32::from(byte)));
        }

        Self {
            a: a & 0xffff,
            b: b & 0xffff,
            len,
        }
    }

    /// Advances the window one byte, dropping `out` and taking in `inbound`.
    ///
    /// The whole reason this scheme is affordable: constant time, so scanning
    /// every offset of a large file is one pass over it.
    pub fn roll(&mut self, out: u8, inbound: u8) {
        self.a = self
            .a
            .wrapping_sub(u32::from(out))
            .wrapping_add(u32::from(inbound))
            & 0xffff;

        self.b = self
            .b
            .wrapping_sub(self.len.wrapping_mul(u32::from(out)))
            .wrapping_add(self.a)
            & 0xffff;
    }

    pub fn digest(&self) -> u32 {
        self.a | (self.b << 16)
    }
}

/// The strong hash of a block.
fn strong(block: &[u8]) -> [u8; STRONG_LEN] {
    let mut out = [0u8; STRONG_LEN];
    out.copy_from_slice(&blake3::hash(block).as_bytes()[..STRONG_LEN]);

    out
}

/// Describes a file, block by block, without holding it in memory.
///
/// A trailing partial block is deliberately not described. The matcher's window
/// is exactly one block wide, so a short block could never be matched against
/// it, and indexing one would only add a case that can never fire. The cost is
/// at most `block_size - 1` bytes sent as literal at the end of a file.
pub async fn signature_of<R>(reader: &mut R, block_size: u32) -> Result<Signature>
where
    R: AsyncRead + Unpin,
{
    let mut blocks = Vec::new();
    let mut block = vec![0u8; block_size as usize];

    loop {
        let mut filled = 0;

        while filled < block.len() {
            match reader
                .read(&mut block[filled..])
                .await
                .map_err(Error::from)?
            {
                0 => break,
                read => filled += read,
            }
        }

        if filled < block.len() {
            break;
        }

        blocks.push(BlockSig {
            weak: Rolling::new(&block).digest(),
            strong: strong(&block),
        });
    }

    Ok(Signature { block_size, blocks })
}

/// Turns a source file into a [`Token`] stream against a target's signature.
///
/// Pull-based rather than callback-based so the caller can write each token to
/// the wire as it comes without lending this a mutable borrow of the
/// connection. It reads its source once, in order, and hashes every byte on the
/// way past, so the whole-file hash that [`Token::Commit`] carries costs
/// nothing extra.
pub struct Matcher<R> {
    reader: R,
    block_size: usize,
    /// Weak checksum to the blocks that share it. Collisions are expected; the
    /// strong hash resolves them.
    candidates: HashMap<u32, Vec<u32>>,
    strong: Vec<[u8; STRONG_LEN]>,
    /// Source read but not yet emitted.
    buffer: Vec<u8>,
    /// Where the window starts within `buffer`.
    pos: usize,
    /// Bytes found nowhere in the target, waiting to go out.
    literal: Vec<u8>,
    /// Carried across steps so a scan that pauses does not recompute it.
    rolling: Option<Rolling>,
    /// A run of adjacent matched blocks, as a byte range in the target's file.
    /// Coalesced so an unchanged region costs one token.
    run: Option<(u64, u64)>,
    ready: std::collections::VecDeque<Token>,
    scratch: Vec<u8>,
    hasher: blake3::Hasher,
    eof: bool,
    finished: bool,
}

impl<R> Matcher<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R, signature: &Signature) -> Self {
        let mut candidates: HashMap<u32, Vec<u32>> = HashMap::new();

        for (index, block) in signature.blocks.iter().enumerate() {
            candidates.entry(block.weak).or_default().push(index as u32);
        }

        Self {
            reader,
            block_size: signature.block_size.max(1) as usize,
            candidates,
            strong: signature.blocks.iter().map(|block| block.strong).collect(),
            buffer: Vec::new(),
            pos: 0,
            literal: Vec::new(),
            rolling: None,
            run: None,
            ready: std::collections::VecDeque::new(),
            scratch: vec![0u8; READ_AHEAD],
            hasher: blake3::Hasher::new(),
            eof: false,
            finished: false,
        }
    }

    /// The next token, or `None` once the source is exhausted.
    pub async fn next_token(&mut self) -> Result<Option<Token>> {
        loop {
            if let Some(token) = self.ready.pop_front() {
                return Ok(Some(token));
            }

            if self.finished {
                return Ok(None);
            }

            self.step().await?;
        }
    }

    /// BLAKE3 of the whole source, valid once the stream has ended.
    pub fn hash(&self) -> [u8; blake3::OUT_LEN] {
        *self.hasher.finalize().as_bytes()
    }

    /// Scans until at least one token is ready, or the source runs out.
    async fn step(&mut self) -> Result<()> {
        self.fill().await?;

        if self.buffer.len() - self.pos < self.block_size {
            // Nothing left is long enough to match a whole block, so whatever
            // remains can only be literal.
            let rest = self.buffer.len();
            if self.pos < rest {
                if self.literal.is_empty() {
                    self.flush_run();
                }

                let tail = self.buffer[self.pos..rest].to_vec();
                self.literal.extend_from_slice(&tail);
                self.pos = rest;
            }

            self.flush_run();
            self.flush_literal();
            self.finished = true;

            return Ok(());
        }

        let mut rolling = match self.rolling.take() {
            Some(rolling) => rolling,
            None => Rolling::new(&self.buffer[self.pos..self.pos + self.block_size]),
        };

        loop {
            if let Some(block) = self.match_at(&rolling) {
                // Literal before copy: the unmatched bytes came first.
                self.flush_literal();
                self.extend_run(block);
                self.pos += self.block_size;
                self.rolling = None;

                return Ok(());
            }

            let out = self.buffer[self.pos];

            // Copy before literal, for the same reason in reverse.
            if self.literal.is_empty() {
                self.flush_run();
            }

            self.literal.push(out);
            self.pos += 1;

            if self.buffer.len() - self.pos < self.block_size {
                // Out of window. Another step refills, or finishes.
                self.rolling = None;

                return Ok(());
            }

            let inbound = self.buffer[self.pos + self.block_size - 1];
            rolling.roll(out, inbound);

            if self.literal.len() >= LITERAL_FLUSH {
                self.flush_literal();
                self.rolling = Some(rolling);

                return Ok(());
            }
        }
    }

    /// Tops the buffer up, and drops what the window has already passed.
    async fn fill(&mut self) -> Result<()> {
        // Bounds memory by the window plus the read-ahead rather than by the
        // file, which is the whole point of streaming this.
        if self.pos > READ_AHEAD {
            self.buffer.drain(..self.pos);
            self.pos = 0;
        }

        let want = self.block_size + READ_AHEAD;

        while !self.eof && self.buffer.len() - self.pos < want {
            match self
                .reader
                .read(&mut self.scratch)
                .await
                .map_err(Error::from)?
            {
                0 => self.eof = true,
                read => {
                    // Hashed here, where every byte passes exactly once and in
                    // order, so the commit hash is free.
                    self.hasher.update(&self.scratch[..read]);
                    self.buffer.extend_from_slice(&self.scratch[..read]);
                }
            }
        }

        Ok(())
    }

    /// The block this window reproduces, if any.
    fn match_at(&self, rolling: &Rolling) -> Option<u32> {
        let candidates = self.candidates.get(&rolling.digest())?;
        let window = &self.buffer[self.pos..self.pos + self.block_size];
        let hash = strong(window);

        candidates
            .iter()
            .copied()
            .find(|&block| self.strong[block as usize] == hash)
    }

    /// Adds a matched block to the open run, or starts a new one.
    ///
    /// Only blocks that are adjacent *in the target* extend a run. A match
    /// that jumps elsewhere in the file has to become its own token, or the
    /// range would name bytes the source never had there.
    fn extend_run(&mut self, block: u32) {
        let offset = u64::from(block) * self.block_size as u64;
        let len = self.block_size as u64;

        match self.run {
            Some((start, run)) if start + run == offset => self.run = Some((start, run + len)),
            Some(_) => {
                self.flush_run();
                self.run = Some((offset, len));
            }
            None => self.run = Some((offset, len)),
        }
    }

    fn flush_run(&mut self) {
        if let Some((offset, len)) = self.run.take() {
            self.ready.push_back(Token::Copy { offset, len });
        }
    }

    fn flush_literal(&mut self) {
        if !self.literal.is_empty() {
            self.ready
                .push_back(Token::Literal(std::mem::take(&mut self.literal)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    async fn signature_of_bytes(bytes: &[u8], block_size: u32) -> Signature {
        signature_of(&mut Cursor::new(bytes.to_vec()), block_size)
            .await
            .expect("a signature")
    }

    async fn tokens(source: &[u8], signature: &Signature) -> (Vec<Token>, [u8; blake3::OUT_LEN]) {
        let mut matcher = Matcher::new(Cursor::new(source.to_vec()), signature);
        let mut tokens = Vec::new();

        while let Some(token) = matcher.next_token().await.expect("a token") {
            tokens.push(token);
        }

        (tokens, matcher.hash())
    }

    /// Rebuilds a file from a target's contents and a token stream, the way the
    /// agent does.
    fn reconstruct(target: &[u8], tokens: &[Token], _block_size: usize) -> Vec<u8> {
        let mut out = Vec::new();

        for token in tokens {
            match token {
                Token::Copy { offset, len } => {
                    let start = *offset as usize;
                    out.extend_from_slice(&target[start..start + *len as usize]);
                }
                Token::Literal(bytes) => out.extend_from_slice(bytes),
                Token::Commit { .. } | Token::Abort { .. } => {}
            }
        }

        out
    }

    fn json_like(entries: usize) -> Vec<u8> {
        let mut out = Vec::new();

        for index in 0..entries {
            out.extend_from_slice(
                format!(
                    r#"{{"id":{index},"name":"record-{index}","value":{}}},"#,
                    index * 7
                )
                .as_bytes(),
            );
        }

        out
    }

    #[test]
    fn rolling_matches_a_fresh_computation_at_every_offset() {
        let data = json_like(400);
        let block = 1024;

        let mut rolling = Rolling::new(&data[..block]);

        for offset in 1..=2000 {
            rolling.roll(data[offset - 1], data[offset + block - 1]);

            assert_eq!(
                rolling.digest(),
                Rolling::new(&data[offset..offset + block]).digest(),
                "rolled and fresh checksums disagree at offset {offset}"
            );
        }
    }

    #[tokio::test]
    async fn an_unchanged_file_sends_no_literals() {
        let data = json_like(4000);
        let signature = signature_of_bytes(&data, 1024).await;

        let (tokens, _) = tokens(&data, &signature).await;

        let literals: usize = tokens
            .iter()
            .map(|token| match token {
                Token::Literal(bytes) => bytes.len(),
                _ => 0,
            })
            .sum();

        // Only the trailing partial block, which is deliberately not indexed.
        assert!(
            literals < 1024,
            "an identical file should send almost nothing, sent {literals} bytes"
        );
    }

    #[tokio::test]
    async fn consecutive_blocks_coalesce_into_one_token() {
        let data = json_like(4000);
        let signature = signature_of_bytes(&data, 1024).await;

        let (tokens, _) = tokens(&data, &signature).await;

        let copies = tokens
            .iter()
            .filter(|token| matches!(token, Token::Copy { .. }))
            .count();

        assert_eq!(
            copies, 1,
            "an unchanged file is one run; got {copies} copy tokens"
        );
    }

    #[tokio::test]
    async fn a_byte_inserted_early_still_matches_every_later_block() {
        // The case fixed-block schemes fail, and the reason this is a *rolling*
        // checksum: everything after the insert is shifted by one byte.
        let target = json_like(4000);
        let mut source = target.clone();
        source.splice(50..50, *b"X");

        let signature = signature_of_bytes(&target, 1024).await;
        let (tokens, _) = tokens(&source, &signature).await;

        let literals: usize = tokens
            .iter()
            .map(|token| match token {
                Token::Literal(bytes) => bytes.len(),
                _ => 0,
            })
            .sum();

        assert!(
            literals < 4096,
            "a one byte insert should not dirty the file; sent {literals} of {} bytes",
            source.len()
        );
        assert_eq!(
            reconstruct(&target, &tokens, 1024),
            source,
            "reconstruction must be exact"
        );
    }

    #[tokio::test]
    async fn scattered_edits_reconstruct_exactly() {
        let target = json_like(6000);
        let mut source = target.clone();

        // Three edits of different shapes: replace, grow, shrink.
        source.splice(1_000..1_010, *b"REPLACED!!");
        source.splice(20_000..20_000, *b"a much longer inserted run of bytes");
        source.splice(50_000..50_400, *b"tiny");

        let signature = signature_of_bytes(&target, 1024).await;
        let (tokens, hash) = tokens(&source, &signature).await;

        assert_eq!(
            reconstruct(&target, &tokens, 1024),
            source,
            "reconstruction must be byte exact"
        );
        assert_eq!(
            hash,
            *blake3::hash(&source).as_bytes(),
            "the commit hash must cover the whole source"
        );
    }

    #[tokio::test]
    async fn a_target_with_nothing_in_common_is_all_literal() {
        let target = json_like(2000);
        let source: Vec<u8> = (0..40_000u32)
            .flat_map(|i| *blake3::hash(&i.to_le_bytes()).as_bytes())
            .collect();

        let signature = signature_of_bytes(&target, 1024).await;
        let (tokens, _) = tokens(&source, &signature).await;

        assert_eq!(
            reconstruct(&target, &tokens, 1024),
            source,
            "an unrelated file must still arrive intact"
        );
    }

    #[tokio::test]
    async fn an_empty_target_sends_the_whole_source() {
        let source = json_like(500);
        let signature = Signature {
            block_size: 1024,
            blocks: Vec::new(),
        };

        let (tokens, _) = tokens(&source, &signature).await;

        assert_eq!(reconstruct(&[], &tokens, 1024), source);
    }

    #[tokio::test]
    async fn a_source_shorter_than_one_block_is_literal() {
        let target = json_like(2000);
        let source = b"{}".to_vec();

        let signature = signature_of_bytes(&target, 1024).await;
        let (tokens, _) = tokens(&source, &signature).await;

        assert_eq!(reconstruct(&target, &tokens, 1024), source);
    }

    #[test]
    fn the_block_size_stays_within_its_bounds() {
        assert_eq!(block_size_for(0), MIN_BLOCK as u32);
        assert_eq!(block_size_for(1024), MIN_BLOCK as u32);

        // The cap needs a genuinely enormous file: the square root only
        // reaches 128 KiB somewhere past sixteen gigabytes.
        assert_eq!(block_size_for(u64::pow(1024, 4)), MAX_BLOCK as u32);

        // The size this was built for.
        assert_eq!(
            block_size_for(1_500_000_000),
            64 * 1024,
            "a 1.5 GB file should land on 64 KiB blocks, about 24k of them"
        );
    }
}
