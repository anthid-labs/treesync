//! The half of the remote protocol that runs on the target host.
//!
//! Started by the client as `treesync agent --root <path>` over SSH, and
//! speaks [`super::protocol`] on stdin and stdout. It is the same
//! binary as the client, and there is no separate agent build, so the target
//! side of a sync is exactly the code that is tested locally.
//!
//! # Why an agent at all
//!
//! The alternative is forking `rsync` per pass, which rebuilds its file list
//! over the link every time and puts the sync's correctness in another tool's
//! hands. An agent indexes the target in the target's own process, answers the
//! one scope that was asked about, and applies the plan through the same
//! [`LocalSink`] the local path uses, so a remote sync and a local sync
//! differ in transport and nothing else.
//!
//! # stdout belongs to the protocol
//!
//! Nothing here may print. A stray `println!` lands in the middle of a frame
//! and desynchronises the stream; the client would report a decode failure
//! naming a byte offset, which says nothing about the actual cause. Logging
//! goes to stderr, which the client drains and re-logs. See
//! [`ssh`](super::ssh).

use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

use super::delta;
use super::protocol::{
    self, CHUNK_SIZE, Chunk, PROTOCOL_VERSION, Request, Response, Token, WireIndex, WireSignature,
    index_options,
};
use crate::error::{Error, Result};
use crate::reconcile::{Index, Scope};
use crate::sink::local::{is_permission_denied, relax_dir, restore_mode, temporary_name};
use crate::sink::{LocalSink, Sink};

/// Prefix for the temporary file an incoming transfer accumulates in.
///
/// Distinct from the one [`LocalSink`] uses for a local copy, so a leftover
/// names the path that produced it.
pub(crate) const TEMP_PREFIX: &str = ".treesync-incoming-";

/// Serves one session on the given streams, returning when the peer hangs up.
///
/// Takes the streams rather than reaching for stdin/stdout so the whole agent
/// can be driven over a pipe pair in tests, with no SSH and no second host
/// involved.
pub async fn serve<R, W>(root: PathBuf, input: R, output: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut input = BufReader::new(input);
    let mut output = BufWriter::new(output);
    let agent = Agent { root };

    // The handshake is mandatory and first. Serving requests to a peer that has
    // not agreed a version means decoding its frames with this version's layout
    // and acting on whatever that produces, against a filesystem.
    match protocol::expect_frame::<_, Request>(&mut input, "the opening handshake").await? {
        Request::Hello { version } if version == PROTOCOL_VERSION => {
            protocol::write_frame(
                &mut output,
                &Response::Hello {
                    version: PROTOCOL_VERSION,
                    build: env!("CARGO_PKG_VERSION").to_string(),
                },
            )
            .await?;
        }
        Request::Hello { version } => {
            let error = Error::Unsupported(format!(
                "client speaks protocol {version}, this agent speaks {PROTOCOL_VERSION}"
            ));

            // Reported rather than dropped, so the client can say what is
            // wrong instead of reporting a closed pipe.
            protocol::write_frame(&mut output, &Response::from_error(&error)).await?;

            return Err(error);
        }
        other => {
            let error = Error::Internal(format!("expected a handshake, got {other:?}"));
            protocol::write_frame(&mut output, &Response::from_error(&error)).await?;

            return Err(error);
        }
    }

    tracing::info!(root = %agent.root.display(), "agent serving");

    while let Some(request) = protocol::read_frame::<_, Request>(&mut input).await? {
        if matches!(request, Request::Goodbye) {
            protocol::write_frame(&mut output, &Response::Ok).await?;
            break;
        }

        // A failed request is answered and the session continues. One
        // unreadable file must not cost the rest of the batch its connection,
        // which is the same rule `apply` follows on the client side.
        let response = match agent.handle(request, &mut input).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "request failed");
                Response::from_error(&error)
            }
        };

        // Encoded first, and separately, because a reply can be well formed and
        // still too large to send. Letting that failure out of this loop would
        // end the session, which the client cannot tell from the link dropping:
        // it would reconnect, ask exactly the same question, and get exactly the
        // same silence, forever.
        protocol::send_frame(&mut output, &encode_reply(&response)?).await?;
    }

    tracing::info!("agent done");

    Ok(())
}

/// Encodes a reply, falling back to reporting why it could not be sent.
///
/// A reply that does not fit in a frame is still a reply the client is waiting
/// for. The only message that realistically reaches the limit is the index of a
/// very large tree, and the shape of that failure is what makes it worth
/// handling here rather than letting it out: `handle` has already returned
/// `Ok`, so nothing upstream knows anything is wrong, and the write is the last
/// thing between the agent and a clean exit.
///
/// From the client, an agent that exits is indistinguishable from a link that
/// dropped. It reconnects, reissues the same request, and gets the same result:
/// a daemon that has stopped mirroring while looking like it is retrying. An
/// error frame instead is classified as the agent answering, which is not
/// retried, so the operator sees the reason once and the session stays up.
///
/// The fallback frame is a short string, so it cannot fail the same way. If it
/// somehow does, that error is returned and the session ends, which at that
/// point is the honest outcome.
fn encode_reply(response: &Response) -> Result<Vec<u8>> {
    match protocol::encode_frame(response) {
        Ok(framed) => Ok(framed),
        Err(error) => {
            tracing::warn!(%error, "a reply was too large to send");

            protocol::encode_frame(&Response::from_error(&error))
        }
    }
}

struct Agent {
    root: PathBuf,
}

/// Where an incoming transfer for `destination` accumulates.
///
/// One definition, used by both the receiving path and the resume report, so
/// the two can never disagree about which file they are talking about.
///
/// The name goes through [`temporary_name`], which keeps it inside the
/// filesystem's limit on a single component. A destination whose own name is
/// long but legal would otherwise produce a temporary the kernel refuses, and
/// that file could never be received however often the transfer was retried.
fn temporary_for(destination: &Path) -> Option<PathBuf> {
    let parent = destination.parent()?;
    let name = destination.file_name()?.to_string_lossy().to_string();

    Some(parent.join(temporary_name(TEMP_PREFIX, &name)))
}

/// Opens the temporary an incoming transfer accumulates in.
///
/// Created fresh rather than opened with `create`, for the reason
/// [`crate::sink::local`] does the same: the name is derived from the
/// destination's and is therefore predictable, so anything already sitting at it
/// would be followed. A symlink placed there by another account on the target
/// host would take the transfer's writes with it and be published in place of
/// the file.
///
/// Unlinking does not follow a symlink, so the first step removes the link
/// itself, and `create_new` then refuses to reuse anything that reappears.
async fn create_temporary(temporary: &Path) -> Result<tokio::fs::File> {
    match tokio::fs::remove_file(temporary).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(Error::from(err)),
    }

    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)
        .await
        .map_err(Error::from)
}

/// Something a transfer's frames can be recognised as.
///
/// Both stream kinds end the same way, and [`discard_transfer`] only needs to
/// know which frame that was.
trait TransferFrame: serde::de::DeserializeOwned {
    /// Whether this frame ends the transfer, either way.
    fn ends_the_transfer(&self) -> bool;
}

impl TransferFrame for Chunk {
    fn ends_the_transfer(&self) -> bool {
        matches!(self, Chunk::Commit { .. } | Chunk::Abort { .. })
    }
}

impl TransferFrame for Token {
    fn ends_the_transfer(&self) -> bool {
        matches!(self, Token::Commit { .. } | Token::Abort { .. })
    }
}

/// Reads and drops the frames of a transfer that is not going to happen, then
/// reports why.
///
/// The client does not learn that a transfer was refused until it has finished
/// sending it: the protocol takes strict turns, and the reply comes after the
/// stream. So a request that fails before the receiving loop still has to take
/// that stream off the wire. Returning without doing so leaves file content in
/// front of the next request, which the agent then decodes as a request, and the
/// session fails on something unrelated with nothing to say why.
///
/// Every failure inside the loop is already handled this way, by recording the
/// error and draining to the end. This is the same rule for the failures that
/// happen before the loop is reached.
async fn discard_transfer<R, F>(input: &mut R, error: Error, expecting: &str) -> Result<Response>
where
    R: AsyncRead + Unpin,
    F: TransferFrame,
{
    loop {
        // A transport failure is the stream itself going away, and there is
        // nothing left to resynchronise to, so it replaces the original error.
        let frame: F = protocol::expect_frame(input, expecting).await?;

        if frame.ends_the_transfer() {
            break;
        }
    }

    Err(error)
}

/// Opens the temporary, widening the directory it lives in if that is what
/// stopped it.
///
/// A target directory mirrored from a read-only source directory cannot be
/// written into, so every transfer of a new file inside it fails, on every pass,
/// forever. The mode is put back by the caller through the returned original,
/// once the transfer has been published or abandoned.
async fn open_temporary(parent: &Path, temporary: &Path) -> (Result<tokio::fs::File>, Option<u32>) {
    let error = match create_temporary(temporary).await {
        Ok(handle) => return (Ok(handle), None),
        Err(error) => error,
    };

    if !is_permission_denied(&error) {
        return (Err(error), None);
    }

    let Some(original) = relax_dir(parent).await else {
        return (Err(error), None);
    };

    (create_temporary(temporary).await, Some(original))
}

/// Copies a range of the existing file into the replacement.
///
/// Streamed in [`CHUNK_SIZE`] pieces rather than read whole. One `Copy` token
/// can name a run of gigabytes, since an unchanged region coalesces into
/// exactly one, and that case is the entire point of the delta, so it must not be the
/// case that allocates a buffer to match.
///
/// The range is checked against the file first. A stream naming bytes past the
/// end is either corrupt or was built against a different file, and both are
/// worth reporting rather than reading whatever happens to be there.
async fn copy_range<W>(
    source: &mut tokio::fs::File,
    out: &mut W,
    hasher: &mut blake3::Hasher,
    offset: u64,
    len: u64,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    // Checked: these are untrusted, and two large `u64`s sum to a small one.
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::Internal(format!("a copy of {len} bytes from {offset} overflows")))?;

    let length = source.metadata().await.map_err(Error::from)?.len();

    if end > length {
        return Err(Error::Internal(format!(
            "the stream reuses bytes {offset}..{end} of a file that is {length} bytes"
        )));
    }

    source
        .seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(Error::from)?;

    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut remaining = len;

    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;

        source
            .read_exact(&mut buffer[..want])
            .await
            .map_err(Error::from)?;
        out.write_all(&buffer[..want]).await.map_err(Error::from)?;
        hasher.update(&buffer[..want]);

        remaining -= want as u64;
    }

    Ok(())
}

/// How long an untouched temporary is kept before it is treated as abandoned.
///
/// A day, because the thing being weighed is a resume that might legitimately
/// be waiting out a long outage against disk held by a transfer nobody is
/// coming back for. At these file sizes the disk matters, but not so much that
/// it is worth cutting short a link that has been down since last night.
const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Removes abandoned transfer temporaries from a directory.
///
/// An interrupted transfer leaves its temporary in place on purpose so it can
/// be resumed. One that is never resumed would otherwise sit there forever, and
/// a forgotten temporary for a file of this size is real disk.
///
/// Swept when a transfer starts rather than on a timer: that is the moment the
/// directory is known to be in use, it costs one readdir of a directory about
/// to be written to anyway, and there is no background task to own. The
/// trade-off is that a directory which stops receiving transfers keeps its
/// leftovers: visible to an operator, invisible to the reconciler, and
/// harmless apart from the space.
///
/// `keep` is the temporary this transfer is about to use, which is never swept
/// however old it looks: it is the one that is about to be resumed.
async fn sweep_temporaries(parent: &Path, keep: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(parent).await else {
        return;
    };

    let now = std::time::SystemTime::now();

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if path == keep {
            continue;
        }

        let is_temporary = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(TEMP_PREFIX));

        if !is_temporary {
            continue;
        }

        let stale = entry
            .metadata()
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > STALE_AFTER);

        if stale {
            tracing::info!(path = %path.display(), "removing an abandoned transfer");
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

/// Reopens a partial transfer to continue it, trimmed to the agreed point.
///
/// Truncated rather than merely appended to, because the client resumes from an
/// offset it chose: anything past it would be bytes the incoming stream is
/// about to send again.
async fn resume(
    temporary: &Path,
    from: u64,
    hasher: &mut blake3::Hasher,
) -> Result<tokio::fs::File> {
    use tokio::io::AsyncSeekExt;

    // `symlink_metadata`, and then a check, because everything below this point
    // follows the path: the length is read through it, it is opened through it,
    // and `set_len` truncates through it. A symlink here would hand all of that
    // to whatever it points at, and the caller would go on to rename the link
    // over the destination.
    let metadata = tokio::fs::symlink_metadata(temporary)
        .await
        .map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => {
                Error::NotFound(format!("nothing left to resume at {}", temporary.display()))
            }
            _ => Error::from(err),
        })?;

    if !metadata.is_file() {
        return Err(Error::InvalidPath(format!(
            "{} is not a regular file, so there is no partial transfer to resume",
            temporary.display()
        )));
    }

    let length = metadata.len();

    if length < from {
        return Err(Error::Internal(format!(
            "asked to resume {} at {from} but only {length} bytes are here",
            temporary.display()
        )));
    }

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(temporary)
        .await
        .map_err(Error::from)?;

    file.set_len(from).await.map_err(Error::from)?;
    reabsorb(temporary, from, hasher).await?;

    let mut file = file;
    file.seek(std::io::SeekFrom::Start(from))
        .await
        .map_err(Error::from)?;

    Ok(file)
}

/// Re-reads a partial transfer to pick its hash back up where it left off.
///
/// BLAKE3 cannot be resumed from a digest, only from the bytes, so continuing
/// a transfer means reading back what is already on disk. That is a local pass
/// over a local file, which is the cheap half of the trade resumption makes:
/// re-reading gigabytes here beats re-sending them over the link.
async fn reabsorb(path: &Path, bytes: u64, hasher: &mut blake3::Hasher) -> Result<()> {
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path).await.map_err(Error::from)?;
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut remaining = bytes;

    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;

        file.read_exact(&mut buffer[..want])
            .await
            .map_err(Error::from)?;
        hasher.update(&buffer[..want]);

        remaining -= want as u64;
    }

    Ok(())
}

impl Agent {
    /// Opens a sink on the target root, creating the root if it is not there.
    ///
    /// Lazy, and only on the mutating paths: a first sync into a target that
    /// does not exist yet is the normal case, but creating it merely to answer
    /// an index would make `--dry-run` write to the target host.
    fn sink(&self) -> Result<LocalSink> {
        if !self.root.exists() {
            tracing::info!(root = %self.root.display(), "creating target root");
            std::fs::create_dir_all(&self.root).map_err(Error::from)?;
        }

        LocalSink::new(self.root.clone())
    }

    /// Works out where an incoming transfer for `relative` will land.
    ///
    /// The destination, the directory holding it, and the temporary it
    /// accumulates in. One function for both receiving paths, so the whole-file
    /// and delta transfers cannot disagree about any of the three, and so the
    /// resume report can name the same temporary either of them would.
    ///
    /// Resolved through the sink, so a path arriving over the network gets the
    /// same containment check as one produced by a local walk: `../../etc/ssh`
    /// is what arrives here if anything upstream is wrong, and so is an ordinary
    /// looking path whose parent is a symlink out of the tree.
    fn transfer_paths(&self, relative: &Path) -> Result<(PathBuf, PathBuf, PathBuf)> {
        let sink = self.sink()?;
        let destination = sink.resolve(relative)?;

        let parent = destination
            .parent()
            .ok_or_else(|| Error::InvalidPath(format!("{} has no parent", destination.display())))?
            .to_path_buf();

        let temporary = temporary_for(&destination).ok_or_else(|| {
            Error::InvalidPath(format!("{} has no file name", destination.display()))
        })?;

        Ok((destination, parent, temporary))
    }

    async fn handle<R>(&self, request: Request, input: &mut R) -> Result<Response>
    where
        R: AsyncRead + Unpin,
    {
        match request {
            Request::Hello { .. } => Err(Error::Internal(
                "a second handshake on an open session".to_string(),
            )),

            // Handled by the caller, which needs to stop the loop.
            Request::Goodbye => Ok(Response::Ok),

            Request::Index {
                scope,
                exclude,
                verify,
            } => {
                let options = index_options(&exclude, verify)?;
                let scope = scope.into_scope();

                // An absent root is an empty target, not a failure: it is
                // exactly what the first sync to a fresh host sees.
                if !self.root.exists() {
                    return Ok(Response::Index(WireIndex::new(&Index::new())));
                }

                let index = LocalSink::new(self.root.clone())?
                    .index(&scope, &options)
                    .await?;

                tracing::debug!(
                    entries = index.len(),
                    scope = %describe(&scope),
                    "indexed"
                );

                Ok(Response::Index(WireIndex::new(&index)))
            }

            Request::CreateDir { path } => {
                self.sink()?.create_dir(&path.into_path()).await?;
                Ok(Response::Ok)
            }

            Request::WriteFile { path } => self.receive_file(path.into_path(), input).await,

            Request::Signature { path, block_size } => {
                self.signature(path.into_path(), block_size).await
            }

            Request::PatchFile { path, resume_from } => {
                self.receive_patch(path.into_path(), resume_from, input)
                    .await
            }

            Request::ResumeState { path } => self.resume_state(path.into_path()).await,

            Request::CreateSymlink { path, target } => {
                self.sink()?
                    .create_symlink(&path.into_path(), &target.into_path())
                    .await?;
                Ok(Response::Ok)
            }

            Request::Remove { path } => {
                self.sink()?.remove(&path.into_path()).await?;
                Ok(Response::Ok)
            }

            Request::Rename { from, to } => {
                self.sink()?
                    .rename(&from.into_path(), &to.into_path())
                    .await?;
                Ok(Response::Ok)
            }

            Request::SetMetadata {
                path,
                metadata,
                preserve,
            } => {
                self.sink()?
                    .set_metadata(
                        &path.into_path(),
                        &metadata.into_metadata(),
                        preserve.into_preserve(),
                    )
                    .await?;
                Ok(Response::Ok)
            }
        }
    }

    /// Describes what the target already holds at `path`, block by block.
    ///
    /// Nothing at that path is an empty signature rather than an error. The
    /// client is entitled to ask before it knows, and "I have none of this" is
    /// a true answer that costs it one round trip instead of a failed action.
    ///
    /// This reads the target file but never sends it: what crosses the link is
    /// around twenty bytes per block. That asymmetry is the whole reason the
    /// agent exists on this side of the connection.
    async fn signature(&self, relative: PathBuf, block_size: u32) -> Result<Response> {
        if block_size == 0 {
            return Err(Error::Internal(
                "a signature was asked for with a zero block size".to_string(),
            ));
        }

        let empty = || {
            Ok(Response::Signature(WireSignature::new(&delta::Signature {
                block_size,
                blocks: Vec::new(),
            })))
        };

        // Deliberately not through `sink()`, which creates the root: describing
        // a target must not bring it into being.
        if !self.root.exists() {
            return empty();
        }

        let path = LocalSink::new(self.root.clone())?.resolve(&relative)?;

        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return empty(),
            Err(err) => return Err(Error::from(err)),
        };

        let signature = delta::signature_of(&mut file, block_size).await?;

        tracing::debug!(
            path = %relative.display(),
            blocks = signature.blocks.len(),
            block_size,
            "described a target file"
        );

        Ok(Response::Signature(WireSignature::new(&signature)))
    }

    /// Reports what an interrupted transfer left at `path`.
    ///
    /// Nothing there is `bytes: 0`, which is a perfectly good answer: the
    /// client starts clean. The hash is over exactly the bytes reported, so the
    /// client can check them against the same prefix of its own source before
    /// deciding to build on them.
    async fn resume_state(&self, relative: PathBuf) -> Result<Response> {
        let nothing = || {
            Ok(Response::ResumeState {
                bytes: 0,
                hash: *blake3::Hasher::new().finalize().as_bytes(),
            })
        };

        if !self.root.exists() {
            return nothing();
        }

        let destination = LocalSink::new(self.root.clone())?.resolve(&relative)?;

        let Some(temporary) = temporary_for(&destination) else {
            return nothing();
        };

        // Not `metadata`: a symlink at the temporary path would report the
        // length and hash of whatever it points at, and the client would then
        // resume onto bytes that were never part of this transfer. Anything that
        // is not a plain file is reported as nothing to resume, which is always
        // a safe answer, since it costs a fresh transfer and nothing else.
        let bytes = match tokio::fs::symlink_metadata(&temporary).await {
            Ok(metadata) if metadata.is_file() => metadata.len(),
            Ok(_) => return nothing(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return nothing(),
            Err(err) => return Err(Error::from(err)),
        };

        if bytes == 0 {
            return nothing();
        }

        let mut hasher = blake3::Hasher::new();
        reabsorb(&temporary, bytes, &mut hasher).await?;

        tracing::debug!(
            path = %relative.display(),
            bytes,
            "reporting a partial transfer"
        );

        Ok(Response::ResumeState {
            bytes,
            hash: *hasher.finalize().as_bytes(),
        })
    }

    /// Rebuilds a file from what is already here plus what the client sent.
    ///
    /// The same publish discipline as [`Self::receive_file`]: reconstruct into a
    /// temporary beside the destination, then rename. The existing file is read
    /// throughout and replaced only at the end, so a patch that fails leaves the
    /// version that was already working exactly where it was.
    ///
    /// The commit hash is checked before the rename. A reconstruction that does
    /// not match what the client read is never published, which covers a stale
    /// block reused out of this side's own copy, a bug here, and corruption on
    /// the disk underneath.
    async fn receive_patch<R>(
        &self,
        relative: PathBuf,
        resume_from: u64,
        input: &mut R,
    ) -> Result<Response>
    where
        R: AsyncRead + Unpin,
    {
        use tokio::io::AsyncWriteExt;

        // Discarded rather than returned, for the reason the chunk loop below
        // runs to its end even after a failure: the client is already streaming
        // this file's tokens, and leaving them in the stream means the next
        // request decodes content as if it were a request.
        let (destination, parent, temporary) = match self.transfer_paths(&relative) {
            Ok(paths) => paths,
            Err(error) => return discard_transfer::<_, Token>(input, error, "a delta token").await,
        };

        let mut failure = match tokio::fs::create_dir_all(&parent).await {
            Ok(()) => None,
            Err(err) => Some(Error::from(err)),
        };

        sweep_temporaries(&parent, &temporary).await;

        // Held open for the whole patch. Its absence is not fatal, since a
        // stream that reuses nothing needs nothing from it, so this only becomes an
        // error if a `Copy` actually arrives.
        let mut existing = tokio::fs::File::open(&destination).await.ok();
        let mut hasher = blake3::Hasher::new();

        // Picking up where an interrupted attempt stopped, rather than starting
        // over. The client has already checked that these bytes are the right
        // ones, having hashed the same prefix of its own source and compared,
        // so what is left here is to trim anything past the agreed point and
        // read the kept part back into the running hash.
        // Set when the parent had to be widened to take this transfer, and put
        // back once the patch is published or abandoned.
        let mut relaxed: Option<u32> = None;

        let mut handle = if failure.is_some() {
            None
        } else if resume_from > 0 {
            // Widened up front here, unlike the branch below, which waits for a
            // failure and reacts to it.
            //
            // That difference is not a style choice. Creating a file in a
            // directory needs write permission on the directory, so a fresh
            // transfer that cannot open its temporary has said everything there
            // is to say and can be answered. *Reopening* one needs write
            // permission on the file, and nothing at all from the directory, so
            // a resume into a read-only directory opens perfectly well, runs the
            // whole patch, and only fails at the rename that publishes it. There
            // is no failure to react to until the work is already done.
            //
            // The interrupted attempt that left this temporary behind put the
            // directory's mode back on its way out, so this is the ordinary
            // case after a dropped link, not an unusual one. Left alone, a large
            // file in a read-only directory would lose its entire transfer to a
            // single blip.
            //
            // `relax_dir` changes nothing and reports `None` when the directory
            // is already writable, so the cost here is one stat, on the rare
            // path.
            relaxed = relax_dir(&parent).await;

            match resume(&temporary, resume_from, &mut hasher).await {
                Ok(handle) => {
                    tracing::info!(
                        path = %relative.display(),
                        resume_from,
                        "resuming an interrupted patch"
                    );

                    Some(handle)
                }
                Err(error) => {
                    failure = Some(error);
                    None
                }
            }
        } else {
            let (opened, widened) = open_temporary(&parent, &temporary).await;
            relaxed = widened;

            match opened {
                Ok(handle) => Some(handle),
                Err(error) => {
                    failure = Some(error);
                    None
                }
            }
        };

        let mut committed = None;

        loop {
            let token: Token = match protocol::expect_frame(input, "a delta token").await {
                Ok(token) => token,
                Err(error) => {
                    // The stream died mid-patch. The temporary is *kept* here,
                    // unlike every other failure path: it is the only record of
                    // how far this got, and re-sending a gigabyte because a link
                    // blipped at the end of it is the cost this avoids.
                    //
                    // Keeping it is safe because it is never published without
                    // matching the commit hash, and never resumed onto without
                    // the client checking the prefix against its own source
                    // first. A leftover nobody comes back for is swept up by
                    // [`sweep_temporaries`] on a later transfer into the same
                    // directory.
                    if let Some(mut file) = handle {
                        let _ = file.flush().await;
                    }

                    // Even on the way out. The directory was widened to take
                    // this transfer, and leaving it that way would mirror a
                    // permission the source never had.
                    if let Some(original) = relaxed {
                        restore_mode(&parent, original).await;
                    }

                    return Err(error);
                }
            };

            match token {
                Token::Copy { offset, len } => {
                    // Kept draining after a failure, exactly as the whole-file
                    // path does: the frames have to come off the stream either
                    // way or the next request reads content as a request.
                    if failure.is_some() {
                        continue;
                    }

                    match (existing.as_mut(), handle.as_mut()) {
                        (Some(source), Some(out)) => {
                            if let Err(error) =
                                copy_range(source, out, &mut hasher, offset, len).await
                            {
                                failure = Some(error);
                                handle = None;
                            }
                        }
                        _ => {
                            failure = Some(Error::Internal(format!(
                                "the stream reuses bytes {offset}..{} of {}, which is not here",
                                offset.saturating_add(len),
                                relative.display()
                            )));
                            handle = None;
                        }
                    }
                }
                Token::Literal(bytes) => {
                    if let Some(out) = handle.as_mut() {
                        if let Err(err) = out.write_all(&bytes).await {
                            failure = Some(Error::from(err));
                            handle = None;
                        } else {
                            hasher.update(&bytes);
                        }
                    }
                }
                Token::Commit { mtime, hash } => {
                    committed = Some((mtime, hash));
                    break;
                }
                Token::Abort { reason } => {
                    failure = Some(failure.unwrap_or_else(|| {
                        Error::Internal(format!("the sender aborted the patch: {reason}"))
                    }));
                    break;
                }
            }
        }

        let outcome = async {
            if let Some(error) = failure {
                return Err(error);
            }

            let (mtime, expected) =
                committed.ok_or_else(|| Error::Internal("a patch never committed".to_string()))?;

            let mut file = handle.ok_or_else(|| {
                Error::Internal("a patch committed with no open file".to_string())
            })?;

            let actual = *hasher.finalize().as_bytes();

            if actual != expected {
                return Err(Error::Internal(format!(
                    "the reconstructed {} does not match the source: expected {}, built {}. \
                     The existing file has been left alone.",
                    relative.display(),
                    blake3::Hash::from_bytes(expected).to_hex(),
                    blake3::Hash::from_bytes(actual).to_hex(),
                )));
            }

            // Flushed and closed before the timestamp, or the buffered writes
            // would land after it and move it again.
            file.flush().await.map_err(Error::from)?;
            drop(file);

            filetime::set_file_mtime(
                &temporary,
                filetime::FileTime::from_system_time(mtime.into_system_time()),
            )
            .map_err(Error::from)?;

            tokio::fs::rename(&temporary, &destination)
                .await
                .map_err(Error::from)
        }
        .await;

        if outcome.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }

        if let Some(original) = relaxed {
            restore_mode(&parent, original).await;
        }

        outcome.map(|()| Response::Ok)
    }

    /// Streams one file in and publishes it atomically.
    ///
    /// The content lands in a temporary beside its destination, so the rename
    /// that publishes it stays within one filesystem and a reader on the target
    /// sees either the old file or the new one. A transfer that fails or is
    /// aborted takes its temporary with it and leaves the existing file alone,
    /// which matters most for the case this protects against: a link that drops
    /// halfway through replacing a file that was perfectly good.
    ///
    /// The chunk loop must run to its end even when something has already gone
    /// wrong. Returning early would leave the rest of the file's frames in the
    /// stream, and the next request would read content as if it were a request.
    async fn receive_file<R>(&self, relative: PathBuf, input: &mut R) -> Result<Response>
    where
        R: AsyncRead + Unpin,
    {
        use tokio::io::AsyncWriteExt;

        // Discarded rather than returned, for the same reason the chunk loop
        // below runs to its end even after a failure: the client is already
        // streaming this file's frames, and leaving them in the stream means the
        // next request decodes content as if it were a request.
        let (destination, parent, temporary) = match self.transfer_paths(&relative) {
            Ok(paths) => paths,
            Err(error) => return discard_transfer::<_, Chunk>(input, error, "a file chunk").await,
        };

        let mut failure = match tokio::fs::create_dir_all(&parent).await {
            Ok(()) => None,
            Err(err) => Some(Error::from(err)),
        };

        // Set when the parent had to be widened to take this transfer, and put
        // back once the file is published or abandoned.
        let mut relaxed: Option<u32> = None;

        let mut handle = if failure.is_none() {
            let (opened, widened) = open_temporary(&parent, &temporary).await;
            relaxed = widened;

            match opened {
                Ok(handle) => Some(handle),
                Err(error) => {
                    failure = Some(error);
                    None
                }
            }
        } else {
            None
        };

        let mut committed = None;

        loop {
            let chunk: Chunk = match protocol::expect_frame(input, "a file chunk").await {
                Ok(chunk) => chunk,
                Err(error) => {
                    // The stream is unusable, so there is no reply to send and
                    // no way to resynchronise. Take the temporary down on the
                    // way out rather than leaving it for the next transfer to
                    // find.
                    drop(handle);
                    let _ = tokio::fs::remove_file(&temporary).await;

                    if let Some(original) = relaxed {
                        restore_mode(&parent, original).await;
                    }

                    return Err(error);
                }
            };

            match chunk {
                Chunk::Data(bytes) => {
                    // Kept reading even after a failure: the frames have to be
                    // drained either way to keep the stream aligned.
                    if let Some(file) = handle.as_mut()
                        && let Err(err) = file.write_all(&bytes).await
                    {
                        failure = Some(Error::from(err));
                        handle = None;
                    }
                }
                Chunk::Commit { mtime } => {
                    committed = Some(mtime);
                    break;
                }
                Chunk::Abort { reason } => {
                    failure = Some(failure.unwrap_or_else(|| {
                        Error::Internal(format!("the sender aborted the transfer: {reason}"))
                    }));
                    break;
                }
            }
        }

        let outcome = async {
            if let Some(error) = failure {
                return Err(error);
            }

            let mtime =
                committed.ok_or_else(|| Error::Internal("transfer never committed".to_string()))?;

            let mut file = handle.ok_or_else(|| {
                Error::Internal("transfer committed with no open file".to_string())
            })?;

            // Flushed and closed before the timestamp is set: setting an mtime
            // on a handle with buffered writes outstanding would be overwritten
            // by the write that follows it.
            file.flush().await.map_err(Error::from)?;
            drop(file);

            filetime::set_file_mtime(
                &temporary,
                filetime::FileTime::from_system_time(mtime.into_system_time()),
            )
            .map_err(Error::from)?;

            tokio::fs::rename(&temporary, &destination)
                .await
                .map_err(Error::from)
        }
        .await;

        if outcome.is_err() {
            // Otherwise every failed transfer leaves one of these behind.
            let _ = tokio::fs::remove_file(&temporary).await;
        }

        if let Some(original) = relaxed {
            restore_mode(&parent, original).await;
        }

        outcome.map(|()| Response::Ok)
    }
}

fn describe(scope: &Scope) -> String {
    match scope {
        Scope::Paths(paths) => format!("{} path(s)", paths.len()),
        Scope::Subtree(prefix) if prefix.as_os_str().is_empty() => "whole tree".to_string(),
        Scope::Subtree(prefix) => prefix.display().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::protocol::WireTime;

    #[test]
    fn a_named_path_scope_is_described_by_count() {
        let scope = Scope::Paths(vec![PathBuf::from("a"), PathBuf::from("b")]);

        assert_eq!(describe(&scope), "2 path(s)");
    }

    #[test]
    fn the_empty_subtree_is_described_as_the_whole_tree() {
        assert_eq!(describe(&Scope::Subtree(PathBuf::new())), "whole tree");
    }

    #[test]
    fn a_subtree_is_described_by_its_prefix() {
        assert_eq!(
            describe(&Scope::Subtree(PathBuf::from("src/deep"))),
            "src/deep"
        );
    }

    #[test]
    fn the_temporary_prefix_is_distinct_from_a_local_copys() {
        // Both can land in the same directory, so a leftover should name the
        // path that produced it.
        assert_ne!(TEMP_PREFIX, crate::sink::local::TEMP_PREFIX);
        assert!(TEMP_PREFIX.starts_with('.'), "{TEMP_PREFIX}");
    }

    /// Feeds a token stream to `receive_patch` and returns what it made of it.
    ///
    /// Drives the agent over an in-memory pipe rather than a process, so a
    /// stream can be built by hand, including the malformed ones a correct
    /// client would never send, which are the interesting cases here.
    async fn patch_with(
        root: &std::path::Path,
        relative: &str,
        resume_from: u64,
        tokens: Vec<Token>,
    ) -> Result<Response> {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (_client_read, mut client_write) = tokio::io::split(client);

        for token in &tokens {
            protocol::write_frame(&mut client_write, token)
                .await
                .expect("write a token");
        }

        // Dropped so a stream that never commits reaches a clean end rather
        // than hanging the test.
        drop(client_write);

        let agent = Agent {
            root: root.to_path_buf(),
        };
        let mut server = server;

        agent
            .receive_patch(PathBuf::from(relative), resume_from, &mut server)
            .await
    }

    #[tokio::test]
    async fn a_patch_whose_hash_does_not_match_is_not_published() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let existing = b"the original contents, which must survive this".to_vec();
        std::fs::write(dir.path().join("a.txt"), &existing).expect("seed the target");

        // A well formed stream, but committing to a hash of something else.
        let outcome = patch_with(
            dir.path(),
            "a.txt",
            0,
            vec![
                Token::Literal(b"replacement content".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: *blake3::hash(b"something else entirely").as_bytes(),
                },
            ],
        )
        .await;

        assert!(
            outcome.is_err(),
            "a reconstruction that does not match must be refused"
        );
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            existing,
            "the file that was already there must be left exactly as it was"
        );
    }

    #[tokio::test]
    async fn a_refused_patch_leaves_no_temporary_behind() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"original").expect("seed");

        let _ = patch_with(
            dir.path(),
            "a.txt",
            0,
            vec![
                Token::Literal(b"nope".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: [0u8; blake3::OUT_LEN],
                },
            ],
        )
        .await;

        let strays: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect();

        assert!(strays.is_empty(), "left {strays:?} behind");
    }

    #[tokio::test]
    async fn a_patch_reusing_blocks_past_the_end_is_refused() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"0123456789abcdef").expect("seed");

        // One block exists; the stream asks for a hundred.
        let outcome = patch_with(
            dir.path(),
            "a.txt",
            0,
            vec![
                Token::Copy {
                    offset: 0,
                    len: 1600,
                },
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: [0u8; blake3::OUT_LEN],
                },
            ],
        )
        .await;

        assert!(
            outcome.is_err(),
            "reading past the end of the file must be refused, not attempted"
        );
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            b"0123456789abcdef",
            "the existing file must be untouched"
        );
    }

    #[tokio::test]
    async fn a_patch_reusing_a_file_that_is_not_there_is_refused() {
        let dir = tempfile::TempDir::new().expect("temp dir");

        let outcome = patch_with(
            dir.path(),
            "missing.txt",
            0,
            vec![
                Token::Copy { offset: 0, len: 16 },
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: [0u8; blake3::OUT_LEN],
                },
            ],
        )
        .await;

        assert!(
            outcome.is_err(),
            "there is nothing to reuse, and inventing something would be worse"
        );
    }

    /// Seeds the temporary an interrupted transfer would have left behind.
    fn seed_partial(root: &std::path::Path, name: &str, bytes: &[u8]) -> PathBuf {
        let temporary = root.join(format!("{TEMP_PREFIX}{name}"));
        std::fs::write(&temporary, bytes).expect("seed the partial transfer");

        temporary
    }

    #[tokio::test]
    async fn a_partial_transfer_is_reported_with_its_hash() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let partial = b"the first half of something larger".to_vec();
        seed_partial(dir.path(), "a.txt", &partial);

        let agent = Agent {
            root: dir.path().to_path_buf(),
        };

        let response = agent
            .resume_state(PathBuf::from("a.txt"))
            .await
            .expect("resume state");

        match response {
            Response::ResumeState { bytes, hash } => {
                assert_eq!(bytes, partial.len() as u64);
                assert_eq!(
                    hash,
                    *blake3::hash(&partial).as_bytes(),
                    "the hash must cover exactly the bytes reported, or the \
                     client cannot check them against its own source"
                );
            }
            other => panic!("expected a resume state, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nothing_to_resume_is_reported_as_zero() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let agent = Agent {
            root: dir.path().to_path_buf(),
        };

        let response = agent
            .resume_state(PathBuf::from("never-started.txt"))
            .await
            .expect("resume state");

        assert!(
            matches!(response, Response::ResumeState { bytes: 0, .. }),
            "got {response:?}"
        );
    }

    #[tokio::test]
    async fn a_resumed_patch_continues_where_it_stopped() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"0123456789abcdef").expect("seed the target");

        // What an interrupted attempt had already reconstructed.
        seed_partial(dir.path(), "a.txt", b"0123456789abcdef");

        let rebuilt = b"0123456789abcdefAPPENDED".to_vec();

        // Only the remainder is sent this time.
        let outcome = patch_with(
            dir.path(),
            "a.txt",
            16,
            vec![
                Token::Literal(b"APPENDED".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: *blake3::hash(&rebuilt).as_bytes(),
                },
            ],
        )
        .await;

        assert!(outcome.is_ok(), "the resume should publish: {outcome:?}");
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            rebuilt,
            "the kept prefix and the resumed remainder must join up exactly"
        );
    }

    #[tokio::test]
    async fn a_resumed_patch_into_a_read_only_directory_still_publishes() {
        use std::os::unix::fs::PermissionsExt;

        // The state a dropped link leaves behind: a partial temporary, and a
        // directory back at the mode the source says it has, because the
        // interrupted attempt put it back on its way out.
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"0123456789abcdef").expect("seed the target");
        seed_partial(dir.path(), "a.txt", b"0123456789abcdef");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
            .expect("chmod");

        let rebuilt = b"0123456789abcdefAPPENDED".to_vec();

        let outcome = patch_with(
            dir.path(),
            "a.txt",
            16,
            vec![
                Token::Literal(b"APPENDED".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: *blake3::hash(&rebuilt).as_bytes(),
                },
            ],
        )
        .await;

        let mode = std::fs::metadata(dir.path())
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;

        // Before any assertion, or the TempDir cannot be removed.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod");

        assert!(
            outcome.is_ok(),
            "reopening a partial transfer needs write permission on the *file*, \
             not on the directory, so a resume into a read-only directory opens \
             perfectly well and then fails at the rename that publishes it. \
             One dropped link would cost a large file its whole transfer: {outcome:?}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            rebuilt
        );
        assert_eq!(
            mode, 0o555,
            "and the mode still has to be exactly as it was found"
        );
    }

    #[tokio::test]
    async fn a_resume_trims_anything_past_the_agreed_point() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"0123456789abcdef").expect("seed the target");

        // Longer than the resume point: a partial write that made it to disk
        // after the last byte the client knows about. It has to go, or it would
        // be duplicated by the bytes now being sent.
        seed_partial(dir.path(), "a.txt", b"0123456789abcdefLEFTOVER");

        let rebuilt = b"0123456789abcdefAPPENDED".to_vec();

        let outcome = patch_with(
            dir.path(),
            "a.txt",
            16,
            vec![
                Token::Literal(b"APPENDED".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: *blake3::hash(&rebuilt).as_bytes(),
                },
            ],
        )
        .await;

        assert!(outcome.is_ok(), "{outcome:?}");
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            rebuilt,
            "the stale tail must have been truncated, not kept"
        );
    }

    #[tokio::test]
    async fn a_resume_past_what_is_there_is_refused() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"original").expect("seed");
        seed_partial(dir.path(), "a.txt", b"short");

        let outcome = patch_with(
            dir.path(),
            "a.txt",
            9_000,
            vec![Token::Commit {
                mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                hash: [0u8; blake3::OUT_LEN],
            }],
        )
        .await;

        assert!(
            outcome.is_err(),
            "resuming past the end would build on bytes that are not there"
        );
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            b"original",
            "the existing file must be untouched"
        );
    }

    #[tokio::test]
    async fn an_abandoned_temporary_is_swept_but_a_fresh_one_is_not() {
        let dir = tempfile::TempDir::new().expect("temp dir");

        let abandoned = dir.path().join(format!("{TEMP_PREFIX}forgotten.json"));
        std::fs::write(&abandoned, b"gigabytes, notionally").expect("write");
        filetime::set_file_mtime(
            &abandoned,
            filetime::FileTime::from_system_time(
                std::time::SystemTime::now() - STALE_AFTER - std::time::Duration::from_secs(60),
            ),
        )
        .expect("age it");

        let recent = dir.path().join(format!("{TEMP_PREFIX}in-progress.json"));
        std::fs::write(&recent, b"still going").expect("write");

        let keep = dir.path().join(format!("{TEMP_PREFIX}resuming.json"));
        std::fs::write(&keep, b"about to be resumed").expect("write");
        filetime::set_file_mtime(
            &keep,
            filetime::FileTime::from_system_time(
                std::time::SystemTime::now() - STALE_AFTER - std::time::Duration::from_secs(60),
            ),
        )
        .expect("age it");

        let ordinary = dir.path().join("a-real-file.json");
        std::fs::write(&ordinary, b"tree content").expect("write");

        sweep_temporaries(dir.path(), &keep).await;

        assert!(!abandoned.exists(), "a stale temporary should be swept");
        assert!(recent.exists(), "a transfer in flight must not be swept");
        assert!(
            keep.exists(),
            "the temporary about to be resumed must survive however old it looks"
        );
        assert!(ordinary.exists(), "tree content must never be touched");
    }

    // -----------------------------------------------------------------------
    // A reply that cannot be sent
    // -----------------------------------------------------------------------

    /// An index too large to fit in one frame.
    ///
    /// Built from a few very large paths rather than the half a million ordinary
    /// entries it would otherwise take. The encoder counts bytes, not entries,
    /// so this reaches the same limit and builds in an instant.
    fn an_oversized_index() -> Response {
        let mut index = WireIndex::default();

        for _ in 0..70 {
            index.entries.push((
                crate::remote::protocol::WirePath(vec![b'a'; 1024 * 1024]),
                crate::remote::protocol::WireEntry::Dir {
                    meta: crate::remote::protocol::WireMetadata {
                        mode: 0o755,
                        uid: 0,
                        gid: 0,
                    },
                },
            ));
        }

        Response::Index(index)
    }

    #[tokio::test]
    async fn a_reply_too_large_to_send_becomes_an_error_the_client_can_read() {
        let framed = encode_reply(&an_oversized_index()).expect("a reply must always be encodable");

        let mut reader = std::io::Cursor::new(framed);
        let decoded: Response = protocol::read_frame(&mut reader)
            .await
            .expect("read")
            .expect("a frame");

        match decoded {
            Response::Error { message, .. } => assert!(
                message.contains("frame limit"),
                "the operator has to be told what happened: {message}"
            ),
            other => panic!(
                "an unsendable reply must come back as an error, not kill the \
                 session: an agent that exits looks exactly like a link that \
                 dropped, and the client reconnects and asks again forever. \
                 Got {other:?}"
            ),
        }
    }

    #[tokio::test]
    async fn a_reply_that_fits_is_passed_through_untouched() {
        let framed = encode_reply(&Response::Ok).expect("encode");
        let expected = protocol::encode_frame(&Response::Ok).expect("encode");

        assert_eq!(framed, expected);
    }

    // -----------------------------------------------------------------------
    // A frame the session did not expect
    // -----------------------------------------------------------------------

    /// Drives a whole session over a pipe from a handwritten frame sequence.
    ///
    /// Returns every reply the agent sent. Built by hand rather than through
    /// `SshSink` precisely so the sequence can be wrong in ways a correct client
    /// never is.
    async fn session(root: &std::path::Path, requests: Vec<Vec<u8>>) -> Vec<Response> {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (mut client_read, mut client_write) = tokio::io::split(client);

        for framed in &requests {
            protocol::send_frame(&mut client_write, framed)
                .await
                .expect("write");
        }
        drop(client_write);

        let (server_read, server_write) = tokio::io::split(server);
        let _ = serve(root.to_path_buf(), server_read, server_write).await;

        let mut replies = Vec::new();
        while let Ok(Some(response)) = protocol::read_frame::<_, Response>(&mut client_read).await {
            replies.push(response);
        }

        replies
    }

    #[tokio::test]
    async fn a_misplaced_frame_does_not_cost_the_session() {
        // A request arriving where a file chunk belongs. A correct client never
        // does this, but a desynchronised or hostile one does, and the framing
        // is what decides whether it costs one request or every request after
        // it: each message carries its own length, so the reader lands on a
        // boundary either way.
        let dir = tempfile::TempDir::new().expect("temp dir");

        let frames = vec![
            protocol::encode_frame(&Request::Hello {
                version: PROTOCOL_VERSION,
            })
            .expect("encode"),
            protocol::encode_frame(&Request::WriteFile {
                path: crate::remote::protocol::WirePath::new(Path::new("a.txt")),
            })
            .expect("encode"),
            // Where a `Chunk` belongs. `Remove` is variant five, which `Chunk`
            // does not have, so this cannot even be misread as some other chunk.
            protocol::encode_frame(&Request::Remove {
                path: crate::remote::protocol::WirePath::new(Path::new("a")),
            })
            .expect("encode"),
            protocol::encode_frame(&Request::CreateDir {
                path: crate::remote::protocol::WirePath::new(Path::new("after")),
            })
            .expect("encode"),
            protocol::encode_frame(&Request::Goodbye).expect("encode"),
        ];

        let replies = session(dir.path(), frames).await;

        assert!(
            matches!(replies.first(), Some(Response::Hello { .. })),
            "got {replies:?}"
        );
        assert!(
            replies
                .iter()
                .any(|reply| matches!(reply, Response::Error { .. })),
            "the misplaced frame has to be reported: {replies:?}"
        );
        assert!(
            dir.path().join("after").is_dir(),
            "and the request behind it has to still be served; a session that \
             died here would look to the client like a dropped link"
        );
        assert!(
            !dir.path().join("a.txt").exists(),
            "nothing may be published for a transfer that never sent content"
        );

        let strays: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect();

        assert!(strays.is_empty(), "left {strays:?} behind");
    }

    // -----------------------------------------------------------------------
    // The temporary a transfer accumulates in
    // -----------------------------------------------------------------------

    #[test]
    fn a_long_destination_name_still_yields_a_usable_temporary() {
        // The prefix here is longer than the local sink's, so the ceiling on the
        // name it can carry is lower. A temporary the kernel refuses would make
        // this file impossible to receive, on every attempt, forever.
        let destination = PathBuf::from("/srv/app").join(format!("{}.json", "a".repeat(240)));

        let temporary = temporary_for(&destination).expect("a temporary");
        let name = temporary
            .file_name()
            .expect("a name")
            .to_string_lossy()
            .to_string();

        assert!(name.len() <= 255, "got {} bytes", name.len());
        assert!(name.starts_with(TEMP_PREFIX), "{name}");
    }

    #[test]
    fn the_temporary_lands_beside_the_file_it_is_for() {
        // The rename that publishes it has to stay inside one filesystem, or it
        // is a copy with no atomicity.
        let temporary = temporary_for(Path::new("/srv/app/sub/a.txt")).expect("a temporary");

        assert_eq!(temporary.parent(), Some(Path::new("/srv/app/sub")));
    }

    #[tokio::test]
    async fn a_symlink_at_the_temporary_path_is_replaced_not_followed() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let elsewhere = tempfile::TempDir::new().expect("temp dir");
        let victim = elsewhere.path().join("victim.txt");
        std::fs::write(&victim, b"precious").expect("write");

        let temporary = dir.path().join(format!("{TEMP_PREFIX}a.txt"));
        std::os::unix::fs::symlink(&victim, &temporary).expect("symlink");

        let handle = create_temporary(&temporary).await.expect("should create");
        drop(handle);

        assert_eq!(
            std::fs::read(&victim).expect("read"),
            b"precious",
            "an account on the target host can predict this name, and writing \
             through what it finds there hands the transfer to whatever it points at"
        );
        assert!(
            !std::fs::symlink_metadata(&temporary)
                .expect("metadata")
                .file_type()
                .is_symlink(),
            "the link must have been replaced by a real file"
        );
    }

    #[tokio::test]
    async fn a_symlinked_temporary_is_reported_as_nothing_to_resume() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let elsewhere = tempfile::TempDir::new().expect("temp dir");
        let victim = elsewhere.path().join("victim.txt");
        std::fs::write(&victim, b"not part of any transfer").expect("write");

        std::os::unix::fs::symlink(&victim, dir.path().join(format!("{TEMP_PREFIX}a.txt")))
            .expect("symlink");

        let agent = Agent {
            root: dir.path().to_path_buf(),
        };

        let response = agent
            .resume_state(PathBuf::from("a.txt"))
            .await
            .expect("resume state");

        assert!(
            matches!(response, Response::ResumeState { bytes: 0, .. }),
            "reporting the length of whatever a link points at would have the \
             client resume onto bytes that were never part of this file; got {response:?}"
        );
    }

    #[tokio::test]
    async fn a_symlinked_temporary_is_refused_by_a_resume() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let elsewhere = tempfile::TempDir::new().expect("temp dir");
        let victim = elsewhere.path().join("victim.txt");
        std::fs::write(&victim, b"precious, and not a partial transfer").expect("write");

        let temporary = dir.path().join(format!("{TEMP_PREFIX}a.txt"));
        std::os::unix::fs::symlink(&victim, &temporary).expect("symlink");

        let mut hasher = blake3::Hasher::new();
        let result = resume(&temporary, 4, &mut hasher).await;

        assert!(
            result.is_err(),
            "a resume truncates and writes through the path it is given"
        );
        assert_eq!(
            std::fs::read(&victim).expect("read"),
            b"precious, and not a partial transfer",
            "and the file the link pointed at must be exactly as it was"
        );
    }

    #[tokio::test]
    async fn a_patch_that_matches_its_hash_is_published() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"0123456789abcdef").expect("seed");

        // Reuse the one existing block, then append.
        let rebuilt = b"0123456789abcdefAPPENDED".to_vec();

        let outcome = patch_with(
            dir.path(),
            "a.txt",
            0,
            vec![
                Token::Copy { offset: 0, len: 16 },
                Token::Literal(b"APPENDED".to_vec()),
                Token::Commit {
                    mtime: WireTime::new(std::time::SystemTime::UNIX_EPOCH),
                    hash: *blake3::hash(&rebuilt).as_bytes(),
                },
            ],
        )
        .await;

        assert!(
            outcome.is_ok(),
            "a matching patch must publish: {outcome:?}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).expect("read"),
            rebuilt,
            "the reused block and the literal must land in that order"
        );
    }
}
