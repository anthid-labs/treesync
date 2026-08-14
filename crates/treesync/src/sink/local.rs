use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;

use super::Sink;
use crate::error::{Error, Result};
use crate::reconcile::{Index, IndexOptions, Metadata, Preserve, Scope, index_scope};

/// Prefix for the temporary file an atomic write goes through.
///
/// Distinctive so a crash leaves something identifiable behind rather than a
/// file that looks like real content. `pub(crate)` only so the agent can assert
/// its own prefix differs from this one.
pub(crate) const TEMP_PREFIX: &str = ".treesync-tmp-";

/// The longest a single path component may be, in bytes.
///
/// Not a limit treesync gets to choose: the kernel refuses a longer name with
/// `ENAMETOOLONG`, and 255 is what every filesystem it targets allows.
const NAME_MAX: usize = 255;

/// Hex characters of BLAKE3 kept when a name has to be shortened.
///
/// Sixteen is far past what separating the files of one directory needs, and
/// short enough to leave most of the original name readable in the leftover a
/// crash puts on disk.
const DIGEST_LEN: usize = 16;

/// Owner write and execute: permission to create a file in a directory, and to
/// traverse into it.
const OWNER_MAY_WRITE: u32 = 0o300;

/// Names the temporary a transfer to `file_name` accumulates in.
///
/// A source name can be long enough that the prefix pushes the temporary past
/// [`NAME_MAX`] while the name itself is perfectly legal. Left alone, that file
/// can never be published however many times the transfer is retried: nothing
/// about it changes between attempts, so every pass fails the same way forever.
///
/// A name that would not fit is therefore shortened, and what was cut is
/// replaced by a hash of the whole original. Truncating alone would let two long
/// names in one directory collide on a single temporary, and two transfers
/// writing through the same file is worse than either of them failing.
pub(crate) fn temporary_name(prefix: &str, file_name: &str) -> String {
    let whole = format!("{prefix}{file_name}");

    if whole.len() <= NAME_MAX {
        return whole;
    }

    let digest = blake3::hash(file_name.as_bytes()).to_hex();
    let budget = NAME_MAX.saturating_sub(prefix.len() + DIGEST_LEN);

    format!(
        "{prefix}{}{}",
        truncate_bytes(file_name, budget),
        &digest[..DIGEST_LEN]
    )
}

/// The longest prefix of `value` that fits in `bytes` without splitting a
/// character.
fn truncate_bytes(value: &str, bytes: usize) -> &str {
    if value.len() <= bytes {
        return value;
    }

    let mut end = bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    &value[..end]
}

/// Whether an error is the filesystem refusing on permissions.
pub(crate) fn is_permission_denied(error: &Error) -> bool {
    match error {
        Error::PermissionDenied(_) => true,
        Error::Io(err) => err.kind() == std::io::ErrorKind::PermissionDenied,
        _ => false,
    }
}

/// Adds owner write and execute to a directory, returning the mode to put back.
///
/// A source tree may hold a directory nobody is meant to write into, and
/// mirroring its mode faithfully makes the target directory read-only too. Every
/// later pass that has to add a file inside it then fails, and keeps failing:
/// the mirror never converges, and no amount of retrying helps because nothing
/// about the target changes between attempts.
///
/// So the bits are widened for exactly as long as one operation takes, and
/// [`restore_mode`] puts the original back. `None` means the directory was
/// already writable or could not be changed, either of which says permissions on
/// it were not what stopped the caller.
pub(crate) async fn relax_dir(directory: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = tokio::fs::metadata(directory).await.ok()?;

    if !metadata.is_dir() {
        return None;
    }

    let original = metadata.permissions().mode() & 0o7777;

    if original & OWNER_MAY_WRITE == OWNER_MAY_WRITE {
        return None;
    }

    tokio::fs::set_permissions(
        directory,
        std::fs::Permissions::from_mode(original | OWNER_MAY_WRITE),
    )
    .await
    .ok()?;

    tracing::debug!(
        path = %directory.display(),
        mode = format!("{original:o}"),
        "widening a read-only directory for one operation"
    );

    Some(original)
}

/// Puts back the mode [`relax_dir`] widened.
///
/// A failure here leaves the target more permissive than the source says it
/// should be, which the next pass repairs on its own: the two modes now
/// disagree, so a metadata action is planned. Worth a line in the log all the
/// same, because until that pass runs the target is wrong.
pub(crate) async fn restore_mode(directory: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;

    if let Err(error) =
        tokio::fs::set_permissions(directory, std::fs::Permissions::from_mode(mode)).await
    {
        tracing::warn!(
            path = %directory.display(),
            %error,
            "could not restore a directory's mode after writing into it"
        );
    }
}

/// Decides whether a failed operation is worth retrying against a widened
/// directory, and widens it if so.
///
/// `Some(mode)` means try again, then hand `mode` back to [`restore_mode`].
/// `None` means the failure was not about permissions on this directory, so
/// retrying would fail the same way.
///
/// Written as a decision rather than as a wrapper taking the operation, because
/// the sink's methods are `async_trait` methods whose futures are boxed as
/// `dyn Future + Send`, and a closure returning a borrowing future cannot be
/// proved `Send` for every lifetime at that boundary. Two explicit calls at each
/// site read better than fighting that.
///
/// Costs nothing when the directory is writable, which is every ordinary case:
/// the first attempt succeeds and this is never reached.
async fn relax_for(directory: &Path, error: &Error) -> Option<u32> {
    if !is_permission_denied(error) {
        return None;
    }

    relax_dir(directory).await
}

/// Copies `source` through `temporary` and renames it over `destination`.
///
/// The rename is what makes the write atomic: a reader on the target sees the
/// old file or the new one, never a half-written one. Its own function so
/// [`Sink::write_file`] can run it twice, once normally and once against a
/// widened parent directory, without the body being written out twice.
async fn publish(source: &Path, temporary: &Path, destination: &Path) -> Result<()> {
    let result = async {
        copy_into_fresh(source, temporary).await?;

        // Before the rename, so the file is never visible with the wrong
        // timestamp. Without this the reconciler sees a differing mtime on
        // every pass and copies the same file forever.
        let source_mtime = tokio::fs::metadata(source).await?.modified()?;
        filetime::set_file_mtime(
            temporary,
            filetime::FileTime::from_system_time(source_mtime),
        )?;

        tokio::fs::rename(temporary, destination).await?;

        Ok::<(), std::io::Error>(())
    }
    .await;

    if result.is_err() {
        // Leaving these behind would accumulate on every failed transfer.
        let _ = tokio::fs::remove_file(temporary).await;
    }

    result.map_err(Error::from)
}

/// Copies `source` onto `temporary`, which is created fresh.
///
/// The temporary's name is derived from the destination's, so it is entirely
/// predictable, and anything already sitting at it would be *followed* by an
/// ordinary create: the copy would write through a symlink someone else placed
/// and the rename would publish the link rather than the content, destroying
/// whatever the link pointed at along the way.
///
/// Unlinking never follows a symlink, so the first step removes the link itself
/// (or a leftover from a crash, which is the same operation). `create_new` then
/// refuses to reuse anything that reappears in between, so the worst an
/// interfering process can do is make the transfer fail.
///
/// `std::io::copy` between two files reaches `copy_file_range`, so the copy
/// still happens inside the kernel rather than through a userspace buffer.
async fn copy_into_fresh(source: &Path, temporary: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_file(temporary).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    let source = source.to_path_buf();
    let temporary = temporary.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let mut input = std::fs::File::open(&source)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;

        std::io::copy(&mut input, &mut output)?;

        Ok(())
    })
    .await
    .map_err(|err| std::io::Error::other(format!("copy task failed: {err}")))?
}

/// Applies a plan to a directory on this machine.
#[derive(Debug, Clone)]
pub struct LocalSink {
    root: PathBuf,
}

impl LocalSink {
    /// Roots a sink at `root`, which must already exist.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();

        let metadata = std::fs::metadata(&root).map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => {
                Error::NotFound(format!("target root {}", root.display()))
            }
            std::io::ErrorKind::PermissionDenied => {
                Error::PermissionDenied(format!("target root {}", root.display()))
            }
            _ => Error::Io(err),
        })?;

        if !metadata.is_dir() {
            return Err(Error::InvalidPath(format!(
                "target root {} is not a directory",
                root.display()
            )));
        }

        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolves a relative path against the root, refusing anything that could
    /// land outside it.
    ///
    /// Two checks, and both are needed.
    ///
    /// The first is on the components: no absolute paths, no `..`, no `.`, no
    /// Windows prefixes. Checking the *components* rather than the joined string
    /// is what makes it hold, since `a/../../etc` normalizes away under a
    /// textual check but not under this one.
    ///
    /// The second is [`Self::refuse_symlinked_ancestors`], because the first
    /// proves only that a path *spells* something inside the root. It says
    /// nothing about where the path actually leads.
    ///
    /// Paths reaching a sink come from a tree walk locally and from the network
    /// on the agent, and this is the boundary that has to hold in the second
    /// case. `pub(crate)` so the agent can run an incoming transfer's path
    /// through exactly this check before opening anything.
    pub(crate) fn resolve(&self, relative: &Path) -> Result<PathBuf> {
        if relative.as_os_str().is_empty() {
            return Err(Error::InvalidPath("empty path".to_string()));
        }

        for component in relative.components() {
            if !matches!(component, Component::Normal(_)) {
                return Err(Error::InvalidPath(format!(
                    "{} is not confined to the target root",
                    relative.display()
                )));
            }
        }

        self.refuse_symlinked_ancestors(relative)?;

        Ok(self.root.join(relative))
    }

    /// Refuses a path that reaches its destination through a symlink.
    ///
    /// A symlink at any point along a path redirects everything below it. A
    /// target holding `a -> /etc` turns an ordinary write to `a/passwd` into a
    /// write outside the root, and the component check sees nothing wrong with
    /// either component, because there is nothing wrong with either component.
    /// The path is only dangerous once the filesystem is consulted.
    ///
    /// Only the directories leading to the path are examined, never the path
    /// itself. A symlink is a thing this sink legitimately creates, replaces and
    /// removes at the end of a path; what it must not do is *walk through* one.
    ///
    /// An ancestor that does not exist is fine. It is about to be created, and
    /// [`Sink::create_dir`] refuses to accept a symlink in place of one.
    ///
    /// This is a check, not a guarantee. Nothing stops a symlink appearing
    /// between here and the operation that follows, and closing that window
    /// means opening every component with `O_NOFOLLOW` and working from
    /// directory descriptors, which is a larger change than this one. What it
    /// does remove is the case that needs no race at all, where the link is
    /// simply already there.
    fn refuse_symlinked_ancestors(&self, relative: &Path) -> Result<()> {
        let mut ancestor = self.root.clone();
        let mut components = relative.components().peekable();

        while let Some(component) = components.next() {
            // The last component is the path itself, not an ancestor of it.
            if components.peek().is_none() {
                break;
            }

            ancestor.push(component);

            match std::fs::symlink_metadata(&ancestor) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Error::InvalidPath(format!(
                        "{} is reached through the symlink {}, which may leave the target root",
                        relative.display(),
                        ancestor.display()
                    )));
                }
                // Missing, or a real directory. Both are fine.
                _ => {}
            }
        }

        Ok(())
    }

    /// Clears whatever is at `path` and puts a link to `target` there.
    ///
    /// Its own method so [`Sink::create_symlink`] can run it twice, once
    /// normally and once against a widened parent directory, without the body
    /// being written out twice.
    async fn replace_symlink(&self, path: &Path, target: &Path) -> Result<()> {
        self.clear(path).await?;

        tokio::fs::symlink(target, path).await.map_err(Error::from)
    }

    /// Removes whatever is at `path`, if anything, without recursing.
    async fn clear(&self, path: &Path) -> Result<()> {
        match tokio::fs::symlink_metadata(path).await {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::from(err)),
            Ok(metadata) if metadata.is_dir() => {
                tokio::fs::remove_dir(path).await.map_err(Error::from)
            }
            Ok(_) => tokio::fs::remove_file(path).await.map_err(Error::from),
        }
    }
}

#[async_trait]
impl Sink for LocalSink {
    async fn index(&self, scope: &Scope, options: &IndexOptions) -> Result<Index> {
        let root = self.root.clone();
        let scope = scope.clone();
        let options = options.clone();

        // Walking a large tree is blocking work; keeping it off the runtime's
        // worker threads stops it stalling every other sync.
        tokio::task::spawn_blocking(move || index_scope(&root, &scope, &options))
            .await
            .map_err(|err| Error::Internal(format!("index task failed: {err}")))?
    }

    async fn create_dir(&self, relative: &Path) -> Result<()> {
        let path = self.resolve(relative)?;

        // `create_dir_all` is satisfied by a symlink that happens to point at a
        // directory. Accepting one would let it stand in for the directory the
        // source has, and everything written into that directory afterwards
        // would land wherever the link points.
        //
        // With `delete` on, the reconciler has already planned the removal of a
        // target entry whose kind changed, so this is only reachable when the
        // operator asked for no removals at all. Reported rather than worked
        // around: the remedy is theirs to choose.
        if let Ok(metadata) = tokio::fs::symlink_metadata(&path).await
            && metadata.file_type().is_symlink()
        {
            return Err(Error::InvalidPath(format!(
                "{} is a symlink on the target where the source has a directory; \
                 enable `delete` for this sync to let it be replaced",
                relative.display()
            )));
        }

        let parent = path.parent().unwrap_or(&self.root).to_path_buf();

        // `create_dir_all` succeeds when the directory already exists, which
        // keeps re-running a plan safe.
        let error = match tokio::fs::create_dir_all(&path).await {
            Ok(()) => return Ok(()),
            Err(err) => Error::from(err),
        };

        let Some(original) = relax_for(&parent, &error).await else {
            return Err(error);
        };

        let retried = tokio::fs::create_dir_all(&path).await.map_err(Error::from);
        restore_mode(&parent, original).await;

        retried
    }

    async fn write_file(&self, source: &Path, relative: &Path) -> Result<()> {
        let destination = self.resolve(relative)?;

        let parent = destination
            .parent()
            .ok_or_else(|| Error::InvalidPath(format!("{} has no parent", destination.display())))?
            .to_path_buf();
        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(Error::from)?;

        let file_name = destination.file_name().ok_or_else(|| {
            Error::InvalidPath(format!("{} has no file name", destination.display()))
        })?;

        // Same directory as the destination, so the rename below stays within
        // one filesystem and is therefore atomic. A temp file in /tmp would
        // make it a cross-device copy with no such guarantee.
        let temporary = parent.join(temporary_name(TEMP_PREFIX, &file_name.to_string_lossy()));

        let error = match publish(source, &temporary, &destination).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };

        let Some(original) = relax_for(&parent, &error).await else {
            return Err(error);
        };

        let retried = publish(source, &temporary, &destination).await;
        restore_mode(&parent, original).await;

        retried
    }

    async fn create_symlink(&self, relative: &Path, target: &Path) -> Result<()> {
        let path = self.resolve(relative)?;
        let parent = path.parent().unwrap_or(&self.root).to_path_buf();

        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(Error::from)?;

        // `symlink` fails if anything is already there, so replacing one means
        // clearing it first.
        let error = match self.replace_symlink(&path, target).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };

        let Some(original) = relax_for(&parent, &error).await else {
            return Err(error);
        };

        let retried = self.replace_symlink(&path, target).await;
        restore_mode(&parent, original).await;

        retried
    }

    async fn remove(&self, relative: &Path) -> Result<()> {
        let path = self.resolve(relative)?;
        let parent = path.parent().unwrap_or(&self.root).to_path_buf();

        // Deliberately not recursive. A plan removes children before their
        // parents, so a directory should be empty by the time it is reached;
        // if it is not, something is present that the plan did not account for
        // and `remove_dir_all` would destroy it without ever reporting what.
        // Failing loudly is the safer outcome for the copy without a backup.
        let error = match self.clear(&path).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };

        let Some(original) = relax_for(&parent, &error).await else {
            return Err(error);
        };

        let retried = self.clear(&path).await;
        restore_mode(&parent, original).await;

        retried
    }

    async fn set_metadata(
        &self,
        relative: &Path,
        metadata: &Metadata,
        preserve: Preserve,
    ) -> Result<()> {
        let path = self.resolve(relative)?;

        // Both calls below follow a symlink, so applying either to one would
        // change the mode or the owner of whatever it points at, anywhere on the
        // filesystem. The reconciler never asks for metadata on a link, since a
        // link carries none, so arriving here with one means the target is not
        // what the plan was built against.
        if let Ok(current) = tokio::fs::symlink_metadata(&path).await
            && current.file_type().is_symlink()
        {
            return Err(Error::InvalidPath(format!(
                "{} is a symlink on the target; setting metadata on it would \
                 change what it points at",
                relative.display()
            )));
        }

        if preserve.ownership {
            // `chown` is privileged. Reported per path rather than swallowed:
            // silently mirroring a tree with the wrong owners is worse than
            // saying so.
            std::os::unix::fs::chown(&path, Some(metadata.uid), Some(metadata.gid))
                .map_err(Error::from)?;
        }

        if preserve.mode {
            use std::os::unix::fs::PermissionsExt;

            tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(metadata.mode))
                .await
                .map_err(Error::from)?;
        }

        Ok(())
    }

    /// Moves a path within the sink.
    ///
    /// Alone among the mutating methods, this does not widen a read-only parent
    /// directory to get its work done, because nothing currently reaches it: no
    /// plan emits [`Action::Rename`](crate::reconcile::Action), so the rename
    /// optimisation the queue detects is not yet acted on.
    ///
    /// If that changes, this needs the same treatment as the others, and *both*
    /// ends need it: a rename takes write permission on the directory it leaves
    /// as well as the one it arrives in.
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.resolve(from)?;
        let to = self.resolve(to)?;

        if let Some(parent) = to.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(Error::from)?;
        }

        tokio::fs::rename(&from, &to).await.map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    /// A directory outside the sink's root, for the tests that check nothing
    /// reaches one.
    ///
    /// Its own `TempDir` rather than the root's parent, which is the shared
    /// system temporary directory: tests run in parallel, and two of them
    /// picking the same name there would collide.
    fn outside() -> TempDir {
        TempDir::new().expect("temp dir")
    }

    fn sink() -> (TempDir, LocalSink) {
        let dir = TempDir::new().expect("temp dir");
        let sink = LocalSink::new(dir.path()).expect("sink");

        (dir, sink)
    }

    #[test]
    fn a_missing_root_is_rejected() {
        let dir = TempDir::new().expect("temp dir");

        let err = LocalSink::new(dir.path().join("nope")).expect_err("should fail");

        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[test]
    fn a_file_as_root_is_rejected() {
        let dir = TempDir::new().expect("temp dir");
        let file = dir.path().join("regular.txt");
        std::fs::write(&file, b"x").expect("write");

        let err = LocalSink::new(&file).expect_err("should fail");

        assert!(matches!(err, Error::InvalidPath(_)), "got {err:?}");
    }

    #[test]
    fn plain_relative_paths_resolve_under_the_root() {
        let (dir, sink) = sink();

        assert_eq!(
            sink.resolve(Path::new("a/b/c.txt"))
                .expect("should resolve"),
            dir.path().join("a/b/c.txt")
        );
    }

    #[test]
    fn a_parent_traversal_is_refused() {
        let (_dir, sink) = sink();

        assert!(
            sink.resolve(Path::new("../escape")).is_err(),
            "a path leaving the root must never resolve"
        );
    }

    #[test]
    fn a_traversal_that_normalizes_away_is_still_refused() {
        let (_dir, sink) = sink();

        // Textually this ends up inside the root, so a string-based check would
        // pass it. Rejecting on components is what makes the boundary hold.
        assert!(sink.resolve(Path::new("a/../b")).is_err());
    }

    #[test]
    fn a_buried_traversal_is_refused() {
        let (_dir, sink) = sink();

        assert!(
            sink.resolve(Path::new("a/b/../../../../etc/passwd"))
                .is_err()
        );
    }

    #[test]
    fn an_absolute_path_is_refused() {
        let (_dir, sink) = sink();

        assert!(
            sink.resolve(Path::new("/etc/passwd")).is_err(),
            "an absolute path would ignore the root entirely"
        );
    }

    #[test]
    fn a_current_directory_component_is_refused() {
        let (_dir, sink) = sink();

        assert!(sink.resolve(Path::new("./a")).is_err());
    }

    #[test]
    fn an_empty_path_is_refused() {
        let (_dir, sink) = sink();

        assert!(sink.resolve(Path::new("")).is_err());
    }

    #[tokio::test]
    async fn creating_a_directory_twice_succeeds() {
        let (dir, sink) = sink();

        sink.create_dir(Path::new("a/b")).await.expect("first");
        sink.create_dir(Path::new("a/b")).await.expect("second");

        assert!(dir.path().join("a/b").is_dir());
    }

    #[tokio::test]
    async fn writing_a_file_creates_missing_parents() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"content").expect("write");

        sink.write_file(&source, Path::new("deep/nested/out.txt"))
            .await
            .expect("write");

        assert_eq!(
            std::fs::read(dir.path().join("deep/nested/out.txt")).expect("read"),
            b"content"
        );
    }

    #[tokio::test]
    async fn writing_a_file_preserves_the_source_mtime() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"content").expect("write");

        let stamp = filetime::FileTime::from_unix_time(1_000_000, 0);
        filetime::set_file_mtime(&source, stamp).expect("set mtime");

        sink.write_file(&source, Path::new("out.txt"))
            .await
            .expect("write");

        let written = std::fs::metadata(dir.path().join("out.txt")).expect("metadata");
        assert_eq!(
            filetime::FileTime::from_last_modification_time(&written),
            stamp,
            "a fresh mtime makes the reconciler re-copy this file on every pass, forever"
        );
    }

    #[tokio::test]
    async fn writing_leaves_no_temporary_behind() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"content").expect("write");

        sink.write_file(&source, Path::new("out.txt"))
            .await
            .expect("write");

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect();

        assert!(leftovers.is_empty(), "found {leftovers:?}");
    }

    #[tokio::test]
    async fn a_failed_write_cleans_up_its_temporary() {
        let (dir, sink) = sink();

        let result = sink
            .write_file(&dir.path().join("does-not-exist"), Path::new("out.txt"))
            .await;

        assert!(result.is_err());

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect();

        assert!(
            leftovers.is_empty(),
            "a failed transfer must not accumulate temporaries: {leftovers:?}"
        );
    }

    #[tokio::test]
    async fn writing_over_an_existing_file_replaces_it() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"new").expect("write");
        std::fs::write(dir.path().join("out.txt"), b"old contents, longer").expect("write");

        sink.write_file(&source, Path::new("out.txt"))
            .await
            .expect("write");

        assert_eq!(
            std::fs::read(dir.path().join("out.txt")).expect("read"),
            b"new"
        );
    }

    #[tokio::test]
    async fn a_symlink_is_created_and_can_be_repointed() {
        let (dir, sink) = sink();

        sink.create_symlink(Path::new("link"), Path::new("/etc/hosts"))
            .await
            .expect("create");
        assert_eq!(
            std::fs::read_link(dir.path().join("link")).expect("read link"),
            PathBuf::from("/etc/hosts")
        );

        sink.create_symlink(Path::new("link"), Path::new("/etc/services"))
            .await
            .expect("replace");
        assert_eq!(
            std::fs::read_link(dir.path().join("link")).expect("read link"),
            PathBuf::from("/etc/services")
        );
    }

    #[tokio::test]
    async fn removing_a_missing_path_succeeds() {
        let (_dir, sink) = sink();

        // Keeps re-running a plan safe when something already went.
        sink.remove(Path::new("never-existed"))
            .await
            .expect("remove");
    }

    #[tokio::test]
    async fn removing_a_non_empty_directory_fails_rather_than_recursing() {
        let (dir, sink) = sink();
        std::fs::create_dir(dir.path().join("full")).expect("mkdir");
        std::fs::write(dir.path().join("full/kept.txt"), b"data").expect("write");

        let result = sink.remove(Path::new("full")).await;

        assert!(
            result.is_err(),
            "recursive removal would destroy contents the plan never accounted for"
        );
        assert!(
            dir.path().join("full/kept.txt").exists(),
            "the unaccounted-for file must survive"
        );
    }

    #[tokio::test]
    async fn removing_a_symlink_does_not_follow_it() {
        let (dir, sink) = sink();
        let real = dir.path().join("real.txt");
        std::fs::write(&real, b"important").expect("write");
        std::os::unix::fs::symlink(&real, dir.path().join("link")).expect("symlink");

        sink.remove(Path::new("link")).await.expect("remove");

        assert!(!dir.path().join("link").exists());
        assert!(real.exists(), "removing a link must not remove its target");
    }

    #[tokio::test]
    async fn renaming_moves_within_the_root() {
        let (dir, sink) = sink();
        std::fs::write(dir.path().join("before.txt"), b"data").expect("write");

        sink.rename(Path::new("before.txt"), Path::new("sub/after.txt"))
            .await
            .expect("rename");

        assert!(!dir.path().join("before.txt").exists());
        assert_eq!(
            std::fs::read(dir.path().join("sub/after.txt")).expect("read"),
            b"data"
        );
    }

    // -----------------------------------------------------------------------
    // Containment through symlinks
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn a_path_reached_through_a_symlinked_ancestor_is_refused() {
        let (dir, sink) = sink();
        let elsewhere = outside();
        std::os::unix::fs::symlink(elsewhere.path(), dir.path().join("a")).expect("symlink");

        let result = sink.resolve(Path::new("a/escaped.txt"));

        assert!(
            matches!(result, Err(Error::InvalidPath(_))),
            "every component is a plain name, so only consulting the filesystem \
             can catch this; got {result:?}"
        );
    }

    #[test]
    fn an_ancestor_that_does_not_exist_yet_is_fine() {
        let (_dir, sink) = sink();

        // The ordinary case: a plan creates parents before it writes into them.
        assert!(sink.resolve(Path::new("not/there/yet.txt")).is_ok());
    }

    #[test]
    fn a_symlink_at_the_end_of_a_path_is_not_an_ancestor() {
        let (dir, sink) = sink();
        std::os::unix::fs::symlink("/etc/hosts", dir.path().join("link")).expect("symlink");

        // Replacing and removing a link are things this sink does. What it must
        // not do is walk *through* one.
        assert!(sink.resolve(Path::new("link")).is_ok());
    }

    #[tokio::test]
    async fn a_symlink_cannot_stand_in_for_a_directory() {
        let (dir, sink) = sink();
        let elsewhere = outside();
        std::os::unix::fs::symlink(elsewhere.path(), dir.path().join("a")).expect("symlink");

        let result = sink.create_dir(Path::new("a")).await;

        assert!(
            matches!(result, Err(Error::InvalidPath(_))),
            "create_dir_all is satisfied by a link to a directory, and accepting \
             one sends everything written into it elsewhere; got {result:?}"
        );
    }

    #[tokio::test]
    async fn metadata_is_not_applied_through_a_symlink() {
        let (dir, sink) = sink();
        let elsewhere = outside();
        let victim = elsewhere.path().join("victim.txt");
        std::fs::write(&victim, b"someone else's file").expect("write");
        std::os::unix::fs::symlink(&victim, dir.path().join("link")).expect("symlink");

        let result = sink
            .set_metadata(
                Path::new("link"),
                &Metadata {
                    mode: 0o777,
                    uid: 0,
                    gid: 0,
                },
                Preserve {
                    mode: true,
                    ownership: false,
                },
            )
            .await;

        assert!(matches!(result, Err(Error::InvalidPath(_))), "{result:?}");
        assert_ne!(
            std::fs::metadata(&victim)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o777,
            "chmod follows a link, so the file it points at must be untouched"
        );
    }

    #[tokio::test]
    async fn a_symlink_at_the_temporary_path_is_not_written_through() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"new content").expect("write");

        let elsewhere = outside();
        let victim = elsewhere.path().join("victim.txt");
        std::fs::write(&victim, b"precious").expect("write");

        // The temporary's name is derived from the destination's, so anyone who
        // can write to this directory can predict and pre-empt it.
        std::os::unix::fs::symlink(&victim, dir.path().join(".treesync-tmp-out.txt"))
            .expect("symlink");

        sink.write_file(&source, Path::new("out.txt"))
            .await
            .expect("the write should succeed on its own terms");

        assert_eq!(
            std::fs::read(&victim).expect("read"),
            b"precious",
            "the file the link pointed at must be untouched"
        );
        assert_eq!(
            std::fs::read(dir.path().join("out.txt")).expect("read"),
            b"new content"
        );
        assert!(
            !std::fs::symlink_metadata(dir.path().join("out.txt"))
                .expect("metadata")
                .file_type()
                .is_symlink(),
            "the published file must be the content, not the link"
        );
    }

    // -----------------------------------------------------------------------
    // Temporary names
    // -----------------------------------------------------------------------

    #[test]
    fn a_name_that_fits_is_used_as_it_is() {
        assert_eq!(
            temporary_name(TEMP_PREFIX, "a.txt"),
            ".treesync-tmp-a.txt",
            "the ordinary case has to stay readable on disk"
        );
    }

    #[test]
    fn a_name_too_long_for_the_filesystem_is_shortened_to_fit() {
        let long = format!("{}.txt", "a".repeat(245));

        let temporary = temporary_name(TEMP_PREFIX, &long);

        assert!(
            temporary.len() <= NAME_MAX,
            "a temporary the kernel refuses can never be published, however \
             often the transfer is retried; got {} bytes",
            temporary.len()
        );
        assert!(temporary.starts_with(TEMP_PREFIX), "{temporary}");
    }

    #[test]
    fn two_long_names_do_not_collide_on_one_temporary() {
        // Truncating alone would give these the same temporary, and two
        // transfers writing through one file is worse than either failing.
        let first = format!("{}-one.txt", "a".repeat(250));
        let second = format!("{}-two.txt", "a".repeat(250));

        assert_ne!(
            temporary_name(TEMP_PREFIX, &first),
            temporary_name(TEMP_PREFIX, &second)
        );
    }

    #[test]
    fn shortening_does_not_split_a_character() {
        // Every byte of the budget lands mid-character for some length, so this
        // walks the boundary rather than guessing one.
        for count in 60..100 {
            let name = "é".repeat(count);
            let temporary = temporary_name(TEMP_PREFIX, &name);

            assert!(temporary.len() <= NAME_MAX, "{count}: {}", temporary.len());
        }
    }

    #[test]
    fn a_shortened_temporary_is_still_recognisable_as_one() {
        // The walk hides temporaries from the reconciler by this prefix. A
        // shortened one that lost it would be indexed as tree content, and with
        // deletions on that means planning the removal of a live transfer.
        let long = "a".repeat(300);

        assert!(temporary_name(TEMP_PREFIX, &long).starts_with(TEMP_PREFIX));
    }

    // -----------------------------------------------------------------------
    // Writing into a read-only directory
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn a_writable_directory_is_left_alone() {
        let (dir, _sink) = sink();

        assert_eq!(
            relax_dir(dir.path()).await,
            None,
            "nothing should be changed when permissions were never the problem"
        );
    }

    #[tokio::test]
    async fn a_read_only_directory_is_widened_and_reports_its_old_mode() {
        let (dir, _sink) = sink();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("mkdir");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).expect("chmod");

        let original = relax_dir(&locked).await.expect("should widen");

        assert_eq!(original, 0o555);
        assert_eq!(
            std::fs::metadata(&locked)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o700,
            0o700,
            "the owner has to be able to create a file and enter the directory"
        );

        restore_mode(&locked, original).await;

        assert_eq!(
            std::fs::metadata(&locked)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o555,
            "and the mode has to go back exactly as it was"
        );
    }

    #[tokio::test]
    async fn a_file_lands_in_a_read_only_directory_and_the_mode_survives() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"content").expect("write");

        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("mkdir");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).expect("chmod");

        sink.write_file(&source, Path::new("locked/new.txt"))
            .await
            .expect("a mirror that cannot add a file to this directory never converges");

        assert_eq!(
            std::fs::read(locked.join("new.txt")).expect("read"),
            b"content"
        );
        assert_eq!(
            std::fs::metadata(&locked)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o555
        );

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }

    #[test]
    fn only_permission_failures_are_worth_widening_a_directory_for() {
        assert!(is_permission_denied(&Error::PermissionDenied(
            "denied".to_string()
        )));
        assert!(is_permission_denied(&Error::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        ))));
        assert!(
            !is_permission_denied(&Error::NotFound("gone".to_string())),
            "widening a directory cannot help a file that is not there"
        );
        assert!(!is_permission_denied(&Error::Io(std::io::Error::from(
            std::io::ErrorKind::StorageFull
        ))));
    }

    #[tokio::test]
    async fn an_escaping_write_is_refused_before_touching_the_filesystem() {
        let (dir, sink) = sink();
        let source = dir.path().join("source.txt");
        std::fs::write(&source, b"content").expect("write");

        let result = sink.write_file(&source, Path::new("../escaped.txt")).await;

        assert!(matches!(result, Err(Error::InvalidPath(_))));
        assert!(
            !dir.path().parent().unwrap().join("escaped.txt").exists(),
            "nothing may be written outside the root"
        );
    }
}
