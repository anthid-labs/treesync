use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::SystemTime;

use walkdir::WalkDir;

use super::{IndexOptions, Verify};
use crate::error::{Error, Result};

/// What a path is, plus the metadata used to decide whether it needs copying.
///
/// Symlinks carry their target rather than what they point at: a link is
/// replicated as a link, so following it would copy the wrong thing and, for a
/// link pointing outside the tree, would copy data the source never owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    File {
        size: u64,
        mtime: SystemTime,
        /// Content hash, present only under [`Verify::Checksum`].
        ///
        /// Optional because hashing means reading every byte of every candidate
        /// file, which is not a cost to impose on a sync that does not need it.
        hash: Option<blake3::Hash>,
        meta: Metadata,
    },
    Dir {
        meta: Metadata,
    },
    Symlink {
        target: PathBuf,
    },
}

/// Ownership and permission bits.
///
/// Captured unconditionally, since one `stat` already returns them, so recording
/// them is free. Whether a difference is *acted on* is
/// [`Preserve`](super::Preserve)'s decision.
///
/// Symlinks carry none: on Linux a link's own mode is unused, and changing its
/// ownership needs `lchown`, which is a separate problem from the ones this
/// solves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    /// Permission bits only, with the file-type bits masked off.
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl Metadata {
    /// Permission bits, discarding the file-type bits `st_mode` also carries.
    const PERMISSION_BITS: u32 = 0o7777;

    fn from_std(metadata: &std::fs::Metadata) -> Self {
        Self {
            mode: metadata.mode() & Self::PERMISSION_BITS,
            uid: metadata.uid(),
            gid: metadata.gid(),
        }
    }

    /// Whether the parts selected by `preserve` differ.
    pub fn differs_from(&self, other: &Metadata, preserve: super::Preserve) -> bool {
        (preserve.mode && self.mode != other.mode)
            || (preserve.ownership && (self.uid != other.uid || self.gid != other.gid))
    }
}

impl Entry {
    /// Whether replacing `self` with `other` requires transferring content.
    ///
    /// Size and mtime by default, the same quick check rsync makes without
    /// `--checksum`. That misses a rewrite preserving both, which happens on
    /// filesystems with coarse mtime granularity and whenever a tool restores
    /// content with the timestamp intact (`cp -p`, `tar -x`, `touch -r`).
    ///
    /// When both sides carry a hash, the hash decides. That is authoritative in
    /// both directions: it catches a same-size, same-mtime rewrite, and it also
    /// spares a transfer when only the timestamp moved.
    pub fn differs_from(&self, other: &Entry) -> bool {
        match (self, other) {
            (
                Entry::File {
                    size: a_size,
                    mtime: a_mtime,
                    hash: a_hash,
                    ..
                },
                Entry::File {
                    size: b_size,
                    mtime: b_mtime,
                    hash: b_hash,
                    ..
                },
            ) => match (a_hash, b_hash) {
                (Some(a), Some(b)) => a != b,
                _ => a_size != b_size || a_mtime != b_mtime,
            },
            (Entry::Dir { .. }, Entry::Dir { .. }) => false,
            (Entry::Symlink { target: a }, Entry::Symlink { target: b }) => a != b,
            // Different kinds entirely: the target has to be replaced.
            _ => true,
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self, Entry::Dir { .. })
    }

    /// Ownership and permissions, where the entry kind has them.
    pub fn metadata(&self) -> Option<&Metadata> {
        match self {
            Entry::File { meta, .. } | Entry::Dir { meta } => Some(meta),
            Entry::Symlink { .. } => None,
        }
    }
}

/// A snapshot of a tree, keyed by path relative to its root.
///
/// Relative because the whole point is to compare two trees rooted at different
/// places. Owned by one task and never shared, so it needs no locking.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Index {
    entries: HashMap<PathBuf, Entry>,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, path: impl Into<PathBuf>, entry: Entry) {
        self.entries.insert(path.into(), entry);
    }

    pub fn get(&self, path: &Path) -> Option<&Entry> {
        self.entries.get(path)
    }

    pub fn contains(&self, path: &Path) -> bool {
        self.entries.contains_key(path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn paths(&self) -> impl Iterator<Item = &PathBuf> {
        self.entries.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PathBuf, &Entry)> {
        self.entries.iter()
    }

    /// Paths at or beneath `prefix`. An empty prefix selects everything.
    pub fn under<'a>(&'a self, prefix: &'a Path) -> impl Iterator<Item = (&'a PathBuf, &'a Entry)> {
        self.entries
            .iter()
            .filter(move |(path, _)| prefix.as_os_str().is_empty() || path.starts_with(prefix))
    }
}

/// Walks `root` and records everything beneath it.
///
/// The root itself is not an entry; paths are relative to it.
///
/// Symlinks are recorded, never followed. Following them would let a link
/// pointing at `/` pull the entire filesystem into the sync, and a link cycle
/// would not terminate.
///
/// Entries that vanish mid-walk are skipped rather than failing the walk: a
/// tree being actively written always races the walk, and the next event or
/// rescan covers whatever moved.
///
/// A transfer's own temporary files are never reported. They are treesync's
/// working state, not tree content, and an interrupted transfer now leaves one
/// in place deliberately so it can be resumed. Reporting one would make it look
/// like a file the source does not have, which, with `delete` on, would plan
/// the removal of the very thing a resume is waiting to continue.
///
/// Neither is the running binary itself; see [`is_own_binary`].
pub fn walk(root: &Path, options: &IndexOptions) -> Result<Index> {
    ensure_root(root)?;

    let mut index = Index::new();

    let walker = WalkDir::new(root)
        .follow_links(false)
        .min_depth(1)
        .into_iter()
        // Prunes rather than filters: an excluded directory is never descended
        // into, so `node_modules/` costs nothing instead of being walked and
        // then discarded entry by entry.
        .filter_entry(|entry| {
            if is_transfer_temporary(entry.path()) || is_own_binary(entry.path()) {
                return false;
            }

            match entry.path().strip_prefix(root) {
                Ok(relative) => !options.filter.excludes(relative),
                Err(_) => true,
            }
        });

    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                // `depth() > 0` confines the skip to entries beneath the root.
                // A failure at the root itself is never a vanished child, and
                // must not be mistaken for an empty tree.
                if err.depth() > 0
                    && let Some(io_err) = err.io_error()
                    && io_err.kind() == std::io::ErrorKind::NotFound
                {
                    // Removed between being listed and being stat'd. Routine in
                    // a tree that is actively being written.
                    continue;
                }

                return Err(walk_error(root, err));
            }
        };

        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|_| {
                Error::InvalidPath(format!(
                    "{} escaped walk root {}",
                    entry.path().display(),
                    root.display()
                ))
            })?
            .to_path_buf();

        let file_type = entry.file_type();

        let recorded = if file_type.is_dir() {
            match entry.metadata() {
                Ok(metadata) => Entry::Dir {
                    meta: Metadata::from_std(&metadata),
                },
                Err(err)
                    if err.io_error().map(std::io::Error::kind)
                        == Some(std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(err) => return Err(walk_error(root, err)),
            }
        } else if file_type.is_symlink() {
            match std::fs::read_link(entry.path()) {
                Ok(target) => Entry::Symlink { target },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(Error::from(err)),
            }
        } else if !file_type.is_file() {
            skip_special(entry.path());
            continue;
        } else {
            match entry.metadata() {
                Ok(metadata) => Entry::File {
                    size: metadata.len(),
                    mtime: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    hash: hash_if_asked(entry.path(), options)?,
                    meta: Metadata::from_std(&metadata),
                },
                Err(err)
                    if err.io_error().map(std::io::Error::kind)
                        == Some(std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(err) => return Err(walk_error(root, err)),
            }
        };

        index.insert(relative, recorded);
    }

    Ok(index)
}

/// Notes a path that is not tree content, and is therefore not indexed.
///
/// A FIFO, a socket, or a device node. There is nothing to mirror in any of
/// them: what a FIFO holds is whatever a writer is putting through it right now,
/// and a device node is a number that means something only on the machine it is
/// on. Recreating one also needs `mknod`, which is privileged.
///
/// Opening one is worse than useless. A FIFO with no writer blocks the opening
/// thread indefinitely, and since a plan is applied one action at a time, that
/// is the whole sync stopped, with no error, no timeout, and nothing in the log
/// to say why. A socket fails with `ENXIO` instead, which at least reports, but
/// reports something an operator can do nothing about.
///
/// Skipped rather than failed, and skipped on both sides: both trees are indexed
/// through here, so a special file on the target is never mistaken for something
/// the source deleted.
fn skip_special(path: &Path) {
    tracing::debug!(
        path = %path.display(),
        "skipping a special file: not a regular file, directory or symlink"
    );
}

/// Checks that there is a tree here at all, before anything reads it as one.
///
/// Applied on every path into this module, and separately from the per-entry
/// handling. Skipping an entry that vanished is right for a tree being written
/// underneath us; applying that same rule to the *root* turns "the source is
/// gone" into "the source is empty", and with deletions enabled that is a plan
/// to remove the entire target.
///
/// [`walk`] has always made this check. [`stat_paths`] did not, which left the
/// gap open on exactly the path a running daemon uses: after the source is
/// unmounted, renamed or removed, the watcher's last batch of paths indexes as
/// empty and every one of them reads as a deletion to propagate. A full pass
/// refused to do that; the incremental pass beside it did it happily.
fn ensure_root(root: &Path) -> Result<()> {
    let metadata = std::fs::metadata(root).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => Error::NotFound(format!("walk root {}", root.display())),
        std::io::ErrorKind::PermissionDenied => {
            Error::PermissionDenied(format!("walk root {}", root.display()))
        }
        _ => Error::Io(err),
    })?;

    if !metadata.is_dir() {
        return Err(Error::InvalidPath(format!(
            "walk root {} is not a directory",
            root.display()
        )));
    }

    Ok(())
}

fn walk_error(root: &Path, err: walkdir::Error) -> Error {
    let path = err.path().unwrap_or(root).display().to_string();

    match err.io_error().map(std::io::Error::kind) {
        Some(std::io::ErrorKind::NotFound) => Error::NotFound(path),
        Some(std::io::ErrorKind::PermissionDenied) => Error::PermissionDenied(path),
        _ => Error::Internal(format!("walking {path}: {err}")),
    }
}

/// Builds the index a [`Scope`](super::Scope) needs, and no more.
///
/// The point of the incremental path: a batch naming three files stats three
/// files, rather than walking the tree to learn what rsync would relearn on
/// every invocation.
pub fn index_scope(root: &Path, scope: &super::Scope, options: &IndexOptions) -> Result<Index> {
    match scope {
        super::Scope::Paths(paths) => stat_paths(root, paths, options),
        super::Scope::Subtree(prefix) => walk_subtree(root, prefix, options),
    }
}

/// Stats named paths, omitting any that are not there.
///
/// Absence is an answer, not an error: a path in the batch that no longer
/// exists is exactly how a deletion is observed.
///
/// # A named directory is walked, not just stat'd
///
/// A directory in a batch may be hiding a subtree nothing ever reported. The
/// kernel only generates events for directories it already has a watch on, and
/// `mkdir -p a/b/c` creates all three faster than watches can be installed on
/// them. Measured on inotify, the whole of `a/b/c/deep.txt` arrives as the
/// single event `Create a`. Stat'ing `a` alone would mirror an empty directory
/// and silently lose everything beneath it.
///
/// So a directory here is walked in full. That is what this crate's own rule
/// requires. Reconciliation is the source of truth and notification is only an
/// optimization, so a missed event has to cost a re-walk rather than
/// correctness, and it is close to free in practice: a directory appears in a
/// batch only when it is *created*, never when files inside it change. Writing
/// into a directory that already exists reports the files and nothing else.
pub fn stat_paths(root: &Path, paths: &[PathBuf], options: &IndexOptions) -> Result<Index> {
    // Before any path is stat'd, because "not there" only means "deleted" if
    // there is still a tree for it to have been deleted from. See [`ensure_root`].
    ensure_root(root)?;

    let mut index = Index::new();

    for relative in paths {
        if options.filter.excludes(relative) {
            continue;
        }

        let full = root.join(relative);

        // The same rule the walk applies, applied on the path a watcher
        // reported. A scope rarely names the agent's own binary, since scopes
        // come from events on the source tree and the binary lives on the
        // target, but "never" has to hold on both routes into an index or it
        // is only a default.
        if is_own_binary(&full) {
            continue;
        }

        let metadata = match std::fs::symlink_metadata(&full) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                // Distinct from absent: treating an unreadable path as deleted
                // would propagate a removal for a file that is still there.
                return Err(Error::PermissionDenied(full.display().to_string()));
            }
            Err(err) => return Err(Error::from(err)),
        };

        let entry = if metadata.is_dir() {
            // Everything beneath it, plus the directory itself. See the note on
            // this function: what the watcher reported may be the only trace of
            // an entire subtree.
            for (path, entry) in walk_subtree(root, relative, options)?.iter() {
                index.insert(path.clone(), entry.clone());
            }

            continue;
        } else if metadata.is_symlink() {
            match std::fs::read_link(&full) {
                Ok(target) => Entry::Symlink { target },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(Error::from(err)),
            }
        } else if !metadata.is_file() {
            skip_special(&full);
            continue;
        } else {
            Entry::File {
                size: metadata.len(),
                mtime: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                hash: hash_if_asked(&full, options)?,
                meta: Metadata::from_std(&metadata),
            }
        };

        index.insert(relative.clone(), entry);
    }

    Ok(index)
}

/// Walks one subtree, recording paths relative to `root` rather than to the
/// subtree, so the result compares directly against a full-tree index.
///
/// A subtree that is gone yields an empty index. That is the normal way a
/// rescan discovers a deleted directory, not a failure.
pub fn walk_subtree(root: &Path, prefix: &Path, options: &IndexOptions) -> Result<Index> {
    if prefix.as_os_str().is_empty() {
        return walk(root, options);
    }

    // The root, not the subtree. A subtree that is gone is a deletion to
    // discover, which is what the empty index below reports. A *root* that is
    // gone is a tree that cannot be read, and reporting that as an empty
    // subtree would propagate the removal of everything under it.
    ensure_root(root)?;

    let subtree = root.join(prefix);

    match std::fs::symlink_metadata(&subtree) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Index::new()),
        Err(err) => return Err(Error::from(err)),
        Ok(metadata) if !metadata.is_dir() => {
            // Not a directory any more, so it has no contents to enumerate;
            // the path itself is still reported.
            return stat_paths(root, std::slice::from_ref(&prefix.to_path_buf()), options);
        }
        Ok(_) => {}
    }

    let mut index = walk(&subtree, options)?
        .iter()
        .map(|(path, entry)| (prefix.join(path), entry.clone()))
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Index::new(), |mut index, (path, entry)| {
            index.insert(path, entry);
            index
        });

    // The subtree root itself, so a comparison can see it needs removing.
    index.insert(
        prefix.to_path_buf(),
        Entry::Dir {
            meta: Metadata::from_std(&std::fs::metadata(&subtree).map_err(Error::from)?),
        },
    );

    Ok(index)
}

/// Hashes a file's contents, when the caller asked for content verification.
///
/// Whether a path is a half-finished transfer rather than tree content.
///
/// Both prefixes are checked here rather than each sink hiding its own, because
/// the walk is the one place that decides what the reconciler is allowed to
/// see, and a temporary that leaked into an index would be acted on as a real
/// file by code that has no way to know better.
fn is_transfer_temporary(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.starts_with(crate::sink::local::TEMP_PREFIX)
                || name.starts_with(crate::remote::agent::TEMP_PREFIX)
        })
}

/// Whether a path is this process's own executable, or a half-uploaded
/// replacement for it.
///
/// `agent_path` is allowed to point inside the tree it mirrors, and that is the
/// layout worth having: an agent under the target directory needs nothing
/// outside it, so a host is left exactly as it was found by removing one
/// directory, and nothing depends on the login account having a writable home.
///
/// What that layout costs, without this, is the agent. The source has no such
/// file, so the first reconcile sees a target path the source lacks, and with
/// `delete` on it plans the removal of the binary currently serving the
/// request. It would then be re-uploaded on the next connection and removed
/// again on the pass after that, which reads as a flapping agent rather than
/// as a configuration mistake.
///
/// Implied rather than required in `exclude`, deliberately. An operator who has
/// to write the rule can leave it out, and leaving it out is silent until the
/// pass that deletes something. treesync knows where its own binary is, so the
/// rule costs nothing to apply and there is no reading of the config that
/// should produce a sync trying to delete the process performing it.
///
/// The consequence to know about: a source tree that genuinely contains this
/// binary, at the same resolved path, is not mirrored. That means syncing the
/// directory treesync itself runs from, which is not a mirror anyone wants
/// silently overwriting a running executable either.
fn is_own_binary(path: &Path) -> bool {
    own_binary().is_some_and(|own| is_binary_at(path, own))
}

/// [`is_own_binary`] against a stated binary, so it can be tested without the
/// test harness's own executable having to be inside the tree being walked.
fn is_binary_at(path: &Path, own: &Path) -> bool {
    let (Some(name), Some(own_name)) = (file_name(path), file_name(own)) else {
        return false;
    };

    // The cheap half first. Resolving every entry in a large tree to answer a
    // question about one file would add a syscall per path walked, and a name
    // that cannot match makes the rest unnecessary.
    if name != own_name && name.strip_prefix(own_name) != Some(crate::remote::ship::UPLOAD_SUFFIX) {
        return false;
    }

    // Both sides, not just the candidate. A root reached through a symlinked
    // parent would otherwise compare unequal to the same file, and while
    // `current_exe` already reports a resolved path, a rule whose failure mode
    // is deleting the running agent should not rest on its caller having done
    // that. One extra syscall, on a path the name check has already narrowed
    // to the file this is about.
    let path = resolve(path);
    let own = resolve(own);

    if path == own {
        return true;
    }

    let mut incoming = own.into_os_string();
    incoming.push(crate::remote::ship::UPLOAD_SUFFIX);

    path.as_os_str() == incoming
}

/// A path with symlinks resolved, or unchanged when it cannot be.
///
/// Unresolvable means gone between being listed and being asked about, which
/// is routine in a tree being written. Comparing the path as given is then the
/// honest answer rather than a reason to fail the walk.
fn resolve(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// This process's executable, resolved once.
///
/// `current_exe` is a syscall, the walk asks per entry, and the answer cannot
/// change for the life of the process. `None` when the platform cannot say,
/// which disables the rule rather than guessing at a path to protect.
fn own_binary() -> Option<&'static Path> {
    static OWN_BINARY: OnceLock<Option<PathBuf>> = OnceLock::new();

    OWN_BINARY
        .get_or_init(|| std::env::current_exe().ok())
        .as_deref()
}

fn file_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(|name| name.to_str())
}

/// Reads the whole file, so this is the cost [`Verify::Checksum`] buys: the
/// ability to notice a change that left size and timestamp untouched.
fn hash_if_asked(path: &Path, options: &IndexOptions) -> Result<Option<blake3::Hash>> {
    if options.verify != Verify::Checksum {
        return Ok(None);
    }

    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        // Vanished between being listed and being read; the caller treats a
        // missing entry as absent rather than failing the whole index.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(Error::from(err)),
    };

    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&mut file).map_err(Error::from)?;

    Ok(Some(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn options() -> IndexOptions {
        IndexOptions::quick()
    }

    fn named(paths: &[&str]) -> Vec<PathBuf> {
        paths.iter().map(PathBuf::from).collect()
    }

    /// Creates a FIFO, or says why it could not.
    ///
    /// Shelled out rather than reached through `libc`, which this crate does not
    /// depend on and would not otherwise need.
    fn make_fifo(path: &Path) -> bool {
        std::process::Command::new("mkfifo")
            .arg(path)
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    // -----------------------------------------------------------------------
    // The root has to be there
    // -----------------------------------------------------------------------

    #[test]
    fn stat_paths_refuses_a_root_that_is_gone() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("source");
        std::fs::create_dir(&root).expect("mkdir");
        std::fs::write(root.join("a.txt"), b"one").expect("write");

        std::fs::remove_dir_all(&root).expect("the source volume goes away");

        let result = stat_paths(&root, &named(&["a.txt"]), &options());

        assert!(
            matches!(result, Err(Error::NotFound(_))),
            "an empty index here reads as every named path having been deleted, \
             and with deletions on that is a plan to empty the target; got {result:?}"
        );
    }

    #[test]
    fn walk_subtree_refuses_a_root_that_is_gone() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("source");

        let result = walk_subtree(&root, Path::new("sub"), &options());

        assert!(matches!(result, Err(Error::NotFound(_))), "{result:?}");
    }

    #[test]
    fn a_subtree_that_is_gone_is_still_an_empty_index() {
        // The distinction the guard has to keep: a missing *subtree* is how a
        // rescan discovers a deleted directory, and must stay an empty index.
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("kept.txt"), b"one").expect("write");

        let index = walk_subtree(dir.path(), Path::new("gone"), &options()).expect("should be ok");

        assert!(index.is_empty());
    }

    #[test]
    fn a_path_that_is_gone_under_a_root_that_is_there_is_still_absent() {
        // And the other half: within a tree that exists, a path that does not is
        // exactly how a deletion is observed.
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("kept.txt"), b"one").expect("write");

        let index = stat_paths(dir.path(), &named(&["kept.txt", "gone.txt"]), &options())
            .expect("should be ok");

        assert!(index.contains(Path::new("kept.txt")));
        assert!(!index.contains(Path::new("gone.txt")));
    }

    #[test]
    fn stat_paths_refuses_a_root_that_is_a_file() {
        let dir = TempDir::new().expect("temp dir");
        let file = dir.path().join("regular.txt");
        std::fs::write(&file, b"x").expect("write");

        let result = stat_paths(&file, &named(&["a.txt"]), &options());

        assert!(matches!(result, Err(Error::InvalidPath(_))), "{result:?}");
    }

    // -----------------------------------------------------------------------
    // Special files are not tree content
    // -----------------------------------------------------------------------

    #[test]
    fn a_fifo_is_not_walked_into_the_index() {
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("ordinary.txt"), b"content").expect("write");

        if !make_fifo(&dir.path().join("pipe")) {
            eprintln!("SKIPPED a_fifo_is_not_walked_into_the_index: no usable mkfifo");
            return;
        }

        let index = walk(dir.path(), &options()).expect("walk");

        assert!(
            !index.contains(Path::new("pipe")),
            "indexing a FIFO means opening it, and an open with no writer on the \
             other end never returns"
        );
        assert!(
            index.contains(Path::new("ordinary.txt")),
            "and the rest of the tree still has to be indexed"
        );
    }

    #[test]
    fn a_fifo_is_not_stat_into_the_index_either() {
        // The incremental path reaches the same tree by a different route, so it
        // needs the same rule or a watcher event is enough to reintroduce one.
        let dir = TempDir::new().expect("temp dir");

        if !make_fifo(&dir.path().join("pipe")) {
            eprintln!("SKIPPED a_fifo_is_not_stat_into_the_index_either: no usable mkfifo");
            return;
        }

        let index = stat_paths(dir.path(), &named(&["pipe"]), &options()).expect("stat");

        assert!(
            index.is_empty(),
            "got {:?}",
            index.paths().collect::<Vec<_>>()
        );
    }

    #[test]
    fn an_ordinary_file_is_still_indexed_beside_a_special_one() {
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.txt"), b"one").expect("write");
        std::os::unix::fs::symlink("/etc/hosts", dir.path().join("link")).expect("symlink");
        std::fs::create_dir(dir.path().join("sub")).expect("mkdir");

        if !make_fifo(&dir.path().join("pipe")) {
            eprintln!("SKIPPED an_ordinary_file_is_still_indexed_beside_a_special_one: no mkfifo");
            return;
        }

        let index = walk(dir.path(), &options()).expect("walk");

        assert_eq!(
            index.len(),
            3,
            "a file, a link and a directory; got {:?}",
            index.paths().collect::<Vec<_>>()
        );
    }

    /// A stand-in for the running binary, so the resolution rule can be
    /// exercised against a path a test controls.
    fn planted_agent(dir: &TempDir) -> PathBuf {
        let own = dir.path().join(".bin").join("treesync");
        std::fs::create_dir_all(own.parent().expect("parent")).expect("mkdir");
        std::fs::write(&own, b"binary").expect("write");

        own
    }

    #[test]
    fn the_agent_binary_is_not_reported_as_tree_content() {
        let dir = TempDir::new().expect("temp dir");
        let own = planted_agent(&dir);

        assert!(is_binary_at(&own, &own));
    }

    #[test]
    fn a_half_uploaded_agent_is_not_reported_either() {
        // The window an upload is open for. With `delete` on, an index that
        // reported this would plan the removal of a transfer in flight.
        let dir = TempDir::new().expect("temp dir");
        let own = planted_agent(&dir);
        let incoming = own.with_file_name("treesync.incoming");
        std::fs::write(&incoming, b"partial").expect("write");

        assert!(is_binary_at(&incoming, &own));
    }

    #[test]
    fn a_file_that_merely_shares_the_name_is_still_indexed() {
        // The rule is about one file, not about a name. A tree that happens to
        // contain something called `treesync` is ordinary content, and silently
        // refusing to mirror it would be a worse surprise than the one this
        // avoids.
        let dir = TempDir::new().expect("temp dir");
        let own = planted_agent(&dir);

        let elsewhere = dir.path().join("treesync");
        std::fs::write(&elsewhere, b"not the agent").expect("write");

        assert!(!is_binary_at(&elsewhere, &own));

        let neighbour = own.with_file_name("treesync.old");
        std::fs::write(&neighbour, b"previous").expect("write");

        assert!(!is_binary_at(&neighbour, &own));
    }

    #[test]
    fn the_running_binary_is_skipped_by_the_walk() {
        // Through the real `current_exe`, and through a symlink, which is what
        // proves the canonicalising step: `current_exe` reports a resolved
        // path, so an unresolved candidate would compare unequal to it.
        let dir = TempDir::new().expect("temp dir");
        let own = std::env::current_exe().expect("current exe");
        let name = own.file_name().expect("the exe has a name");

        std::os::unix::fs::symlink(&own, dir.path().join(name)).expect("symlink");
        std::fs::write(dir.path().join("kept.txt"), b"one").expect("write");

        let index = walk(dir.path(), &options()).expect("walk");

        assert_eq!(
            index.len(),
            1,
            "only kept.txt should be reported; got {:?}",
            index.paths().collect::<Vec<_>>()
        );
        assert!(index.contains(Path::new("kept.txt")));
    }

    #[test]
    fn the_running_binary_is_skipped_by_stat_paths_too() {
        // The other route into an index. A scope naming it is unlikely, but
        // "never" has to hold on both paths or it is only a default.
        let dir = TempDir::new().expect("temp dir");
        let own = std::env::current_exe().expect("current exe");
        let name = own.file_name().expect("the exe has a name");

        std::os::unix::fs::symlink(&own, dir.path().join(name)).expect("symlink");
        std::fs::write(dir.path().join("kept.txt"), b"one").expect("write");

        let index = stat_paths(
            dir.path(),
            &named(&[&name.to_string_lossy(), "kept.txt"]),
            &options(),
        )
        .expect("stat");

        assert_eq!(
            index.len(),
            1,
            "only kept.txt should be reported; got {:?}",
            index.paths().collect::<Vec<_>>()
        );
        assert!(index.contains(Path::new("kept.txt")));
    }
}
