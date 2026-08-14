use std::path::PathBuf;

/// What happened to a path.
///
/// Deliberately narrower than [`notify::EventKind`]: these are the categories
/// the sync engine can act on, and they line up with the event vocabulary the
/// batching layer needs in order to collapse a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A file or directory appeared.
    Create,
    /// Contents changed.
    Modify,
    /// Metadata changed (permissions, ownership, times) but contents did not.
    Attrib,
    /// A file or directory was removed, or renamed out of the watched tree.
    Delete,
    /// The source half of a rename. Pairs with a [`EventKind::MovedTo`] that
    /// carries the same `tracker`.
    MovedFrom,
    /// The destination half of a rename.
    MovedTo,
}

/// A single filesystem change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsEvent {
    pub kind: EventKind,
    pub path: PathBuf,
    /// Backend-supplied rename cookie, used to pair [`EventKind::MovedFrom`]
    /// with its [`EventKind::MovedTo`]. `None` when the backend does not
    /// correlate the two halves, in which case the pairing has to fall back to
    /// heuristics.
    pub tracker: Option<usize>,
}

/// What the watcher hands to its consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// A change was observed.
    Fs(FsEvent),
    /// Events were lost and the watched tree no longer matches what the
    /// consumer has seen.
    ///
    /// Raised when the kernel drops events (inotify's `IN_Q_OVERFLOW`, an
    /// FSEvents kernel drop) or when the event queue fills because the consumer
    /// fell behind. Both mean the same thing: there is a gap, and no sequence
    /// of individual events can close it.
    ///
    /// On receiving this, walk `root` and reconcile it against the target.
    /// Anything still queued is discarded before this is delivered, since those
    /// events describe a state that predates the gap, and the walk observes the
    /// tree as it is now.
    ///
    /// It is expensive, and deliberately so: it is the worst case, not the
    /// common path. lsyncd handles the same condition by tearing down and
    /// restarting the entire daemon.
    Rescan {
        /// Directory to walk, and everything beneath it.
        ///
        /// Narrowed to the smallest directory containing every lost path when
        /// that is knowable, which for a localized burst such as a build
        /// output directory or an unpack is far less than the whole tree. Falls back
        /// to the watch root when the backend does not say what it discarded.
        root: PathBuf,
    },
}
