use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// How much of the tree a pending re-walk has to cover.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scope {
    /// Nothing has been lost.
    Clean,
    /// Everything lost so far lies under this directory.
    Subtree(PathBuf),
    /// What was lost is unknown; the whole watch root must be walked.
    Everything,
}

/// Tracks whether a re-walk is owed, and how much of the tree it must cover.
///
/// Narrowing matters because a rescan is the expensive path. A burst usually
/// has locality, whether a build writing into one output directory, an unpack
/// or a checkout, so the directory containing the lost events is often far smaller
/// than the watch root.
#[derive(Debug)]
pub(super) struct RescanSignal {
    /// Read on every `recv`, so it is kept lock-free. The mutex below is only
    /// taken when something has actually been lost.
    pending: AtomicBool,
    scope: Mutex<Scope>,
}

impl RescanSignal {
    pub(super) fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            scope: Mutex::new(Scope::Clean),
        }
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// Records that an event for `path` was lost, widening the scope to cover it.
    ///
    /// The *containing directory* is folded in rather than the path itself: a
    /// re-walk enumerates a directory, and the entry may already be gone.
    pub(super) fn lost(&self, path: &Path) {
        self.widen(path.parent().unwrap_or(path));
    }

    /// Records that whatever is inside `directory` may never have been reported.
    ///
    /// Distinct from [`Self::lost`], which folds in a path's *parent* because
    /// the path itself may be gone. Here the directory is precisely the extent
    /// of the doubt, and widening to its parent would re-walk a sibling tree
    /// that nothing suggested was stale.
    pub(super) fn lost_under(&self, directory: &Path) {
        self.widen(directory);
    }

    fn widen(&self, directory: &Path) {
        let mut scope = self.scope.lock().unwrap_or_else(|err| err.into_inner());
        *scope = match std::mem::replace(&mut *scope, Scope::Clean) {
            Scope::Clean => Scope::Subtree(directory.to_path_buf()),
            Scope::Subtree(existing) => Scope::Subtree(common_ancestor(&existing, directory)),
            Scope::Everything => Scope::Everything,
        };

        // Ordered after the scope write so a reader that sees `pending` also
        // sees the widened scope.
        self.pending.store(true, Ordering::Release);
    }

    /// Records a loss whose extent cannot be determined, such as a kernel queue
    /// overflow: the backend does not say what it discarded.
    pub(super) fn lost_everything(&self) {
        *self.scope.lock().unwrap_or_else(|err| err.into_inner()) = Scope::Everything;
        self.pending.store(true, Ordering::Release);
    }

    /// Consumes the pending signal, returning the directory to walk.
    ///
    /// Falls back to `root` when the extent is unknown. Also clamps to `root`:
    /// a scope is only ever narrowed from paths inside the tree, but a walk
    /// must never be pointed outside it.
    pub(super) fn take(&self, root: &Path) -> PathBuf {
        let mut scope = self.scope.lock().unwrap_or_else(|err| err.into_inner());
        let taken = std::mem::replace(&mut *scope, Scope::Clean);
        self.pending.store(false, Ordering::Release);

        match taken {
            Scope::Subtree(path) if path.starts_with(root) => path,
            _ => root.to_path_buf(),
        }
    }
}

/// The deepest directory that contains both paths.
fn common_ancestor(a: &Path, b: &Path) -> PathBuf {
    let mut shared = PathBuf::new();

    for (left, right) in a.components().zip(b.components()) {
        if left != right {
            break;
        }

        shared.push(left);
    }

    shared
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        PathBuf::from("/tree")
    }

    #[test]
    fn starts_clean() {
        let signal = RescanSignal::new();

        assert!(!signal.is_pending());
    }

    #[test]
    fn a_single_loss_scopes_to_the_containing_directory() {
        let signal = RescanSignal::new();

        signal.lost(Path::new("/tree/a/b/file.txt"));

        assert!(signal.is_pending());
        assert_eq!(signal.take(&root()), PathBuf::from("/tree/a/b"));
    }

    #[test]
    fn losses_in_one_directory_stay_narrow() {
        let signal = RescanSignal::new();

        signal.lost(Path::new("/tree/build/one.o"));
        signal.lost(Path::new("/tree/build/two.o"));
        signal.lost(Path::new("/tree/build/three.o"));

        assert_eq!(
            signal.take(&root()),
            PathBuf::from("/tree/build"),
            "a burst confined to one directory must not force a whole-tree walk"
        );
    }

    #[test]
    fn losses_across_directories_widen_to_their_ancestor() {
        let signal = RescanSignal::new();

        signal.lost(Path::new("/tree/build/debug/one.o"));
        signal.lost(Path::new("/tree/build/release/two.o"));

        assert_eq!(signal.take(&root()), PathBuf::from("/tree/build"));
    }

    #[test]
    fn unrelated_losses_widen_to_the_root() {
        let signal = RescanSignal::new();

        signal.lost(Path::new("/tree/a/one"));
        signal.lost(Path::new("/tree/b/two"));

        assert_eq!(signal.take(&root()), root());
    }

    #[test]
    fn an_unbounded_loss_forces_the_whole_root() {
        let signal = RescanSignal::new();

        signal.lost(Path::new("/tree/a/b/file.txt"));
        signal.lost_everything();

        assert_eq!(
            signal.take(&root()),
            root(),
            "a kernel drop says nothing about what was lost"
        );
    }

    #[test]
    fn a_narrow_loss_cannot_undo_an_unbounded_one() {
        let signal = RescanSignal::new();

        signal.lost_everything();
        signal.lost(Path::new("/tree/a/b/file.txt"));

        assert_eq!(signal.take(&root()), root());
    }

    #[test]
    fn taking_clears_the_signal() {
        let signal = RescanSignal::new();
        signal.lost(Path::new("/tree/a/file"));

        signal.take(&root());

        assert!(!signal.is_pending());
        assert_eq!(
            signal.take(&root()),
            root(),
            "a second take with nothing pending falls back to the root"
        );
    }

    #[test]
    fn a_scope_outside_the_root_is_clamped() {
        let signal = RescanSignal::new();

        // Only reachable if a backend reported a path outside the watch.
        signal.lost(Path::new("/elsewhere/file"));

        assert_eq!(signal.take(&root()), root());
    }
}
