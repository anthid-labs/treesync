use std::path::PathBuf;

use super::Metadata;

/// One operation to apply to the target.
///
/// Paths are relative to the two roots, so the same plan can be executed
/// against a local directory or a remote agent without rewriting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create a directory, and any missing parents.
    CreateDir(PathBuf),
    /// Copy file content from source to target, replacing whatever is there.
    CopyFile(PathBuf),
    /// Create or replace a symlink pointing at `target`.
    CreateSymlink { path: PathBuf, target: PathBuf },
    /// Remove a path. For a directory, everything beneath it goes too.
    Remove(PathBuf),
    /// Move an existing target path rather than re-transferring it.
    Rename { from: PathBuf, to: PathBuf },
    /// Apply the source's ownership and permissions to an existing path.
    SetMetadata { path: PathBuf, metadata: Metadata },
}

impl Action {
    /// The path this action operates on, for ordering and logging.
    pub fn path(&self) -> &PathBuf {
        match self {
            Action::CreateDir(path)
            | Action::CopyFile(path)
            | Action::CreateSymlink { path, .. }
            | Action::Remove(path)
            | Action::SetMetadata { path, .. } => path,
            Action::Rename { to, .. } => to,
        }
    }

    pub fn is_remove(&self) -> bool {
        matches!(self, Action::Remove(_))
    }

    pub fn is_metadata(&self) -> bool {
        matches!(self, Action::SetMetadata { .. })
    }
}

/// An ordered set of operations.
///
/// Order is part of the contract: applying these in sequence must succeed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub actions: Vec<Action>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }

    /// Puts the actions in an order that can be executed top to bottom.
    ///
    /// Two rules, in tension, which is why removals are kept as a separate
    /// group rather than sorted alongside everything else:
    ///
    /// - Creations run parents-first: a file cannot be written into a directory
    ///   that does not exist yet.
    /// - Removals run children-first: on the wire a directory removal is not
    ///   necessarily recursive, and deleting a parent before its children makes
    ///   the child removals fail against a path that is already gone.
    ///
    /// Removals also come before creations overall, so that replacing a file
    /// with a directory of the same name does not collide.
    ///
    /// Metadata comes last, after every creation. Applying a directory's mode
    /// earlier can make it unwritable, since mirroring a `0500` directory and
    /// then creating files inside it fails, so permissions are the last thing
    /// tightened.
    pub(super) fn order(&mut self) {
        let mut removals = Vec::new();
        let mut writes = Vec::new();
        let mut metadata = Vec::new();

        for action in std::mem::take(&mut self.actions) {
            if action.is_remove() {
                removals.push(action);
            } else if action.is_metadata() {
                metadata.push(action);
            } else {
                writes.push(action);
            }
        }

        removals.sort_by(deepest_first);
        writes.sort_by(shallowest_first);
        metadata.sort_by(deepest_first);

        removals.append(&mut writes);
        removals.append(&mut metadata);
        self.actions = removals;
    }
}

fn depth(action: &Action) -> usize {
    action.path().components().count()
}

fn shallowest_first(a: &Action, b: &Action) -> std::cmp::Ordering {
    depth(a).cmp(&depth(b)).then_with(|| a.path().cmp(b.path()))
}

fn deepest_first(a: &Action, b: &Action) -> std::cmp::Ordering {
    depth(b).cmp(&depth(a)).then_with(|| b.path().cmp(a.path()))
}
