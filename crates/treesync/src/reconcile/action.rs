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

    /// A stable name for this variant, for grouping in reports and metrics.
    ///
    /// Fixed and `'static` because it becomes a metric label: the set of
    /// values is the set of variants, and adding one is a deliberate edit here
    /// rather than something a tree can cause by containing an unusual file.
    pub fn kind(&self) -> &'static str {
        match self {
            Action::CreateDir(_) => "create_dir",
            Action::CopyFile(_) => "copy_file",
            Action::CreateSymlink { .. } => "create_symlink",
            Action::Remove(_) => "remove",
            Action::Rename { .. } => "rename",
            Action::SetMetadata { .. } => "set_metadata",
        }
    }
}

/// How many actions of each kind.
///
/// A breakdown rather than one number, because the kinds are not
/// interchangeable to anyone reading it: a hundred `copy_file` is a busy
/// mirror and a hundred `remove` is a mirror about to delete a hundred files.
/// Counting them here rather than at each call site also keeps the per-action
/// work to an increment, so a plan of a million actions still costs one metric
/// call at the end instead of a million.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionCounts {
    pub create_dir: usize,
    pub copy_file: usize,
    pub create_symlink: usize,
    pub remove: usize,
    pub rename: usize,
    pub set_metadata: usize,
}

impl ActionCounts {
    /// Counts one action.
    pub fn record(&mut self, action: &Action) {
        let slot = match action {
            Action::CreateDir(_) => &mut self.create_dir,
            Action::CopyFile(_) => &mut self.copy_file,
            Action::CreateSymlink { .. } => &mut self.create_symlink,
            Action::Remove(_) => &mut self.remove,
            Action::Rename { .. } => &mut self.rename,
            Action::SetMetadata { .. } => &mut self.set_metadata,
        };

        *slot += 1;
    }

    /// Actions of every kind.
    pub fn total(&self) -> usize {
        self.create_dir
            + self.copy_file
            + self.create_symlink
            + self.remove
            + self.rename
            + self.set_metadata
    }

    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }

    /// Each kind with its count, named as [`Action::kind`] names it.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, usize)> {
        [
            ("create_dir", self.create_dir),
            ("copy_file", self.copy_file),
            ("create_symlink", self.create_symlink),
            ("remove", self.remove),
            ("rename", self.rename),
            ("set_metadata", self.set_metadata),
        ]
        .into_iter()
    }
}

impl std::fmt::Display for ActionCounts {
    /// Only the kinds that occurred, so a routine plan reads as `copy_file 3`
    /// rather than five zeroes and the one number that matters.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;

        for (kind, count) in self.iter() {
            if count == 0 {
                continue;
            }

            if !first {
                write!(f, ", ")?;
            }

            write!(f, "{kind} {count}")?;
            first = false;
        }

        if first {
            write!(f, "nothing")?;
        }

        Ok(())
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

    /// The plan broken down by action kind.
    pub fn counts(&self) -> ActionCounts {
        let mut counts = ActionCounts::default();

        for action in &self.actions {
            counts.record(action);
        }

        counts
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

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_of(actions: Vec<Action>) -> Plan {
        Plan { actions }
    }

    #[test]
    fn counts_are_broken_down_by_kind() {
        let counts = plan_of(vec![
            Action::CopyFile(PathBuf::from("a")),
            Action::CopyFile(PathBuf::from("b")),
            Action::CreateDir(PathBuf::from("d")),
            Action::Remove(PathBuf::from("gone")),
        ])
        .counts();

        assert_eq!(counts.copy_file, 2);
        assert_eq!(counts.create_dir, 1);
        assert_eq!(counts.remove, 1);
        assert_eq!(counts.set_metadata, 0);
        assert_eq!(counts.total(), 4);
    }

    #[test]
    fn an_empty_plan_counts_nothing() {
        let counts = Plan::default().counts();

        assert!(counts.is_empty());
        assert_eq!(counts.total(), 0);
    }

    #[test]
    fn every_kind_has_a_name_and_a_slot() {
        // A variant added without a slot in `ActionCounts` would be counted
        // under whichever arm was copied to make it, and the total would still
        // look right. This catches that.
        let one_of_each = vec![
            Action::CreateDir(PathBuf::from("d")),
            Action::CopyFile(PathBuf::from("f")),
            Action::CreateSymlink {
                path: PathBuf::from("l"),
                target: PathBuf::from("t"),
            },
            Action::Remove(PathBuf::from("r")),
            Action::Rename {
                from: PathBuf::from("a"),
                to: PathBuf::from("b"),
            },
            Action::SetMetadata {
                path: PathBuf::from("m"),
                metadata: Metadata {
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
            },
        ];

        let kinds: Vec<&str> = one_of_each.iter().map(Action::kind).collect();
        let counts = plan_of(one_of_each).counts();

        assert_eq!(counts.total(), 6);
        assert!(
            counts.iter().all(|(_, count)| count == 1),
            "every slot should have exactly one: {counts:?}"
        );

        let named: Vec<&str> = counts.iter().map(|(kind, _)| kind).collect();
        for kind in kinds {
            assert!(named.contains(&kind), "{kind} is not in {named:?}");
        }
    }

    #[test]
    fn display_names_only_what_happened() {
        let counts = plan_of(vec![
            Action::CopyFile(PathBuf::from("a")),
            Action::CopyFile(PathBuf::from("b")),
            Action::Remove(PathBuf::from("c")),
        ])
        .counts();

        assert_eq!(counts.to_string(), "copy_file 2, remove 1");
        assert_eq!(ActionCounts::default().to_string(), "nothing");
    }
}
