//! Decides what the target needs in order to match the source.
//!
//! The watcher says something happened; the queue says which paths. Neither is
//! trusted about *what* happened. This layer stats the paths and compares the
//! two trees, because the filesystem is the only authority (see
//! [`crate::queue`] for why the event kind is not carried this far).
//!
//! # Why an index
//!
//! rsync rebuilds its file list on every invocation, so its cost is O(tree)
//! whether one file changed or a million did. A resident daemon can keep the
//! tree in memory and consult it in O(changes). That index is owned by a single
//! task and passed by message instead of shared, so no locks and therefore no
//! lock-ordering bugs.
//!
//! # Scopes
//!
//! An incremental batch can only reconcile the paths it names. It cannot
//! discover a deletion nobody reported, which is exactly the gap a
//! [`Scope::Subtree`] closes: comparing every target entry beneath a directory
//! against the source finds entries the source no longer has. That is what
//! makes a rescan a genuine repair and not just replayed work.

mod action;
mod filter;
mod index;

pub use action::{Action, Plan};
pub use filter::Filter;
pub use index::{Entry, Index, Metadata, index_scope, stat_paths, walk, walk_subtree};

use std::path::{Path, PathBuf};

/// Which part of the tree a reconcile covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Compare only these paths. Cannot detect deletions elsewhere.
    Paths(Vec<PathBuf>),
    /// Compare everything beneath this directory, in both trees. An empty path
    /// means the whole tree.
    Subtree(PathBuf),
}

/// How closely two files are compared before deciding they match.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verify {
    /// Size and modification time. One `stat` per file.
    ///
    /// The default, and what rsync does without `--checksum`. Misses a rewrite
    /// that leaves both unchanged.
    #[default]
    Quick,

    /// Also compare content hashes.
    ///
    /// Reads every candidate file on both sides, so the cost is proportional to
    /// the data in scope rather than to the number of files. Worth it where a
    /// missed change matters more than the I/O, or on filesystems whose mtime
    /// granularity is a whole second.
    Checksum,
}

/// Which attributes are mirrored onto the target.
///
/// Content, modification time and symlink targets are always replicated. A
/// copy that did not preserve mtime would re-transfer every file on every pass.
/// These are the ones with a reason to be optional.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preserve {
    /// Permission bits.
    ///
    /// On by default. A mirrored executable that arrives without its execute
    /// bit is not a useful mirror.
    #[serde(default = "default_true")]
    pub mode: bool,

    /// Owning uid and gid.
    ///
    /// Off by default: `chown` requires privilege, so for an unprivileged
    /// process every file would report a failure it can do nothing about. The
    /// uids also have to mean the same thing on both ends, which across hosts
    /// or containers they frequently do not.
    #[serde(default)]
    pub ownership: bool,
}

fn default_true() -> bool {
    true
}

impl Default for Preserve {
    fn default() -> Self {
        Self {
            mode: true,
            ownership: false,
        }
    }
}

/// What to include in an index, and how carefully to compare it.
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// Paths excluded from the sync, applied to both trees.
    pub filter: Filter,
    pub verify: Verify,
}

impl IndexOptions {
    /// Everything included, compared by size and mtime.
    pub fn quick() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileConfig {
    /// Whether to remove target paths the source no longer has.
    ///
    /// Off by default. A bug in the walk, or a source directory that is
    /// transiently unreadable, would otherwise be indistinguishable from a
    /// legitimate deletion, and the target is the copy that has no backup.
    pub delete: bool,

    /// How closely two files are compared before deciding they match.
    ///
    /// Only consulted when both indexes were built with the same setting; see
    /// [`IndexOptions`].
    pub verify: Verify,

    /// Which attributes are mirrored onto the target.
    pub preserve: Preserve,
}

/// Produces the operations that would bring `target` in line with `source`.
///
/// Pure: it reads two snapshots and returns a plan. Nothing is applied here, so
/// a caller can log or discard the plan, which is what a dry run is.
pub fn plan(source: &Index, target: &Index, scope: &Scope, config: &ReconcileConfig) -> Plan {
    let mut plan = Plan::default();

    match scope {
        Scope::Paths(paths) => {
            for path in paths {
                // `under` is inclusive of the prefix, so this covers the named
                // path itself as well as anything the index holds beneath it.
                //
                // There *is* something beneath it whenever the path is a
                // directory: [`stat_paths`](crate::reconcile::stat_paths) walks
                // one rather than merely stat'ing it, because a single reported
                // directory can be the only trace of a whole subtree the
                // watcher never saw created. Comparing just the named path
                // would then create an empty directory on the target and
                // silently drop everything inside it.
                for (candidate, _) in source.under(path) {
                    push_comparison(&mut plan, source, target, candidate, config);
                }

                // Paths the target holds under this one that the source does
                // not. Same rule as the subtree case below: no event named
                // them, and only a comparison of the whole subtree can find
                // them. Safe because both indexes were built from this same
                // scope, so both walked this directory.
                if config.delete {
                    for (candidate, _) in target.under(path) {
                        if !source.contains(candidate) {
                            plan.actions.push(Action::Remove(candidate.clone()));
                        }
                    }
                }
            }
        }
        Scope::Subtree(prefix) => {
            for (path, _) in source.under(prefix) {
                push_comparison(&mut plan, source, target, path, config);
            }

            // Only a subtree comparison can find these: an entry the target
            // still has and the source does not, which no event ever named.
            if config.delete {
                for (path, _) in target.under(prefix) {
                    if !source.contains(path) {
                        plan.actions.push(Action::Remove(path.clone()));
                    }
                }
            }
        }
    }

    plan.order();
    plan
}

/// Compares one path between the trees and records what the target needs.
fn push_comparison(
    plan: &mut Plan,
    source: &Index,
    target: &Index,
    path: &Path,
    config: &ReconcileConfig,
) {
    match (source.get(path), target.get(path)) {
        // Gone from the source.
        (None, Some(_)) => {
            if config.delete {
                plan.actions.push(Action::Remove(path.to_path_buf()));
            }
        }
        // Absent from both: nothing to do. Reached routinely, because a path
        // created and deleted inside one window is reported but never existed
        // as far as either snapshot is concerned.
        (None, None) => {}
        (Some(entry), existing) => {
            let needs_replacing = match existing {
                None => true,
                Some(existing) => entry.differs_from(existing),
            };

            // Deliberately not an early return: content can match while
            // permissions do not, and a mode change alone is still work.
            if needs_replacing {
                // A kind change cannot be done in place: a directory cannot be
                // overwritten by a file, and replacing a directory means
                // removing what it contained.
                if let Some(existing) = existing
                    && existing.is_dir() != entry.is_dir()
                    && config.delete
                {
                    plan.actions.push(Action::Remove(path.to_path_buf()));
                }

                plan.actions.push(match entry {
                    Entry::Dir { .. } => Action::CreateDir(path.to_path_buf()),
                    Entry::File { .. } => Action::CopyFile(path.to_path_buf()),
                    Entry::Symlink { target } => Action::CreateSymlink {
                        path: path.to_path_buf(),
                        target: target.clone(),
                    },
                });
            }
        }
    }

    push_metadata(plan, source.get(path), target.get(path), path, config);
}

/// Records a metadata change when the selected attributes differ.
///
/// Emitted alongside a content change as well as on its own: a freshly created
/// directory gets the process umask, and a copied file inherits whatever the
/// copy produced, neither of which is necessarily the source's mode.
fn push_metadata(
    plan: &mut Plan,
    source: Option<&Entry>,
    target: Option<&Entry>,
    path: &Path,
    config: &ReconcileConfig,
) {
    if !config.preserve.mode && !config.preserve.ownership {
        return;
    }

    let Some(wanted) = source.and_then(Entry::metadata) else {
        return;
    };

    // A path the target does not have yet is always worth stamping: whatever
    // creates it will not have used the source's mode.
    let needed = match target.and_then(Entry::metadata) {
        None => true,
        Some(current) => wanted.differs_from(current, config.preserve),
    };

    if needed {
        plan.actions.push(Action::SetMetadata {
            path: path.to_path_buf(),
            metadata: *wanted,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn file(size: u64) -> Entry {
        Entry::File {
            size,
            mtime: SystemTime::UNIX_EPOCH,
            hash: None,
            meta: meta(),
        }
    }

    fn file_at(size: u64, secs: u64) -> Entry {
        Entry::File {
            size,
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
            hash: None,
            meta: meta(),
        }
    }

    fn meta() -> Metadata {
        Metadata {
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    fn dir() -> Entry {
        Entry::Dir { meta: meta() }
    }

    fn link(target: &str) -> Entry {
        Entry::Symlink {
            target: PathBuf::from(target),
        }
    }

    fn index(entries: &[(&str, Entry)]) -> Index {
        let mut index = Index::new();
        for (path, entry) in entries {
            index.insert(*path, entry.clone());
        }

        index
    }

    fn scope(paths: &[&str]) -> Scope {
        Scope::Paths(paths.iter().map(PathBuf::from).collect())
    }

    fn everything() -> Scope {
        Scope::Subtree(PathBuf::new())
    }

    /// Deletions on, metadata off. Preservation has its own tests; leaving it
    /// on here would add a `SetMetadata` to every expected plan and bury what
    /// each case is actually asserting.
    fn deleting() -> ReconcileConfig {
        ReconcileConfig {
            delete: true,
            preserve: Preserve {
                mode: false,
                ownership: false,
            },
            ..Default::default()
        }
    }

    fn preserving_mode() -> ReconcileConfig {
        ReconcileConfig {
            delete: true,
            preserve: Preserve {
                mode: true,
                ownership: false,
            },
            ..Default::default()
        }
    }

    fn with_mode(mode: u32) -> Entry {
        Entry::File {
            size: 10,
            mtime: SystemTime::UNIX_EPOCH,
            hash: None,
            meta: Metadata {
                mode,
                uid: 0,
                gid: 0,
            },
        }
    }

    #[test]
    fn a_new_file_is_copied() {
        let source = index(&[("a.txt", file(10))]);

        let plan = plan(
            &source,
            &Index::new(),
            &scope(&["a.txt"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert_eq!(plan.actions, vec![Action::CopyFile(PathBuf::from("a.txt"))]);
    }

    #[test]
    fn an_identical_file_is_left_alone() {
        let source = index(&[("a.txt", file(10))]);
        let target = index(&[("a.txt", file(10))]);

        let plan = plan(
            &source,
            &target,
            &scope(&["a.txt"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert!(plan.is_empty(), "matching files must not be re-transferred");
    }

    #[test]
    fn a_size_change_is_copied() {
        let source = index(&[("a.txt", file(20))]);
        let target = index(&[("a.txt", file(10))]);

        let plan = plan(
            &source,
            &target,
            &scope(&["a.txt"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert_eq!(plan.actions, vec![Action::CopyFile(PathBuf::from("a.txt"))]);
    }

    #[test]
    fn an_mtime_change_at_the_same_size_is_copied() {
        let source = index(&[("a.txt", file_at(10, 500))]);
        let target = index(&[("a.txt", file_at(10, 100))]);

        let plan = plan(
            &source,
            &target,
            &scope(&["a.txt"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert_eq!(
            plan.actions,
            vec![Action::CopyFile(PathBuf::from("a.txt"))],
            "a same-size rewrite must still be caught"
        );
    }

    #[test]
    fn a_symlink_is_replicated_not_followed() {
        let source = index(&[("link", link("/etc/passwd"))]);

        let plan = plan(
            &source,
            &Index::new(),
            &scope(&["link"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert_eq!(
            plan.actions,
            vec![Action::CreateSymlink {
                path: PathBuf::from("link"),
                target: PathBuf::from("/etc/passwd"),
            }],
            "a link out of the tree must stay a link, never be dereferenced"
        );
    }

    #[test]
    fn a_repointed_symlink_is_replaced() {
        let source = index(&[("link", link("b"))]);
        let target = index(&[("link", link("a"))]);

        let plan = plan(
            &source,
            &target,
            &scope(&["link"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn deletions_are_withheld_unless_asked_for() {
        let target = index(&[("stale.txt", file(10))]);

        let plan = plan(
            &Index::new(),
            &target,
            &scope(&["stale.txt"]),
            &ReconcileConfig {
                preserve: Preserve {
                    mode: false,
                    ownership: false,
                },
                ..Default::default()
            },
        );

        assert!(
            plan.is_empty(),
            "destructive propagation must be opt-in; the target is the copy with no backup"
        );
    }

    #[test]
    fn deletions_are_emitted_when_asked_for() {
        let target = index(&[("stale.txt", file(10))]);

        let plan = plan(&Index::new(), &target, &scope(&["stale.txt"]), &deleting());

        assert_eq!(
            plan.actions,
            vec![Action::Remove(PathBuf::from("stale.txt"))]
        );
    }

    #[test]
    fn a_path_absent_from_both_trees_produces_nothing() {
        // Routine: a file created and removed inside one window is reported by
        // the queue but never existed as far as either snapshot is concerned.
        let plan = plan(
            &Index::new(),
            &Index::new(),
            &scope(&["transient.tmp"]),
            &deleting(),
        );

        assert!(plan.is_empty());
    }

    #[test]
    fn a_file_replacing_a_directory_removes_it_first() {
        let source = index(&[("thing", file(10))]);
        let target = index(&[("thing", dir())]);

        let plan = plan(&source, &target, &scope(&["thing"]), &deleting());

        assert_eq!(
            plan.actions,
            vec![
                Action::Remove(PathBuf::from("thing")),
                Action::CopyFile(PathBuf::from("thing")),
            ],
            "a directory cannot be overwritten in place by a file"
        );
    }

    #[test]
    fn a_named_path_scope_cannot_discover_unreported_deletions() {
        let source = index(&[("kept.txt", file(10))]);
        let target = index(&[("kept.txt", file(10)), ("orphan.txt", file(10))]);

        let plan = plan(&source, &target, &scope(&["kept.txt"]), &deleting());

        assert!(
            plan.is_empty(),
            "an incremental batch only knows about the paths it names"
        );
    }

    #[test]
    fn a_subtree_scope_discovers_unreported_deletions() {
        let source = index(&[("kept.txt", file(10))]);
        let target = index(&[("kept.txt", file(10)), ("orphan.txt", file(10))]);

        let plan = plan(&source, &target, &everything(), &deleting());

        assert_eq!(
            plan.actions,
            vec![Action::Remove(PathBuf::from("orphan.txt"))],
            "this is what makes a rescan a repair rather than replayed work"
        );
    }

    #[test]
    fn a_subtree_scope_ignores_paths_outside_it() {
        let source = index(&[("build/a.o", file(10))]);
        let target = index(&[("src/orphan.rs", file(10))]);

        let plan = plan(
            &source,
            &target,
            &Scope::Subtree(PathBuf::from("build")),
            &deleting(),
        );

        assert_eq!(
            plan.actions,
            vec![Action::CopyFile(PathBuf::from("build/a.o"))],
            "a scoped rescan must not touch what it did not cover"
        );
    }

    #[test]
    fn creations_are_ordered_parents_before_children() {
        let source = index(&[
            ("a/b/c/deep.txt", file(10)),
            ("a", dir()),
            ("a/b/c", dir()),
            ("a/b", dir()),
        ]);

        let plan = plan(&source, &Index::new(), &everything(), &deleting());

        assert_eq!(
            plan.actions,
            vec![
                Action::CreateDir(PathBuf::from("a")),
                Action::CreateDir(PathBuf::from("a/b")),
                Action::CreateDir(PathBuf::from("a/b/c")),
                Action::CopyFile(PathBuf::from("a/b/c/deep.txt")),
            ],
            "a file cannot be written into a directory that does not exist yet"
        );
    }

    #[test]
    fn removals_are_ordered_children_before_parents() {
        let target = index(&[("a", dir()), ("a/b", dir()), ("a/b/deep.txt", file(10))]);

        let plan = plan(&Index::new(), &target, &everything(), &deleting());

        assert_eq!(
            plan.actions,
            vec![
                Action::Remove(PathBuf::from("a/b/deep.txt")),
                Action::Remove(PathBuf::from("a/b")),
                Action::Remove(PathBuf::from("a")),
            ],
            "removing a parent first leaves the child removals pointing at nothing"
        );
    }

    #[test]
    fn a_mode_change_alone_is_a_metadata_action() {
        let source = index(&[("a.txt", with_mode(0o755))]);
        let target = index(&[("a.txt", with_mode(0o644))]);

        let plan = plan(&source, &target, &scope(&["a.txt"]), &preserving_mode());

        assert_eq!(
            plan.actions,
            vec![Action::SetMetadata {
                path: PathBuf::from("a.txt"),
                metadata: Metadata {
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                },
            }],
            "identical content with a different mode must not re-transfer the file"
        );
    }

    #[test]
    fn a_matching_mode_produces_nothing() {
        let source = index(&[("a.txt", with_mode(0o644))]);
        let target = index(&[("a.txt", with_mode(0o644))]);

        let plan = plan(&source, &target, &scope(&["a.txt"]), &preserving_mode());

        assert!(plan.is_empty());
    }

    #[test]
    fn a_new_file_is_stamped_as_well_as_copied() {
        let source = index(&[("a.txt", with_mode(0o755))]);

        let plan = plan(
            &source,
            &Index::new(),
            &scope(&["a.txt"]),
            &preserving_mode(),
        );

        // Whatever creates the file will not have used the source's mode, so a
        // copy alone would land an executable without its execute bit.
        assert_eq!(
            plan.actions,
            vec![
                Action::CopyFile(PathBuf::from("a.txt")),
                Action::SetMetadata {
                    path: PathBuf::from("a.txt"),
                    metadata: Metadata {
                        mode: 0o755,
                        uid: 0,
                        gid: 0,
                    },
                },
            ]
        );
    }

    #[test]
    fn ownership_is_ignored_unless_asked_for() {
        let source = index(&[(
            "a.txt",
            Entry::File {
                size: 10,
                mtime: SystemTime::UNIX_EPOCH,
                hash: None,
                meta: Metadata {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            },
        )]);
        let target = index(&[("a.txt", with_mode(0o644))]);

        let mode_only = plan(&source, &target, &scope(&["a.txt"]), &preserving_mode());
        assert!(
            mode_only.is_empty(),
            "chown is privileged, so a uid difference alone must not act by default"
        );

        let with_ownership = ReconcileConfig {
            delete: true,
            preserve: Preserve {
                mode: true,
                ownership: true,
            },
            ..Default::default()
        };
        let both = plan(&source, &target, &scope(&["a.txt"]), &with_ownership);
        assert_eq!(both.len(), 1);
    }

    #[test]
    fn metadata_is_applied_after_everything_is_created() {
        let source = index(&[
            (
                "dir",
                Entry::Dir {
                    meta: Metadata {
                        mode: 0o500,
                        uid: 0,
                        gid: 0,
                    },
                },
            ),
            ("dir/inner.txt", with_mode(0o644)),
        ]);

        let plan = plan(&source, &Index::new(), &everything(), &preserving_mode());

        let last_write = plan
            .actions
            .iter()
            .rposition(|action| !action.is_metadata())
            .expect("there are writes");
        let first_metadata = plan
            .actions
            .iter()
            .position(Action::is_metadata)
            .expect("there is metadata");

        // Tightening `dir` to 0o500 before writing into it would fail the copy.
        assert!(
            first_metadata > last_write,
            "metadata must follow every creation, got {:?}",
            plan.actions
        );
    }

    #[test]
    fn removals_run_before_creations() {
        let source = index(&[("thing", dir())]);
        let target = index(&[("thing", file(10))]);

        let plan = plan(&source, &target, &everything(), &deleting());

        assert_eq!(
            plan.actions,
            vec![
                Action::Remove(PathBuf::from("thing")),
                Action::CreateDir(PathBuf::from("thing")),
            ]
        );
    }
}
