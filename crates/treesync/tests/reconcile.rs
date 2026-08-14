//! Exercises the walk and the planner against real directory trees.

use std::path::{Path, PathBuf};

use tempfile::TempDir;
use treesync::reconcile::{Action, Entry, IndexOptions, ReconcileConfig, Scope, plan, walk};

fn tree(entries: &[(&str, &str)]) -> TempDir {
    let dir = TempDir::new().expect("temp dir");

    for (path, contents) in entries {
        let full = dir.path().join(path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).expect("create parents");
        }

        std::fs::write(&full, contents).expect("write");
    }

    dir
}

/// Deletions on, metadata off. Preservation is covered separately; leaving it
/// on here would add a `SetMetadata` to every expected plan.
fn deleting() -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        preserve: treesync::reconcile::Preserve {
            mode: false,
            ownership: false,
        },
        ..Default::default()
    }
}

fn everything() -> Scope {
    Scope::Subtree(PathBuf::new())
}

#[test]
fn walks_a_nested_tree() {
    let dir = tree(&[
        ("a.txt", "one"),
        ("sub/b.txt", "two"),
        ("sub/deep/c.txt", "three"),
    ]);

    let index = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    // Three files plus the two directories holding them.
    assert_eq!(index.len(), 5);
    assert_eq!(
        index.get(Path::new("a.txt")),
        Some(&Entry::File {
            size: 3,
            mtime: index_mtime(&index, "a.txt"),
            hash: None,
            meta: *index
                .get(Path::new("a.txt"))
                .and_then(Entry::metadata)
                .expect("a file has metadata"),
        })
    );
    assert!(matches!(
        index.get(Path::new("sub")),
        Some(Entry::Dir { .. })
    ));
    assert!(matches!(
        index.get(Path::new("sub/deep")),
        Some(Entry::Dir { .. })
    ));
}

fn index_mtime(index: &treesync::reconcile::Index, path: &str) -> std::time::SystemTime {
    match index.get(Path::new(path)) {
        Some(Entry::File { mtime, .. }) => *mtime,
        other => panic!("expected a file at {path}, got {other:?}"),
    }
}

#[test]
fn paths_are_relative_to_the_walk_root() {
    let dir = tree(&[("sub/b.txt", "two")]);

    let index = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    assert!(
        index.paths().all(|path| path.is_relative()),
        "absolute paths would not compare across two differently-rooted trees"
    );
}

#[test]
fn records_symlinks_without_following_them() {
    let dir = TempDir::new().expect("temp dir");
    let inside = dir.path().join("real.txt");
    std::fs::write(&inside, "content").expect("write");

    // Points outside the tree entirely: following this would copy data the
    // source never owned.
    std::os::unix::fs::symlink("/etc/hosts", dir.path().join("escape")).expect("symlink");
    std::os::unix::fs::symlink("real.txt", dir.path().join("local")).expect("symlink");

    let index = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    assert_eq!(
        index.get(Path::new("escape")),
        Some(&Entry::Symlink {
            target: PathBuf::from("/etc/hosts")
        })
    );
    assert_eq!(
        index.get(Path::new("local")),
        Some(&Entry::Symlink {
            target: PathBuf::from("real.txt")
        })
    );
}

#[test]
fn a_symlink_cycle_terminates() {
    let dir = TempDir::new().expect("temp dir");
    std::fs::create_dir(dir.path().join("a")).expect("mkdir");
    // Following this would recurse forever.
    std::os::unix::fs::symlink(dir.path(), dir.path().join("a/loop")).expect("symlink");

    let index = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    assert_eq!(
        index.len(),
        2,
        "the loop is recorded as a link, not descended"
    );
}

#[test]
fn a_missing_root_is_reported_as_not_found() {
    let dir = TempDir::new().expect("temp dir");

    let err = walk(&dir.path().join("nope"), &IndexOptions::quick()).expect_err("should fail");

    assert!(
        matches!(err, treesync::error::Error::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

#[test]
fn planning_two_real_trees_copies_only_what_differs() {
    let source = tree(&[
        ("same.txt", "identical"),
        ("changed.txt", "long source contents"),
        ("only-source.txt", "new"),
    ]);
    let target = tree(&[
        ("same.txt", "identical"),
        ("changed.txt", "short"),
        ("only-target.txt", "stale"),
    ]);

    // mtimes differ between the two temp trees, so force the one file that is
    // meant to match to share a timestamp with its counterpart.
    let stamp = std::fs::metadata(target.path().join("same.txt"))
        .expect("metadata")
        .modified()
        .expect("mtime");
    filetime::set_file_mtime(
        source.path().join("same.txt"),
        filetime::FileTime::from_system_time(stamp),
    )
    .expect("set mtime");

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk target");

    let plan = plan(&source_index, &target_index, &everything(), &deleting());

    assert_eq!(
        plan.actions,
        vec![
            Action::Remove(PathBuf::from("only-target.txt")),
            Action::CopyFile(PathBuf::from("changed.txt")),
            Action::CopyFile(PathBuf::from("only-source.txt")),
        ],
        "same.txt matches on size and mtime and must not be re-transferred"
    );
}

#[test]
fn planning_an_empty_target_recreates_the_whole_tree_in_order() {
    let source = tree(&[("sub/deep/c.txt", "three")]);

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    let plan = plan(
        &source_index,
        &treesync::reconcile::Index::new(),
        &everything(),
        &deleting(),
    );

    assert_eq!(
        plan.actions,
        vec![
            Action::CreateDir(PathBuf::from("sub")),
            Action::CreateDir(PathBuf::from("sub/deep")),
            Action::CopyFile(PathBuf::from("sub/deep/c.txt")),
        ]
    );
}

#[test]
fn an_unreadable_source_root_never_looks_like_an_empty_tree() {
    let target = tree(&[
        ("important.txt", "data"),
        ("sub/also-important.txt", "data"),
    ]);
    let missing = TempDir::new().expect("temp dir").path().join("gone");

    // The failure this guards: if `walk` skipped the missing root the way it
    // skips a vanished child, it would return an empty index, and an empty
    // source with deletions enabled plans the removal of everything on the
    // target. The error has to surface instead.
    let result = walk(&missing, &IndexOptions::quick());

    assert!(
        result.is_err(),
        "an unreadable source must be an error, not an empty index that deletes the target"
    );

    // And to be explicit about the consequence being guarded against:
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk target");
    let would_delete = plan(
        &treesync::reconcile::Index::new(),
        &target_index,
        &everything(),
        &deleting(),
    );
    assert_eq!(
        would_delete.len(),
        3,
        "an empty source really would wipe the target, which is why walk must fail loudly"
    );
}

#[test]
fn a_file_as_the_source_root_is_rejected() {
    let dir = tree(&[("regular.txt", "data")]);

    let err =
        walk(&dir.path().join("regular.txt"), &IndexOptions::quick()).expect_err("should fail");

    assert!(
        matches!(err, treesync::error::Error::InvalidPath(_)),
        "expected InvalidPath, got {err:?}"
    );
}

/// The shipped example is the first thing an operator copies. If it stops
/// parsing, everyone who starts from it gets a startup error.
#[test]
fn the_example_config_is_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../treesync.example.toml");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()));

    let config = treesync::config::file::Config::parse(&contents).expect("example must parse");
    let resolved = config.resolve();

    assert_eq!(resolved.len(), 2, "example should show both target kinds");
    assert!(
        resolved
            .iter()
            .any(|sync| matches!(sync.target, treesync::config::file::Target::Ssh { .. })),
        "example should document the ssh target"
    );
}

// --- scoped indexing -------------------------------------------------------

#[test]
fn stat_paths_reports_only_what_was_asked_for() {
    let dir = tree(&[("a.txt", "one"), ("b.txt", "two"), ("sub/c.txt", "three")]);

    let index = treesync::reconcile::stat_paths(
        dir.path(),
        &[PathBuf::from("a.txt"), PathBuf::from("sub/c.txt")],
        &IndexOptions::quick(),
    )
    .expect("stat");

    assert_eq!(index.len(), 2, "a batch of two paths must stat two paths");
    assert!(index.contains(Path::new("a.txt")));
    assert!(!index.contains(Path::new("b.txt")));
}

#[test]
fn stat_paths_omits_what_is_gone() {
    let dir = tree(&[("here.txt", "one")]);

    let index = treesync::reconcile::stat_paths(
        dir.path(),
        &[PathBuf::from("here.txt"), PathBuf::from("gone.txt")],
        &IndexOptions::quick(),
    )
    .expect("stat");

    assert_eq!(index.len(), 1);
    assert!(
        !index.contains(Path::new("gone.txt")),
        "absence is how a deletion is observed, not an error"
    );
}

#[test]
fn walking_a_subtree_reports_paths_relative_to_the_whole_tree() {
    let dir = tree(&[
        ("build/a.o", "one"),
        ("build/deep/b.o", "two"),
        ("src/x.rs", "code"),
    ]);

    let index =
        treesync::reconcile::walk_subtree(dir.path(), Path::new("build"), &IndexOptions::quick())
            .expect("walk");

    // Prefixed against the sync root, so this compares directly with a
    // full-tree index rather than needing to be rebased first.
    assert!(index.contains(Path::new("build/a.o")));
    assert!(index.contains(Path::new("build/deep/b.o")));
    assert!(
        index.contains(Path::new("build")),
        "the subtree root itself must appear so a comparison can see it removed"
    );
    assert!(
        !index.contains(Path::new("src/x.rs")),
        "a scoped walk must not report outside its scope"
    );
}

#[test]
fn walking_a_subtree_that_is_gone_yields_nothing() {
    let dir = tree(&[("src/x.rs", "code")]);

    let index =
        treesync::reconcile::walk_subtree(dir.path(), Path::new("deleted"), &IndexOptions::quick())
            .expect("walk");

    assert!(
        index.is_empty(),
        "a rescan of a directory that has since been removed is normal, not a failure"
    );
}

#[test]
fn an_empty_prefix_walks_the_whole_tree() {
    let dir = tree(&[("a.txt", "one"), ("sub/b.txt", "two")]);

    let scoped =
        treesync::reconcile::walk_subtree(dir.path(), Path::new(""), &IndexOptions::quick())
            .expect("walk");
    let full = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    assert_eq!(scoped, full);
}

#[test]
fn index_scope_dispatches_on_the_scope() {
    let dir = tree(&[("a.txt", "one"), ("build/b.o", "two")]);

    let named = treesync::reconcile::index_scope(
        dir.path(),
        &Scope::Paths(vec![PathBuf::from("a.txt")]),
        &IndexOptions::quick(),
    )
    .expect("index");
    assert_eq!(named.len(), 1);

    let subtree = treesync::reconcile::index_scope(
        dir.path(),
        &Scope::Subtree(PathBuf::from("build")),
        &IndexOptions::quick(),
    )
    .expect("index");
    assert!(subtree.contains(Path::new("build/b.o")));
    assert!(!subtree.contains(Path::new("a.txt")));
}

// --- content hashing -------------------------------------------------------

fn checksum() -> IndexOptions {
    IndexOptions {
        filter: Default::default(),
        verify: treesync::reconcile::Verify::Checksum,
    }
}

fn deleting_with(verify: treesync::reconcile::Verify) -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        verify,
        preserve: treesync::reconcile::Preserve {
            mode: false,
            ownership: false,
        },
    }
}

/// Forces two files to share a timestamp, the way `cp -p` or a coarse-grained
/// filesystem would.
fn share_mtime(a: &Path, b: &Path) {
    let stamp = std::fs::metadata(a)
        .expect("metadata")
        .modified()
        .expect("mtime");
    filetime::set_file_mtime(b, filetime::FileTime::from_system_time(stamp)).expect("set mtime");
}

#[test]
fn quick_verification_misses_a_rewrite_that_preserved_size_and_mtime() {
    let source = tree(&[("a.txt", "AAAA")]);
    let target = tree(&[("a.txt", "BBBB")]);
    share_mtime(&source.path().join("a.txt"), &target.path().join("a.txt"));

    let plan = plan(
        &walk(source.path(), &IndexOptions::quick()).expect("walk"),
        &walk(target.path(), &IndexOptions::quick()).expect("walk"),
        &everything(),
        &deleting(),
    );

    // Documents the gap rather than endorsing it: same length, same timestamp,
    // different bytes. This is why Checksum exists.
    assert!(
        plan.is_empty(),
        "quick verification compares size and mtime, so it cannot see this"
    );
}

#[test]
fn checksum_verification_catches_a_rewrite_that_preserved_size_and_mtime() {
    let source = tree(&[("a.txt", "AAAA")]);
    let target = tree(&[("a.txt", "BBBB")]);
    share_mtime(&source.path().join("a.txt"), &target.path().join("a.txt"));

    let plan = plan(
        &walk(source.path(), &checksum()).expect("walk"),
        &walk(target.path(), &checksum()).expect("walk"),
        &everything(),
        &deleting_with(treesync::reconcile::Verify::Checksum),
    );

    assert_eq!(
        plan.actions,
        vec![Action::CopyFile(PathBuf::from("a.txt"))],
        "identical size and timestamp, different content: only the hash can tell"
    );
}

#[test]
fn checksum_verification_leaves_identical_files_alone() {
    let source = tree(&[("a.txt", "same bytes")]);
    let target = tree(&[("a.txt", "same bytes")]);
    share_mtime(&source.path().join("a.txt"), &target.path().join("a.txt"));

    let plan = plan(
        &walk(source.path(), &checksum()).expect("walk"),
        &walk(target.path(), &checksum()).expect("walk"),
        &everything(),
        &deleting_with(treesync::reconcile::Verify::Checksum),
    );

    assert!(
        plan.is_empty(),
        "matching content must not be re-transferred"
    );
}

#[test]
fn checksum_verification_spares_a_transfer_when_only_the_timestamp_moved() {
    let source = tree(&[("a.txt", "unchanged")]);
    let target = tree(&[("a.txt", "unchanged")]);
    // Deliberately different mtimes, identical bytes: a `touch` with no edit.
    filetime::set_file_mtime(
        source.path().join("a.txt"),
        filetime::FileTime::from_unix_time(2_000_000_000, 0),
    )
    .expect("set mtime");

    let quick = plan(
        &walk(source.path(), &IndexOptions::quick()).expect("walk"),
        &walk(target.path(), &IndexOptions::quick()).expect("walk"),
        &everything(),
        &deleting(),
    );
    assert_eq!(
        quick.len(),
        1,
        "quick verification re-copies on mtime alone"
    );

    let hashed = plan(
        &walk(source.path(), &checksum()).expect("walk"),
        &walk(target.path(), &checksum()).expect("walk"),
        &everything(),
        &deleting_with(treesync::reconcile::Verify::Checksum),
    );
    assert!(
        hashed.is_empty(),
        "the hash is authoritative both ways: it also avoids a pointless transfer"
    );
}

#[test]
fn hashes_are_absent_under_quick_verification() {
    let dir = tree(&[("a.txt", "content")]);

    let index = walk(dir.path(), &IndexOptions::quick()).expect("walk");

    match index.get(Path::new("a.txt")) {
        Some(Entry::File { hash, .. }) => assert!(
            hash.is_none(),
            "quick verification must not read file contents"
        ),
        other => panic!("expected a file, got {other:?}"),
    }
}

#[test]
fn a_hash_covers_the_whole_file_not_a_prefix() {
    // Differs only in the final byte, and only past any plausible read buffer.
    let source = tree(&[("big.bin", &("x".repeat(200_000) + "A"))]);
    let target = tree(&[("big.bin", &("x".repeat(200_000) + "B"))]);
    share_mtime(
        &source.path().join("big.bin"),
        &target.path().join("big.bin"),
    );

    let plan = plan(
        &walk(source.path(), &checksum()).expect("walk"),
        &walk(target.path(), &checksum()).expect("walk"),
        &everything(),
        &deleting_with(treesync::reconcile::Verify::Checksum),
    );

    assert_eq!(
        plan.len(),
        1,
        "a difference in the last byte must be caught"
    );
}

// --- exclusions ------------------------------------------------------------

fn excluding(patterns: &[&str]) -> IndexOptions {
    IndexOptions {
        filter: treesync::reconcile::Filter::new(
            &patterns.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        )
        .expect("patterns"),
        verify: Default::default(),
    }
}

#[test]
fn an_excluded_file_never_enters_the_index() {
    let dir = tree(&[("keep.rs", "code"), ("scratch.tmp", "junk")]);

    let index = walk(dir.path(), &excluding(&["*.tmp"])).expect("walk");

    assert!(index.contains(Path::new("keep.rs")));
    assert!(!index.contains(Path::new("scratch.tmp")));
}

#[test]
fn an_excluded_directory_is_not_descended_into() {
    let dir = tree(&[
        ("src/main.rs", "code"),
        ("node_modules/pkg/index.js", "vendored"),
        ("node_modules/pkg/deep/more.js", "vendored"),
    ]);

    let index = walk(dir.path(), &excluding(&["node_modules/"])).expect("walk");

    assert!(index.contains(Path::new("src/main.rs")));
    assert!(!index.contains(Path::new("node_modules")));
    assert!(
        index.paths().all(|path| !path.starts_with("node_modules")),
        "pruning must skip the subtree, not walk it and discard entries"
    );
}

#[test]
fn exclusions_apply_to_named_paths_too() {
    let dir = tree(&[("a.txt", "one"), ("b.tmp", "two")]);

    let index = treesync::reconcile::stat_paths(
        dir.path(),
        &[PathBuf::from("a.txt"), PathBuf::from("b.tmp")],
        &excluding(&["*.tmp"]),
    )
    .expect("stat");

    assert_eq!(index.len(), 1);
    assert!(!index.contains(Path::new("b.tmp")));
}

#[test]
fn an_excluded_file_on_the_target_is_not_deleted() {
    let source = tree(&[("keep.rs", "code")]);
    let target = tree(&[("keep.rs", "code"), ("local.tmp", "target-only")]);
    share_mtime(
        &source.path().join("keep.rs"),
        &target.path().join("keep.rs"),
    );

    let options = excluding(&["*.tmp"]);
    let plan = plan(
        &walk(source.path(), &options).expect("walk"),
        &walk(target.path(), &options).expect("walk"),
        &everything(),
        &deleting(),
    );

    // The failure this guards: filtering only the source would make every
    // excluded file on the target look like something the source deleted, and
    // deletion would remove exactly the files the operator protected.
    assert!(
        plan.is_empty(),
        "an excluded target file must be left alone, got {:?}",
        plan.actions
    );
}

#[test]
fn an_excluded_subtree_on_the_target_is_not_deleted() {
    let source = tree(&[("src/main.rs", "code")]);
    let target = tree(&[("src/main.rs", "code"), ("target/debug/app", "binary")]);
    share_mtime(
        &source.path().join("src/main.rs"),
        &target.path().join("src/main.rs"),
    );

    let options = excluding(&["target/"]);
    let plan = plan(
        &walk(source.path(), &options).expect("walk"),
        &walk(target.path(), &options).expect("walk"),
        &everything(),
        &deleting(),
    );

    assert!(plan.is_empty(), "got {:?}", plan.actions);
}

#[test]
fn exclusions_and_checksums_compose() {
    let source = tree(&[("a.txt", "AAAA"), ("b.tmp", "AAAA")]);
    let target = tree(&[("a.txt", "BBBB"), ("b.tmp", "BBBB")]);
    share_mtime(&source.path().join("a.txt"), &target.path().join("a.txt"));
    share_mtime(&source.path().join("b.tmp"), &target.path().join("b.tmp"));

    let options = IndexOptions {
        filter: treesync::reconcile::Filter::new(&["*.tmp".to_string()]).expect("patterns"),
        verify: treesync::reconcile::Verify::Checksum,
    };

    let plan = plan(
        &walk(source.path(), &options).expect("walk"),
        &walk(target.path(), &options).expect("walk"),
        &everything(),
        &deleting_with(treesync::reconcile::Verify::Checksum),
    );

    assert_eq!(
        plan.actions,
        vec![Action::CopyFile(PathBuf::from("a.txt"))],
        "the hash catches a.txt; the filter keeps b.tmp out of it entirely"
    );
}

/// The bug this guards against, traced 2026-08-12:
///
/// `mkdir -p a/b/c && echo > a/b/c/deep.txt` produced exactly one watcher
/// event, `Create a`. The kernel cannot report inside a directory it has no
/// watch on yet, and a watch can only be installed once the directory exists.
/// The batch therefore named `a` and nothing else.
///
/// Both halves of the reconciler then dropped the subtree: `stat_paths` stat'd
/// `a` alone, and `plan` compared only the literally-named paths. The target
/// got an empty `a` and `deep.txt` was never mirrored, silently, and with no
/// failed action to notice.
///
/// These are filesystem-level, not watcher-level, so they hold regardless of
/// backend timing.
#[test]
fn a_named_directory_carries_its_whole_subtree_into_the_index() {
    let source = tree(&[("a/b/c/deep.txt", "deep"), ("elsewhere.txt", "x")]);

    let index = treesync::reconcile::index_scope(
        source.path(),
        &Scope::Paths(vec![PathBuf::from("a")]),
        &IndexOptions::quick(),
    )
    .expect("index");

    for expected in ["a", "a/b", "a/b/c", "a/b/c/deep.txt"] {
        assert!(
            index.contains(Path::new(expected)),
            "naming `a` must reach {expected}; the watcher reported nothing else"
        );
    }

    assert!(
        !index.contains(Path::new("elsewhere.txt")),
        "and must not pull in what was never named"
    );
}

#[test]
fn a_named_directory_is_planned_with_its_whole_subtree() {
    let source = tree(&[("a/b/c/deep.txt", "deep")]);
    let target = tree(&[]);

    let scope = Scope::Paths(vec![PathBuf::from("a")]);
    let options = IndexOptions::quick();

    let plan = plan(
        &treesync::reconcile::index_scope(source.path(), &scope, &options).expect("source"),
        &treesync::reconcile::index_scope(target.path(), &scope, &options).expect("target"),
        &scope,
        &deleting(),
    );

    assert!(
        plan.actions
            .contains(&Action::CopyFile(PathBuf::from("a/b/c/deep.txt"))),
        "the file has to be planned, not just its ancestors; got {:?}",
        plan.actions
    );
    for dir in ["a", "a/b", "a/b/c"] {
        assert!(
            plan.actions
                .contains(&Action::CreateDir(PathBuf::from(dir))),
            "{dir} has to be created before the file lands in it; got {:?}",
            plan.actions
        );
    }
}
