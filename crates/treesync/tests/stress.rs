//! Load and scale tests.
//!
//! Ignored by default. These build trees of a hundred thousand files or more,
//! which costs minutes and gigabytes, and `cargo test --workspace` is meant to
//! stay quick enough that people run it. Ask for them:
//!
//! ```bash
//! cargo test -p treesync --test stress --release -- --ignored --nocapture
//! ```
//!
//! `--release` is not optional in spirit. A debug build spends its time in
//! `walkdir` and `HashMap`, so a debug measurement describes rustc rather than
//! treesync.
//!
//! `TREESYNC_STRESS_FILES` sets the file count. The default is 100_000, which
//! is large enough for the shape of the curve to show and small enough to run
//! while you wait. The number the design gets argued about is a million:
//!
//! ```bash
//! TREESYNC_STRESS_FILES=1000000 \
//!   cargo test -p treesync --test stress --release -- --ignored --nocapture
//! ```
//!
//! At a million files expect roughly 4 GB per tree, and there are two trees.
//! A temporary directory is only cleaned up when the test ends, so a run killed
//! part way through leaves both behind.
//!
//! # What is asserted, and what is only printed
//!
//! Two properties, neither of them a duration:
//!
//! - **The mirror converges.** After every file in the tree has changed, one
//!   pass makes the target match, and the pass after it plans nothing. A
//!   pipeline that re-copies an unchanged tree forever passes every small test
//!   in isolation and is useless at this size.
//! - **An incremental pass costs the change, not the tree.** One file changing
//!   in a million-file tree stats one file and plans one action. That is the
//!   claim treesync is built on, and the only place it can be checked is a tree
//!   big enough for the difference to be real.
//!
//! Times and memory are printed, never asserted. A wall clock on a machine
//! doing other things is not a property, and a test that fails when a laptop is
//! busy trains people to ignore it.
//!
//! # Why the watcher is not driven at this size
//!
//! A million filesystem events is a test of the kernel's queue, not of this
//! code. Both backends drop events under that load, which treesync already
//! answers by re-walking the affected subtree, and that answer is asserted at a
//! size where it can be provoked reliably in `watcher_queue.rs`. What is worth
//! knowing at a million files is what a pass over one costs, which is what
//! these measure.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use treesync::reconcile::{
    IndexOptions, Preserve, ReconcileConfig, Scope, index_scope, plan, walk,
};
use treesync::sink::{LocalSink, apply};

/// Files per directory.
///
/// A million entries in one directory measures the filesystem's directory
/// implementation, not this one. Real trees fan out, so this one does too.
const FANOUT: usize = 1_000;

const DEFAULT_FILES: usize = 100_000;

fn file_count() -> usize {
    match std::env::var("TREESYNC_STRESS_FILES") {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("TREESYNC_STRESS_FILES must be a number, got {value:?}")),
        Err(_) => DEFAULT_FILES,
    }
}

fn shard_count(files: usize) -> usize {
    files.div_ceil(FANOUT)
}

fn relative_path(index: usize) -> PathBuf {
    PathBuf::from(format!("shard-{:05}/file-{:08}", index / FANOUT, index))
}

/// Deletions on, metadata off.
///
/// Preservation is covered by its own tests. Leaving it on here would add a
/// `SetMetadata` per directory to every plan and blur the counts this asserts.
fn config() -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        preserve: Preserve {
            mode: false,
            ownership: false,
        },
        ..Default::default()
    }
}

/// Resident memory of this process, in megabytes.
///
/// Read from `ps` rather than through a crate, because a load test that needs a
/// dependency to report a number is a dependency the library then carries.
/// Sampled while both indexes are alive, which is when a mirror of this size
/// holds the most.
fn resident_mb() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();

    match output {
        Ok(output) => String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .map(|kb| kb / 1024)
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Free space on the filesystem holding `path`, in bytes.
///
/// `-P` asks for the POSIX output format, which keeps a long device name on one
/// line instead of wrapping it and shifting every column along.
fn free_bytes(path: &Path) -> Option<u64> {
    let output = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .ok()?;

    let text = String::from_utf8_lossy(&output.stdout);
    let available: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;

    Some(available * 1024)
}

/// Whether there is room for the trees this run would build.
///
/// Both trees are written in full before anything is asserted, so running out
/// of disk half way through costs the entire run and leaves a failure that
/// reads like a treesync bug. Refusing up front is the difference between a
/// clear skip and a filled disk on a laptop or a runner.
fn enough_space(path: &Path, count: usize, test: &str) -> bool {
    // A file of a few bytes still occupies a whole block, and there are two
    // trees, so this is the floor rather than an estimate of the content.
    const BYTES_PER_FILE: u64 = 4096;

    let needed = BYTES_PER_FILE * count as u64 * 2;

    match free_bytes(path) {
        Some(free) if free < needed => {
            eprintln!(
                "SKIPPED {test}: {count} files needs about {} MB across both trees, \
                 and {} MB is free. Lower TREESYNC_STRESS_FILES, or free some space.",
                needed / (1024 * 1024),
                free / (1024 * 1024),
            );
            false
        }
        _ => true,
    }
}

/// Writes `count` small files under `root`, in parallel.
///
/// Creating a million files one syscall at a time is dominated by the
/// filesystem, not by anything being tested, and it is the part of the run
/// people give up waiting on. Parallel writing keeps the setup from being the
/// measurement.
fn populate<F>(root: &Path, count: usize, contents: F)
where
    F: Fn(usize) -> String + Sync,
{
    // Created once, up front. Left to the writers, every thread would race to
    // create the same parent directory for its first file.
    for shard in 0..shard_count(count) {
        std::fs::create_dir_all(root.join(format!("shard-{shard:05}"))).expect("create shard");
    }

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let chunk = count.div_ceil(threads);

    std::thread::scope(|scope| {
        for thread in 0..threads {
            let contents = &contents;

            scope.spawn(move || {
                let start = thread * chunk;
                let end = ((thread + 1) * chunk).min(count);

                for index in start..end {
                    std::fs::write(root.join(relative_path(index)), contents(index))
                        .expect("write file");
                }
            });
        }
    });
}

/// One whole-tree pass, returning the actions applied and how long it took.
async fn full_pass(source: &Path, target: &Path, report_memory: bool) -> (usize, Duration) {
    let config = config();
    let scope = Scope::Subtree(PathBuf::new());
    let started = Instant::now();

    let source_index = walk(source, &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target, &IndexOptions::quick()).expect("walk target");

    if report_memory {
        println!(
            "    indexes: {} source + {} target entries, {} MB resident",
            source_index.len(),
            target_index.len(),
            resident_mb()
        );
    }

    let plan = plan(&source_index, &target_index, &scope, &config);

    let sink = LocalSink::new(target).expect("sink");
    let report = apply(&plan, source, &sink, config.preserve).await;

    // Truncated: a plan of this size that goes wrong goes wrong in bulk, and a
    // million failure lines buries the one detail that explains them.
    assert!(
        report.is_complete(),
        "{} action(s) failed, first few: {:?}",
        report.failures.len(),
        &report.failures[..report.failures.len().min(3)]
    );

    (report.applied, started.elapsed())
}

fn rate(count: usize, elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();

    if seconds <= 0.0 {
        return "n/a".to_string();
    }

    format!("{:.0} files/s", count as f64 / seconds)
}

/// Every file in a large tree changes at once, and the mirror catches up.
///
/// This is the load case: not a big file, but a great many of them, all
/// different, in one pass.
#[tokio::test]
#[ignore = "builds a tree of 100k files or more; run with --ignored"]
async fn a_whole_tree_changing_converges() {
    let count = file_count();
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");

    if !enough_space(source.path(), count, "a_whole_tree_changing_converges") {
        return;
    }

    println!("\n{count} files, {} directories", shard_count(count));

    let started = Instant::now();
    populate(source.path(), count, |index| format!("v1 {index}"));
    println!(
        "  created  {count} files in {:?} ({})",
        started.elapsed(),
        rate(count, started.elapsed())
    );

    let (applied, elapsed) = full_pass(source.path(), target.path(), true).await;
    println!(
        "  cold     {applied} actions in {elapsed:?} ({})",
        rate(count, elapsed)
    );
    assert_eq!(
        applied,
        count + shard_count(count),
        "the first pass copies every file and creates every directory"
    );

    // The load itself. Every file gets different content and a different
    // length, so the quick check sees all of them.
    let started = Instant::now();
    populate(source.path(), count, |index| {
        format!("v2 {index} rewritten wholesale")
    });
    println!("  rewrote  {count} files in {:?}", started.elapsed());

    let (applied, elapsed) = full_pass(source.path(), target.path(), true).await;
    println!(
        "  changed  {applied} actions in {elapsed:?} ({})",
        rate(count, elapsed)
    );
    assert_eq!(
        applied, count,
        "every changed file is copied, and nothing that did not change"
    );

    // The assertion a mirror that re-copies itself forever fails.
    let (applied, elapsed) = full_pass(source.path(), target.path(), false).await;
    println!("  settled  {applied} actions in {elapsed:?}");
    assert_eq!(applied, 0, "a converged mirror has nothing left to plan");

    let sample = relative_path(count / 2);
    assert_eq!(
        std::fs::read_to_string(target.path().join(&sample)).expect("read target file"),
        format!("v2 {} rewritten wholesale", count / 2),
        "the target holds the new content, not the old"
    );
}

/// One file changes in a very large tree, and the pass costs one file.
///
/// The whole design rests on this. rsync rebuilds its file list every
/// invocation, so its cost tracks the tree; a batch here names the paths that
/// changed and the reconciler stats exactly those. At ten files the difference
/// is noise, which is why it is asserted at this size instead.
#[tokio::test]
#[ignore = "builds a tree of 100k files or more; run with --ignored"]
async fn one_change_in_a_large_tree_costs_one_file() {
    let count = file_count();
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");

    if !enough_space(
        source.path(),
        count,
        "one_change_in_a_large_tree_costs_one_file",
    ) {
        return;
    }

    println!("\n{count} files, {} directories", shard_count(count));

    populate(source.path(), count, |index| format!("v1 {index}"));

    let (_, cold) = full_pass(source.path(), target.path(), false).await;
    println!("  cold     whole tree in {cold:?}");

    // How long the whole-tree comparison takes with nothing to do. This is what
    // an incremental pass is being compared against, and it is also what a tool
    // that rebuilds its file list pays on every invocation.
    let started = Instant::now();
    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk target");
    let whole_tree = started.elapsed();
    assert_eq!(source_index.len(), target_index.len());
    drop((source_index, target_index));
    println!("  walk     both trees in {whole_tree:?}");

    // One file, in the middle, so nothing about its position helps.
    let changed = relative_path(count / 2);
    std::fs::write(
        source.path().join(&changed),
        "changed, and the only thing that did",
    )
    .expect("write the one changed file");

    let scope = Scope::Paths(vec![changed.clone()]);

    let started = Instant::now();
    let source_index = index_scope(source.path(), &scope, &IndexOptions::quick()).expect("source");
    let target_index = index_scope(target.path(), &scope, &IndexOptions::quick()).expect("target");
    let incremental_plan = plan(&source_index, &target_index, &scope, &config());
    let incremental = started.elapsed();

    println!(
        "  batch    1 of {count} files in {incremental:?}, {} times faster than the walk",
        (whole_tree.as_secs_f64() / incremental.as_secs_f64().max(f64::EPSILON)) as u64
    );

    // The property, stated as counts rather than as a duration: the batch named
    // one path, so one path was looked at, whatever the tree around it holds.
    assert_eq!(
        source_index.len(),
        1,
        "a batch naming one path must stat one path"
    );
    assert_eq!(
        incremental_plan.len(),
        1,
        "one changed file is one action, not a re-plan of the tree"
    );

    let sink = LocalSink::new(target.path()).expect("sink");
    let report = apply(&incremental_plan, source.path(), &sink, config().preserve).await;
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    assert_eq!(
        std::fs::read_to_string(target.path().join(&changed)).expect("read target file"),
        "changed, and the only thing that did"
    );

    // And the incremental pass left the rest of the tree alone, which a plan
    // count cannot show on its own.
    let (applied, _) = full_pass(source.path(), target.path(), false).await;
    assert_eq!(
        applied, 0,
        "after the one action the whole tree is already in sync"
    );
}
