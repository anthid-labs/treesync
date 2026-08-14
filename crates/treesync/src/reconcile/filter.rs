use std::path::Path;

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::error::{Error, Result};

/// Decides which paths take part in a sync.
///
/// Applied to **both** trees. Excluding a pattern from only the source would
/// make every matching file on the target look like something the source had
/// deleted, and a sync with deletions enabled would remove exactly the files
/// the operator asked it to leave alone.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// The patterns this was built from, kept so the filter can be rebuilt
    /// somewhere else.
    ///
    /// A remote agent indexes the target tree in its own process, and the
    /// compiled [`GlobSet`] cannot be sent to it. Sending the patterns and
    /// recompiling there is what guarantees both sides exclude the same paths
    /// Filtering only one tree turns every excluded file on the target into
    /// an apparent deletion.
    patterns: Vec<String>,
    set: Option<GlobSet>,
}

impl Filter {
    /// A filter that excludes nothing.
    pub fn allow_all() -> Self {
        Self {
            patterns: Vec::new(),
            set: None,
        }
    }

    /// Builds a filter from configured patterns.
    ///
    /// Each pattern is expanded so the obvious spellings do what an operator
    /// expects:
    ///
    /// - `*.tmp` matches `a.tmp` and `sub/a.tmp`. A bare pattern with no
    ///   separator matches at any depth, the way `.gitignore` behaves.
    /// - `node_modules/` matches the directory and everything beneath it. The
    ///   trailing slash is optional; `node_modules` does the same.
    /// - `build/*.o` has a separator, so it is anchored at the sync root and
    ///   does not match `sub/build/x.o`.
    pub fn new(patterns: &[String]) -> Result<Self> {
        if patterns.is_empty() {
            return Ok(Self::allow_all());
        }

        let mut builder = GlobSetBuilder::new();

        for pattern in patterns {
            let trimmed = pattern.trim().trim_end_matches('/');

            if trimmed.is_empty() {
                return Err(Error::Config(format!(
                    "exclude pattern {pattern:?} is empty"
                )));
            }

            let anchored = trimmed.contains('/');

            let mut add =
                |glob: String| -> Result<()> {
                    builder.add(Glob::new(&glob).map_err(|err| {
                        Error::Config(format!("exclude pattern {pattern:?}: {err}"))
                    })?);

                    Ok(())
                };

            // The path itself, and everything under it if it is a directory.
            add(trimmed.to_string())?;
            add(format!("{trimmed}/**"))?;

            if !anchored {
                // No separator, so it applies at every depth.
                add(format!("**/{trimmed}"))?;
                add(format!("**/{trimmed}/**"))?;
            }
        }

        let set = builder
            .build()
            .map_err(|err| Error::Config(format!("building exclude patterns: {err}")))?;

        Ok(Self {
            patterns: patterns.to_vec(),
            set: Some(set),
        })
    }

    /// The patterns this filter was built from.
    ///
    /// For rebuilding an identical filter in another process; see the field.
    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    /// Whether `relative` is excluded from the sync.
    pub fn excludes(&self, relative: &Path) -> bool {
        match &self.set {
            None => false,
            Some(set) => set.is_match(relative),
        }
    }

    /// Whether anything is filtered at all, for skipping work when nothing is.
    pub fn is_empty(&self) -> bool {
        self.set.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn filter(patterns: &[&str]) -> Filter {
        Filter::new(
            &patterns
                .iter()
                .map(|pattern| pattern.to_string())
                .collect::<Vec<_>>(),
        )
        .expect("patterns should compile")
    }

    fn excludes(filter: &Filter, path: &str) -> bool {
        filter.excludes(&PathBuf::from(path))
    }

    #[test]
    fn an_empty_pattern_list_excludes_nothing() {
        let filter = Filter::new(&[]).expect("build");

        assert!(filter.is_empty());
        assert!(!excludes(&filter, "anything/at/all.txt"));
    }

    #[test]
    fn a_suffix_pattern_matches_at_any_depth() {
        let filter = filter(&["*.tmp"]);

        assert!(excludes(&filter, "a.tmp"));
        assert!(
            excludes(&filter, "deep/nested/a.tmp"),
            "an operator writing *.tmp means everywhere, not just the root"
        );
        assert!(!excludes(&filter, "a.txt"));
        assert!(!excludes(&filter, "tmp/keep.txt"));
    }

    #[test]
    fn a_directory_pattern_matches_the_directory_and_its_contents() {
        let filter = filter(&["node_modules/"]);

        assert!(excludes(&filter, "node_modules"));
        assert!(excludes(&filter, "node_modules/left-pad/index.js"));
        assert!(excludes(&filter, "app/node_modules/x/y.js"));
        assert!(!excludes(&filter, "src/node_modules_helper.js"));
    }

    #[test]
    fn the_trailing_slash_is_optional() {
        assert_eq!(
            excludes(&filter(&[".git/"]), ".git/config"),
            excludes(&filter(&[".git"]), ".git/config")
        );
    }

    #[test]
    fn a_pattern_with_a_separator_is_anchored_at_the_root() {
        let filter = filter(&["build/*.o"]);

        assert!(excludes(&filter, "build/main.o"));
        assert!(
            !excludes(&filter, "sub/build/main.o"),
            "a rooted pattern must not float to arbitrary depths"
        );
    }

    #[test]
    fn several_patterns_all_apply() {
        let filter = filter(&["*.tmp", ".git/", "target/"]);

        assert!(excludes(&filter, "x.tmp"));
        assert!(excludes(&filter, ".git/HEAD"));
        assert!(excludes(&filter, "target/debug/binary"));
        assert!(!excludes(&filter, "src/main.rs"));
    }

    #[test]
    fn a_character_class_works() {
        let filter = filter(&["*.[oa]"]);

        assert!(excludes(&filter, "main.o"));
        assert!(excludes(&filter, "lib.a"));
        assert!(!excludes(&filter, "main.rs"));
    }

    #[test]
    fn an_empty_pattern_is_rejected() {
        let err = Filter::new(&["   ".to_string()]).expect_err("should fail");

        assert!(matches!(err, Error::Config(_)), "got {err:?}");
    }

    #[test]
    fn a_malformed_pattern_is_rejected_naming_itself() {
        let err = Filter::new(&["[unclosed".to_string()]).expect_err("should fail");

        match err {
            Error::Config(message) => assert!(
                message.contains("[unclosed"),
                "the error must name the pattern: {message}"
            ),
            other => panic!("got {other:?}"),
        }
    }
}
