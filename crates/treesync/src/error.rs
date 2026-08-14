use std::io;

use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by treesync itself.
///
/// Variants are the categories a caller can act on, not a mirror of every
/// underlying failure. Anything a caller cannot branch on belongs in
/// [`Error::Internal`] or [`Error::Io`].
#[derive(Debug, ThisError)]
pub enum Error {
    /// A path that was expected to exist does not.
    ///
    /// Routine during a sync: files disappear between listing a directory and
    /// acting on an entry. Treat it as an expected outcome, not a failure.
    #[error("not found: {0}")]
    NotFound(String),

    /// The process lacks the rights to read, write, or traverse a path.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// A path that was expected to be absent already exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// A path is unusable: non-UTF-8, escapes the sync root, or otherwise fails
    /// validation before any syscall is attempted.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// Startup configuration is missing or malformed.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// An I/O failure with no more specific category. The source is preserved
    /// so the raw `io::ErrorKind` stays reachable.
    #[error("io error: {0}")]
    Io(#[source] io::Error),

    /// The configuration is valid but asks for something treesync cannot do yet.
    ///
    /// Distinct from [`Error::Config`]: the file is not wrong, the feature is
    /// missing. Says so plainly rather than failing as if the operator erred.
    #[error("not supported yet: {0}")]
    Unsupported(String),

    /// An invariant was violated. Reaching this is a bug in treesync.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Maps the `io::ErrorKind`s that callers branch on into their own variants and
/// keeps the rest as [`Error::Io`], so a caller can match on `NotFound` without
/// reaching through to the source.
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::NotFound => Error::NotFound(error.to_string()),
            io::ErrorKind::PermissionDenied => Error::PermissionDenied(error.to_string()),
            io::ErrorKind::AlreadyExists => Error::AlreadyExists(error.to_string()),
            _ => Error::Io(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_not_found_maps_to_not_found() {
        let err = Error::from(io::Error::new(io::ErrorKind::NotFound, "no such file"));

        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn io_permission_denied_maps_to_permission_denied() {
        let err = Error::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));

        assert!(matches!(err, Error::PermissionDenied(_)));
    }

    #[test]
    fn io_already_exists_maps_to_already_exists() {
        let err = Error::from(io::Error::new(io::ErrorKind::AlreadyExists, "exists"));

        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[test]
    fn unmapped_io_kind_stays_io_and_keeps_its_kind() {
        let err = Error::from(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));

        match err {
            Error::Io(source) => assert_eq!(source.kind(), io::ErrorKind::BrokenPipe),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn display_is_prefixed_by_category() {
        let err = Error::NotFound("/tmp/gone".to_string());

        assert_eq!(err.to_string(), "not found: /tmp/gone");
    }
}
