//! Cross-platform `file://` URI ⇄ filesystem path conversion.
//!
//! A managed asset's `data_uri` is a `file://` URI. Building it from
//! `Path::display()` (`format!("file://{}", path.display())`) is only correct on
//! Unix: there an absolute path is `/a/b`, so the result `file:///a/b` is a valid
//! URI. On Windows the same path is `C:\a\b`, so the result is `file://C:\a\b` —
//! backslashes instead of `/`, and a missing leading slash before the drive
//! letter — a malformed URI that the read/delete resolvers cannot parse back,
//! breaking the write→read round-trip.
//!
//! These helpers wrap the [`url`] crate's platform-aware
//! [`Url::from_file_path`]/[`Url::to_file_path`], which emit/accept the canonical
//! form on every platform (`file:///a/b` on Unix, `file:///C:/a/b` on Windows) —
//! matching Python `pathlib.Path.as_uri()`, the shape Python-tiled stores.

use std::path::{Path, PathBuf};
use url::Url;

/// Build a `file://` URI for an absolute filesystem `path`.
///
/// - Unix: `/a/b` → `file:///a/b`
/// - Windows: `C:\a\b` → `file:///C:/a/b`
///
/// Returns `None` if `path` is not absolute (the contract of
/// [`Url::from_file_path`]); callers building under an absolute storage root
/// already guarantee absoluteness.
pub fn path_to_file_uri(path: &Path) -> Option<String> {
    Url::from_file_path(path).ok().map(String::from)
}

/// Parse a `file://` URI back into a filesystem path, cross-platform.
///
/// Accepts the empty and `localhost` authorities (`file:///a/b` and
/// `file://localhost/a/b` both decode to `/a/b`, matching Python's `urlparse`).
/// Returns `None` for anything that is not a `file` URL: other schemes, a
/// scheme-less bare absolute path (which must not bypass the `file://` check),
/// or a `file://` with no path.
pub fn file_uri_to_path(uri: &str) -> Option<PathBuf> {
    let url = Url::parse(uri).ok()?;
    if url.scheme() != "file" {
        return None;
    }
    // Reject any non-empty authority. The `url` crate already normalizes the
    // `localhost` host away (`file://localhost/a/b` == `file:///a/b`, matching
    // Python's `urlparse`), so a *remaining* host is a real remote/UNC
    // authority — never a local managed asset, on either platform. Guarding
    // here is also what keeps the helper robust on Windows: `to_file_path()`'s
    // Windows branch turns a host into a `\\host` UNC path that is not
    // absolute, tripping its internal `debug_assert!(path.is_absolute())`
    // (panic in debug builds) or returning a bogus non-absolute path (release).
    // Refusing the host before that call removes the dual behavior structurally.
    if url.host_str().is_some() {
        return None;
    }
    let path = url.to_file_path().ok()?;
    // Reject a root-only path. The `url` crate normalizes `file://` (no path) to
    // `file:///`, i.e. the filesystem root; the old hand-rolled parser rejected
    // `file://` outright. A `data_uri` pointing at the root is never a real
    // managed asset and is dangerous for the delete path, so refuse it on both
    // platforms (`/` and `C:\` have no parent, so `parent()?` returns `None`).
    path.parent()?;
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_an_absolute_path() {
        // Build from a real absolute path on the host and parse it straight back.
        #[cfg(unix)]
        let p = PathBuf::from("/data/scan001/frame.h5");
        #[cfg(windows)]
        let p = PathBuf::from(r"C:\data\scan001\frame.h5");

        let uri = path_to_file_uri(&p).expect("absolute path");
        assert!(uri.starts_with("file:///"), "got {uri}");
        assert!(uri.ends_with("/frame.h5"), "got {uri}");
        assert_eq!(file_uri_to_path(&uri), Some(p));
    }

    #[test]
    fn non_absolute_path_yields_none() {
        assert_eq!(path_to_file_uri(Path::new("relative/x")), None);
    }

    // --- parse semantics (ported from the tiled-server uri_to_path tests so the
    //     unified helper keeps the same security-relevant behavior) ---

    #[test]
    fn accepts_empty_authority() {
        assert_eq!(
            file_uri_to_path("file:///data/scan001.h5"),
            Some(PathBuf::from("/data/scan001.h5"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn strips_host_authority() {
        // file://localhost/a/b -> /a/b, matching Python's urlparse.
        assert_eq!(
            file_uri_to_path("file://localhost/a/b"),
            Some(PathBuf::from("/a/b"))
        );
    }

    #[test]
    fn rejects_bare_absolute_path() {
        // A scheme-less absolute path must not bypass the file:// check.
        assert_eq!(file_uri_to_path("/etc/passwd"), None);
    }

    #[test]
    fn rejects_other_schemes() {
        assert_eq!(file_uri_to_path("s3://bucket/key"), None);
        assert_eq!(file_uri_to_path("http://host/p"), None);
        assert_eq!(file_uri_to_path("sqlite:///db.sqlite"), None);
    }

    #[test]
    fn rejects_malformed_file_uri() {
        assert_eq!(file_uri_to_path("file://"), None);
        assert_eq!(file_uri_to_path("file://relative-no-slash"), None);
    }
}
