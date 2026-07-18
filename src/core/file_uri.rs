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

/// Map any asset `data_uri` to its backing local filesystem path.
///
/// Mirrors Python tiled's `path_from_uri` (`tiled/utils.py:745`), the resolver
/// the client's `get_asset_filepaths` uses: the `file` scheme decodes
/// cross-platform (via [`file_uri_to_path`]); the `sqlite` and `duckdb` schemes
/// carry the database file path directly after the scheme. Any other scheme
/// (`s3://`, `http://`, …) or a scheme-less bare path yields `None` — upstream
/// raises there, and the caller turns `None` into an error.
///
/// Broader than [`file_uri_to_path`], which is file-only because its callers are
/// the server's read/delete resolvers, where a `sqlite://` asset must not be
/// treated as a plain file path. This helper is for the read-only client path,
/// which only reports where the data lives.
///
/// Deviation from upstream on the SQL schemes: this keeps the full path the Rust
/// server emits (`sqlite://{absolute}`, matching
/// [`crate::server::file_resolver`]'s `sqlite_uri_to_path`), rather than
/// upstream's `parsed.path[1:]`, which strips the leading slash. The Rust
/// convention is what round-trips with the URIs this server actually stores.
pub fn path_from_uri(uri: &str) -> Option<PathBuf> {
    if let Some(path) = file_uri_to_path(uri) {
        return Some(path);
    }
    for scheme in ["sqlite", "duckdb"] {
        if let Some(rest) = uri.strip_prefix(scheme).and_then(|r| r.strip_prefix("://")) {
            // Drop any `?query` suffix sqlx/duckdb would accept on the URI.
            let path_part = rest.split('?').next().unwrap_or(rest);
            if path_part.is_empty() {
                return None;
            }
            return Some(PathBuf::from(path_part));
        }
    }
    None
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

    // An empty authority decodes to the host's absolute path. The path string
    // is platform-specific: a drive-less `/data/...` is a valid absolute path on
    // Unix but not on Windows (no drive letter), so `to_file_path()` only
    // accepts the drive-letter form there.
    #[cfg(unix)]
    #[test]
    fn accepts_empty_authority() {
        assert_eq!(
            file_uri_to_path("file:///data/scan001.h5"),
            Some(PathBuf::from("/data/scan001.h5"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn accepts_empty_authority() {
        assert_eq!(
            file_uri_to_path("file:///C:/data/scan001.h5"),
            Some(PathBuf::from(r"C:\data\scan001.h5"))
        );
    }

    // file://localhost/... -> the same path as file:///... (url strips the
    // localhost host), matching Python's urlparse. The decoded path is
    // platform-specific for the same reason as `accepts_empty_authority`.
    #[cfg(unix)]
    #[test]
    fn strips_host_authority() {
        assert_eq!(
            file_uri_to_path("file://localhost/a/b"),
            Some(PathBuf::from("/a/b"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn strips_host_authority() {
        assert_eq!(
            file_uri_to_path("file://localhost/C:/a/b"),
            Some(PathBuf::from(r"C:\a\b"))
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

    // --- path_from_uri: the broader client-side resolver (file + sqlite +
    //     duckdb), mirroring upstream tiled/utils.py:path_from_uri ---

    #[cfg(unix)]
    #[test]
    fn path_from_uri_decodes_file_scheme() {
        assert_eq!(
            path_from_uri("file:///data/scan001.h5"),
            Some(PathBuf::from("/data/scan001.h5"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn path_from_uri_decodes_file_scheme() {
        assert_eq!(
            path_from_uri("file:///C:/data/scan001.h5"),
            Some(PathBuf::from(r"C:\data\scan001.h5"))
        );
    }

    #[test]
    fn path_from_uri_decodes_sqlite_and_duckdb() {
        // Matches the `sqlite://{absolute}` form the Rust server emits for
        // managed ragged-SQL assets (file_resolver::sqlite_uri_to_path).
        assert_eq!(
            path_from_uri("sqlite:///srv/data/ragged.db"),
            Some(PathBuf::from("/srv/data/ragged.db"))
        );
        assert_eq!(
            path_from_uri("duckdb:///srv/data/tbl.duckdb"),
            Some(PathBuf::from("/srv/data/tbl.duckdb"))
        );
    }

    #[test]
    fn path_from_uri_strips_query_suffix() {
        assert_eq!(
            path_from_uri("sqlite:///srv/data/ragged.db?mode=ro"),
            Some(PathBuf::from("/srv/data/ragged.db"))
        );
    }

    #[test]
    fn path_from_uri_rejects_unsupported_and_empty() {
        assert_eq!(path_from_uri("s3://bucket/key"), None);
        assert_eq!(path_from_uri("http://host/p"), None);
        assert_eq!(path_from_uri("/etc/passwd"), None);
        assert_eq!(path_from_uri("sqlite://"), None);
    }
}
