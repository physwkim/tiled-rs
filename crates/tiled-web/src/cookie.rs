//! Session cookie helpers.
//!
//! The server admin pages bridge browser sessions to the same JWT the
//! API middleware understands. We set `tiled_session=<jwt>; HttpOnly;
//! SameSite=Lax` on login, and the auth middleware reads the cookie as
//! an alternative source for the Bearer token.

use axum::http::header::HeaderValue;

pub const SESSION_COOKIE: &str = "tiled_session";

/// Build the Set-Cookie header value for a fresh session. `max_age` in
/// seconds. `secure=false` allows plain HTTP for development; production
/// deployments behind TLS should pass true.
pub fn build_session_cookie(jwt: &str, max_age: i64, secure: bool) -> HeaderValue {
    let mut s =
        format!("{SESSION_COOKIE}={jwt}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}");
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// Header value that clears the session cookie — used by `/admin/logout`.
pub fn clear_session_cookie(secure: bool) -> HeaderValue {
    let mut s = format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0");
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// Read the session cookie from a Cookie request header. Returns the
/// JWT or None.
pub fn read_session_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    let raw = headers.get("cookie")?.to_str().ok()?;
    for piece in raw.split(';') {
        let trimmed = piece.trim();
        if let Some(value) = trimmed.strip_prefix(&format!("{SESSION_COOKIE}=")) {
            return Some(value.to_string());
        }
    }
    None
}
