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
    // JWT tokens are base64url-encoded (printable ASCII: [A-Za-z0-9_\-\.]+)
    // and all cookie attribute strings are static ASCII — from_str cannot fail.
    HeaderValue::from_str(&s).expect("JWT and cookie attributes are printable ASCII")
}

/// Header value that clears the session cookie — used by `/admin/logout`.
pub fn clear_session_cookie(secure: bool) -> HeaderValue {
    let mut s = format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0");
    if secure {
        s.push_str("; Secure");
    }
    // Fully static ASCII — from_str cannot fail.
    HeaderValue::from_str(&s).expect("cookie clear string is static ASCII")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_session_cookie_sets_expected_attributes() {
        let v = build_session_cookie("hdr.payload.sig", 3600, false);
        let s = v.to_str().unwrap();
        assert!(
            s.starts_with("tiled_session=hdr.payload.sig;"),
            "cookie value"
        );
        assert!(s.contains("HttpOnly"), "HttpOnly attribute");
        assert!(s.contains("SameSite=Lax"), "SameSite=Lax attribute");
        assert!(s.contains("Max-Age=3600"), "Max-Age attribute");
        assert!(!s.contains("Secure"), "no Secure when secure=false");
    }

    #[test]
    fn build_session_cookie_adds_secure_flag() {
        let v = build_session_cookie("h.p.s", 60, true);
        assert!(v.to_str().unwrap().contains("Secure"));
    }

    #[test]
    fn clear_session_cookie_zeroes_max_age() {
        let v = clear_session_cookie(false);
        let s = v.to_str().unwrap();
        assert!(s.starts_with("tiled_session=;"), "empty value");
        assert!(s.contains("Max-Age=0"), "Max-Age=0 to expire cookie");
    }
}
