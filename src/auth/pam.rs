//! PAM authenticator — validates `(username, password)` against the host's
//! Pluggable Authentication Modules stack.
//!
//! Port of Python tiled's `PAMAuthenticator` (`authenticators.py`), which is a
//! thin wrapper over `pamela.authenticate(username, password, service=...)`.
//! On a successful PAM transaction it yields `Subject { provider, sub: username }`
//! (Python's `UserSessionState(username, {})`); any PAM error is a login
//! rejection.
//!
//! Gated on the `pam` feature. Rather than depend on the Linux-PAM–specific
//! `pam` crate (which does not compile against macOS/BSD OpenPAM), this binds a
//! minimal, portable slice of the libpam application API directly:
//! `pam_start` / `pam_authenticate` / `pam_end` plus a conversation callback.
//! Those three calls and the message styles used here (`PAM_PROMPT_ECHO_OFF` /
//! `PAM_PROMPT_ECHO_ON`, `PAM_SUCCESS`) are part of the X/SSO PAM standard and
//! are identical across Linux-PAM and OpenPAM, so the same code builds and runs
//! on both. The PAM C API is blocking, so the transaction runs on a blocking
//! thread.
//!
//! Parity note: `pamela.authenticate` also issues `pam_setcred(REINITIALIZE)`
//! after a successful `pam_authenticate`. That step only re-establishes
//! credentials (e.g. Kerberos tickets) for credential-granting modules and is
//! not needed for a yes/no login check, so it is intentionally omitted to keep
//! the FFI surface to the three portable calls.

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use async_trait::async_trait;
use serde::Deserialize;

use crate::auth::authenticator::{Authenticator, Subject};
use crate::auth::error::{AuthError, Result};

/// `args` for a PAM provider. Mirrors the constructor kwargs of Python's
/// `PAMAuthenticator(service="login", confirmation_message="")`.
#[derive(Debug, Clone, Deserialize)]
pub struct PamConfig {
    /// PAM service name — the policy file under `/etc/pam.d/`. Python default
    /// is `"login"`.
    #[serde(default = "default_service")]
    pub service: String,
    /// May be displayed by the client after a successful login. Carried for
    /// parity; it does not affect authentication.
    #[serde(default)]
    pub confirmation_message: String,
}

fn default_service() -> String {
    "login".to_string()
}

impl Default for PamConfig {
    fn default() -> Self {
        Self {
            service: default_service(),
            confirmation_message: String::new(),
        }
    }
}

/// Validates credentials against a PAM service.
pub struct PamAuthenticator {
    /// Provider name — the `/auth/{name}/login` mount point and the resulting
    /// principal `provider`.
    name: String,
    /// PAM service name (Python default `"login"`).
    service: String,
    /// Surfaced to clients by the About endpoint in Python; parity-only.
    confirmation_message: String,
}

impl PamAuthenticator {
    /// Construct from a provider name and parsed [`PamConfig`].
    pub fn from_config(provider_name: impl Into<String>, config: PamConfig) -> Self {
        Self {
            name: provider_name.into(),
            service: config.service,
            confirmation_message: config.confirmation_message,
        }
    }

    /// PAM service name this authenticator validates against.
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Parity-only confirmation message.
    pub fn confirmation_message(&self) -> &str {
        &self.confirmation_message
    }
}

#[async_trait]
impl Authenticator for PamAuthenticator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn authenticate(&self, username: &str, secret: &str) -> Result<Subject> {
        // Reject blank credentials before opening a PAM transaction: some
        // `pam_unix` stacks treat an empty password as a prompt rather than an
        // outright failure, so guarding here keeps the decision deterministic
        // (and avoids the FFI path entirely for the empty case).
        if username.is_empty() || secret.is_empty() {
            return Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            ));
        }
        let service = self.service.clone();
        let provider = self.name.clone();
        let username = username.to_string();
        let secret = secret.to_string();
        // The PAM C calls block; keep them off the async runtime.
        tokio::task::spawn_blocking(move || -> Result<Subject> {
            pam_transaction(&service, &username, &secret)?;
            Ok(Subject {
                provider,
                sub: username,
            })
        })
        .await
        .map_err(|e| AuthError::Unauthorized(format!("pam task join failed: {e}")))?
    }
}

// --- Portable libpam FFI ---------------------------------------------------

/// `PAM_SUCCESS` — identical across Linux-PAM and OpenPAM.
const PAM_SUCCESS: c_int = 0;
/// Password prompt (no echo) — X/SSO standard value, identical across PAM impls.
const PAM_PROMPT_ECHO_OFF: c_int = 1;
/// Visible prompt (e.g. username) — X/SSO standard value.
const PAM_PROMPT_ECHO_ON: c_int = 2;
/// Conversation-failure return code. The exact non-success value differs
/// between implementations (Linux-PAM 19 vs OpenPAM 6); PAM treats any
/// non-`PAM_SUCCESS` conv return as a conversation error, so a fixed non-zero
/// sentinel is sufficient and portable.
const PAM_CONV_ERR: c_int = 19;

#[repr(C)]
struct PamMessage {
    msg_style: c_int,
    msg: *const c_char,
}

#[repr(C)]
struct PamResponse {
    resp: *mut c_char,
    resp_retcode: c_int,
}

#[repr(C)]
struct PamConv {
    conv: Option<
        unsafe extern "C" fn(
            num_msg: c_int,
            msg: *mut *const PamMessage,
            resp: *mut *mut PamResponse,
            appdata_ptr: *mut c_void,
        ) -> c_int,
    >,
    appdata_ptr: *mut c_void,
}

unsafe extern "C" {
    fn pam_start(
        service_name: *const c_char,
        user: *const c_char,
        pam_conversation: *const PamConv,
        pamh: *mut *mut c_void,
    ) -> c_int;
    fn pam_authenticate(pamh: *mut c_void, flags: c_int) -> c_int;
    fn pam_end(pamh: *mut c_void, pam_status: c_int) -> c_int;
}

// libpam links via the system `libpam` (`-lpam`).
#[link(name = "pam")]
unsafe extern "C" {}

/// PAM conversation callback. `appdata_ptr` points to a NUL-terminated copy of
/// the password. For each echo-on/echo-off prompt PAM raises we hand back a
/// fresh `strdup` of the password; informational/error messages get a NULL
/// response. PAM owns and frees the returned array and each string with the C
/// allocator, so the array is allocated with `calloc` and each string with
/// `strdup`.
unsafe extern "C" fn conversation(
    num_msg: c_int,
    msg: *mut *const PamMessage,
    resp: *mut *mut PamResponse,
    appdata_ptr: *mut c_void,
) -> c_int {
    if num_msg <= 0 || msg.is_null() || resp.is_null() || appdata_ptr.is_null() {
        return PAM_CONV_ERR;
    }
    let n = num_msg as usize;
    // SAFETY: `n > 0`; the array is handed to PAM, which frees it with `free`.
    let array = unsafe { libc::calloc(n, std::mem::size_of::<PamResponse>()) } as *mut PamResponse;
    if array.is_null() {
        return PAM_CONV_ERR;
    }
    let password = appdata_ptr as *const c_char;
    for i in 0..n {
        // SAFETY: `msg` points to an array of `n` message pointers; `array` was
        // allocated with `n` slots above.
        let message = unsafe { *msg.add(i) };
        let slot = unsafe { array.add(i) };
        unsafe {
            (*slot).resp_retcode = 0;
            if message.is_null() {
                (*slot).resp = ptr::null_mut();
                continue;
            }
            match (*message).msg_style {
                // strdup so PAM can free the response independently of our
                // CString (and so each prompt gets its own allocation).
                PAM_PROMPT_ECHO_OFF | PAM_PROMPT_ECHO_ON => {
                    (*slot).resp = libc::strdup(password);
                }
                _ => {
                    (*slot).resp = ptr::null_mut();
                }
            }
        }
    }
    // SAFETY: `resp` is a valid out-pointer (checked non-null above).
    unsafe { *resp = array };
    PAM_SUCCESS
}

/// Run one PAM authentication transaction. Blocking. Returns `Ok(())` only when
/// `pam_authenticate` reports `PAM_SUCCESS`.
fn pam_transaction(service: &str, username: &str, password: &str) -> Result<()> {
    let service_c = CString::new(service)
        .map_err(|_| AuthError::Unauthorized("invalid service name".into()))?;
    let user_c =
        CString::new(username).map_err(|_| AuthError::Unauthorized("invalid username".into()))?;
    // A NUL in the password cannot be passed to C; treat as a failed login.
    let password_c =
        CString::new(password).map_err(|_| AuthError::Unauthorized("invalid password".into()))?;

    let conv = PamConv {
        conv: Some(conversation),
        appdata_ptr: password_c.as_ptr() as *mut c_void,
    };
    let mut handle: *mut c_void = ptr::null_mut();

    // SAFETY: all pointers are valid for the duration of the calls. `conv`
    // borrows `password_c`, which is kept alive (via the explicit `drop` below)
    // until after `pam_authenticate` returns; the conversation runs
    // synchronously inside `pam_authenticate`.
    let auth = unsafe {
        let start = pam_start(service_c.as_ptr(), user_c.as_ptr(), &conv, &mut handle);
        if start != PAM_SUCCESS {
            // No handle to tear down on a failed start.
            return Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            ));
        }
        let auth = pam_authenticate(handle, 0);
        pam_end(handle, auth);
        auth
    };
    // Keep the password buffer alive until the transaction (and thus the
    // conversation callback that reads it) has completed.
    drop(password_c);

    if auth == PAM_SUCCESS {
        Ok(())
    } else {
        Err(AuthError::Unauthorized(
            "invalid username or password".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_match_python() {
        let cfg = PamConfig::default();
        assert_eq!(cfg.service, "login");
        assert_eq!(cfg.confirmation_message, "");
    }

    #[test]
    fn config_deserializes_service_and_message() {
        let cfg: PamConfig =
            serde_yaml::from_str("service: sshd\nconfirmation_message: welcome").unwrap();
        assert_eq!(cfg.service, "sshd");
        assert_eq!(cfg.confirmation_message, "welcome");
    }

    #[test]
    fn config_empty_map_uses_defaults() {
        let cfg: PamConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.service, "login");
        assert_eq!(cfg.confirmation_message, "");
    }

    #[test]
    fn name_and_service_are_carried() {
        let a = PamAuthenticator::from_config(
            "corp-pam",
            PamConfig {
                service: "sshd".into(),
                confirmation_message: "hi".into(),
            },
        );
        assert_eq!(a.name(), "corp-pam");
        assert_eq!(a.service(), "sshd");
        assert_eq!(a.confirmation_message(), "hi");
    }

    #[tokio::test]
    async fn blank_username_is_rejected_without_touching_pam() {
        let a = PamAuthenticator::from_config("pam", PamConfig::default());
        let err = a.authenticate("", "secret").await.unwrap_err();
        assert!(matches!(err, AuthError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn blank_password_is_rejected_without_touching_pam() {
        let a = PamAuthenticator::from_config("pam", PamConfig::default());
        let err = a.authenticate("alice", "").await.unwrap_err();
        assert!(matches!(err, AuthError::Unauthorized(_)));
    }
}
