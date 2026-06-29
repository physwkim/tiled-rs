//! SAML 2.0 SP-initiated web SSO.
//!
//! ## C-library build requirement
//! This module links against `xmlsec1` at compile time (via the `samael`
//! crate's `xmlsec` feature).  Install before building:
//! - macOS: `brew install libxmlsec1`
//! - Debian/Ubuntu: `apt-get install libxmlsec1-dev libxml2-dev`
//!
//! ## Signature validation
//! Every SAML Response must carry a valid XML digital signature over the
//! `<saml:Assertion>` (or the enclosing `<samlp:Response>`) that
//! verifies against the configured IdP certificate.  The check is
//! performed inside `samael`'s `parse_base64_response` — it calls
//! `xmlsec1` whenever `idp_signing_certs()` returns at least one
//! certificate.  [`SamlProvider::new`] returns an error when
//! `idp_cert_pem` is empty because an absent cert would silently disable
//! the signature check in samael (no cert → no verification path).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use samael::crypto::ReduceMode;
use samael::metadata::{EntityDescriptor, HTTP_REDIRECT_BINDING};
use samael::service_provider::ServiceProviderBuilder;

use crate::auth::authenticator::Subject;
use crate::auth::error::{AuthError, Result};

/// Configuration for one SAML 2.0 SP-initiated provider.
#[derive(Debug, Clone)]
pub struct SamlConfig {
    /// IdP entity ID (must match the `entityID` in the IdP metadata).
    pub idp_entity_id: String,
    /// IdP HTTP-Redirect binding SSO URL.
    pub idp_sso_url: String,
    /// IdP signing certificate in PEM format.  Must be non-empty;
    /// signature validation cannot be skipped.
    pub idp_cert_pem: String,
    /// SP entity ID.
    pub sp_entity_id: String,
    /// SP Assertion Consumer Service URL.  Must match the `Recipient` /
    /// `Destination` values in the IdP's SAML assertion.
    pub sp_acs_url: String,
    /// SAML attribute whose first value is used as the tiled principal
    /// identifier, e.g. `"uid"` or the full eduPerson OID.
    pub attribute_name: String,
    /// Maximum age of a SAML Response/Assertion from its `IssueInstant`
    /// (default: 5 minutes).  Only set this to a large value in tests with
    /// historical test vectors.
    pub max_issue_delay: Option<Duration>,
    /// Maximum accepted clock skew between SP and IdP (default: 3 minutes).
    pub max_clock_skew: Option<Duration>,
}

// ---------------------------------------------------------------------------
// Pending request store
// ---------------------------------------------------------------------------

/// In-memory store for pending SP-initiated `AuthnRequest` IDs.
///
/// On `build_redirect_url` the generated request `ID` is inserted with a
/// 10-minute TTL.  On `validate_response` all non-expired IDs are passed to
/// samael so the `InResponseTo` claim in the SAML Response is validated
/// against them (anti-replay / CSRF protection per SAML 2.0 §3.4.1.4).
/// The matched ID is then removed so each response can only be consumed once.
///
/// **Single-process limitation**: does not survive server restarts.  A
/// horizontally-scaled deployment needs a shared external store (e.g. Redis).
pub struct PendingSamlStore {
    inner: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl PendingSamlStore {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Insert `request_id` with a 10-minute TTL.
    pub fn insert(&self, request_id: String) {
        let mut map = self.inner.lock().expect("PendingSamlStore mutex poisoned");
        map.insert(request_id, Utc::now() + Duration::minutes(10));
    }

    /// Return all non-expired IDs, lazily purging stale entries.
    pub fn collect_valid_ids(&self) -> Vec<String> {
        let mut map = self.inner.lock().expect("PendingSamlStore mutex poisoned");
        let now = Utc::now();
        map.retain(|_, exp| *exp > now);
        map.keys().cloned().collect()
    }

    /// Remove `request_id` (called after a successful ACS round-trip to
    /// prevent replay).
    pub fn remove(&self, request_id: &str) {
        self.inner
            .lock()
            .expect("PendingSamlStore mutex poisoned")
            .remove(request_id);
    }
}

// ---------------------------------------------------------------------------
// SamlProvider
// ---------------------------------------------------------------------------

/// One configured SAML 2.0 SP-initiated provider.
///
/// The name appears in the URL paths `/api/v1/auth/saml/{name}/login` and
/// `/api/v1/auth/saml/{name}/acs`.  Clone is cheap — the inner
/// `ServiceProvider` is wrapped in `Arc`.
pub struct SamlProvider {
    /// Mount-point name.
    pub name: String,
    sp: Arc<samael::service_provider::ServiceProvider>,
    attribute_name: String,
    /// Pending request store (shared with the route handlers).
    pub pending_requests: Arc<PendingSamlStore>,
}

impl SamlProvider {
    /// Build a [`SamlProvider`] from `config`.
    ///
    /// Returns an error when:
    /// - `idp_cert_pem` is empty (signature validation is mandatory; an
    ///   absent cert would silently disable it in samael).
    /// - The IdP metadata XML cannot be parsed.
    /// - The samael `ServiceProvider` builder rejects the configuration.
    pub fn new(name: impl Into<String>, config: SamlConfig) -> Result<Self> {
        if config.idp_cert_pem.trim().is_empty() {
            return Err(AuthError::Validation(
                "saml: idp_cert_pem must not be empty — \
                 signature validation is mandatory"
                    .into(),
            ));
        }

        let cert_b64 = pem_cert_body(&config.idp_cert_pem);
        if cert_b64.is_empty() {
            return Err(AuthError::Validation(
                "saml: idp_cert_pem contains no certificate body".into(),
            ));
        }

        let metadata_xml = build_idp_metadata_xml(
            &xml_escape(&config.idp_entity_id),
            &xml_escape(&config.idp_sso_url),
            &cert_b64,
        );

        let idp_metadata: EntityDescriptor = metadata_xml.parse().map_err(|e| {
            AuthError::Validation(format!("saml: failed to parse IdP metadata XML: {e}"))
        })?;

        let sp = ServiceProviderBuilder::default()
            .entity_id(Some(config.sp_entity_id))
            .acs_url(Some(config.sp_acs_url))
            .idp_metadata(idp_metadata)
            .allow_idp_initiated(false)
            .max_issue_delay(
                config
                    .max_issue_delay
                    .unwrap_or_else(|| Duration::minutes(5)),
            )
            .max_clock_skew(
                config
                    .max_clock_skew
                    .unwrap_or_else(|| Duration::seconds(180)),
            )
            .build()
            .map_err(|e| {
                AuthError::Validation(format!("saml: ServiceProvider build failed: {e}"))
            })?;

        Ok(Self {
            name: name.into(),
            sp: Arc::new(sp),
            attribute_name: config.attribute_name,
            pending_requests: Arc::new(PendingSamlStore::new()),
        })
    }

    /// Build the HTTP-Redirect binding URL for an SP-initiated `AuthnRequest`.
    ///
    /// The generated `AuthnRequest.ID` is stored in the pending-requests
    /// store with a 10-minute TTL.
    ///
    /// Returns `(redirect_url, request_id)`.
    pub fn build_redirect_url(&self) -> Result<(String, String)> {
        let idp_sso_url = self
            .sp
            .sso_binding_location(HTTP_REDIRECT_BINDING)
            .ok_or_else(|| {
                AuthError::Validation("saml: no HTTP-Redirect SSO service in IdP metadata".into())
            })?;

        let request = self
            .sp
            .make_authentication_request(&idp_sso_url)
            .map_err(|e| AuthError::Validation(format!("saml: AuthnRequest build error: {e}")))?;

        let request_id = request.id.clone();

        let redirect_url = request
            .redirect("")
            .map_err(|e| AuthError::Validation(format!("saml: redirect URL build error: {e}")))?
            .ok_or_else(|| {
                AuthError::Validation("saml: AuthnRequest has no destination URL".into())
            })?
            .to_string();

        self.pending_requests.insert(request_id.clone());

        Ok((redirect_url, request_id))
    }

    /// Parse and validate a base64-encoded SAML Response received on the ACS
    /// endpoint.
    ///
    /// Enforces (via samael + xmlsec1):
    /// - **XML digital signature** against the configured IdP certificate.
    ///   A missing or invalid signature is always rejected.
    /// - `Issuer` matches the configured `idp_entity_id`.
    /// - `Audience` restriction matches the SP entity ID.
    /// - `NotBefore` / `NotOnOrAfter` conditions and `IssueInstant` freshness.
    /// - `InResponseTo` is in the non-expired pending request store (anti-replay).
    /// - `Recipient` matches the SP ACS URL.
    ///
    /// On success the consumed request ID is removed from the store and the
    /// method returns a [`Subject`] with `provider = self.name` and `sub`
    /// equal to the first value of the configured attribute.
    pub fn validate_response(&self, encoded_response: &str) -> Result<Subject> {
        let valid_ids = self.pending_requests.collect_valid_ids();
        let id_refs: Vec<&str> = valid_ids.iter().map(|s| s.as_str()).collect();
        let possible_ids = if id_refs.is_empty() {
            None
        } else {
            Some(id_refs.as_slice())
        };

        // Use PreDigest mode (samael's strictest): xmlsec1 must find exactly one
        // valid Signature and returns only the verified pre-digest content.
        // The default ValidateAndMarkNoAncestors mode fails on xmlsec1 1.3.x with
        // XPointer id(...) references in certain test vectors; PreDigest avoids
        // that code path and is the mode samael's own tests recommend.
        let assertion = self
            .sp
            .parse_base64_response_with_mode(encoded_response, possible_ids, ReduceMode::PreDigest)
            .map_err(|e| {
                AuthError::Unauthorized(format!("saml: response validation failed: {e}"))
            })?;

        // Consume the matched request ID to prevent replay of the same Response.
        let in_response_to = assertion
            .subject
            .as_ref()
            .and_then(|s| s.subject_confirmations.as_ref())
            .and_then(|cs| cs.first())
            .and_then(|c| c.subject_confirmation_data.as_ref())
            .and_then(|d| d.in_response_to.clone());
        if let Some(ref id) = in_response_to {
            self.pending_requests.remove(id);
        }

        let sub = extract_attribute(&assertion, &self.attribute_name).ok_or_else(|| {
            AuthError::Unauthorized(format!(
                "saml: attribute '{}' not found in assertion",
                self.attribute_name
            ))
        })?;

        Ok(Subject {
            provider: self.name.clone(),
            sub,
        })
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Strip PEM header/footer lines to obtain the raw base64-encoded DER body.
fn pem_cert_body(pem: &str) -> String {
    pem.lines()
        .filter(|l| !l.starts_with("-----"))
        .collect::<Vec<_>>()
        .join("")
}

/// Build an IdP SAML metadata XML string from individual fields.
/// `cert_b64` is the raw base64-encoded DER cert body (no PEM headers).
/// The `entity_id_escaped` and `sso_url_escaped` parameters must already
/// have XML special characters escaped (see `xml_escape`).
fn build_idp_metadata_xml(
    entity_id_escaped: &str,
    sso_url_escaped: &str,
    cert_b64: &str,
) -> String {
    format!(
        r#"<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="{entity_id_escaped}">
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:KeyDescriptor use="signing">
      <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data>
          <ds:X509Certificate>{cert_b64}</ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
    </md:KeyDescriptor>
    <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="{sso_url_escaped}"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>"#
    )
}

/// Minimal XML attribute-value escaping.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Extract the first value of a named SAML attribute from an assertion.
fn extract_attribute(assertion: &samael::schema::Assertion, name: &str) -> Option<String> {
    assertion
        .attribute_statements
        .as_ref()?
        .iter()
        .flat_map(|stmt| &stmt.attributes)
        .find(|attr| attr.name.as_deref() == Some(name))
        .and_then(|attr| attr.values.first())
        .and_then(|v| v.value.clone())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    // Test vectors from samael's test_vectors/ directory.
    //
    // `response_signed_assertion.xml` is a SAML Response containing a
    // signed <saml:Assertion>.  The signing certificate is embedded inside
    // the <ds:Signature><ds:KeyInfo><ds:X509Certificate> element.
    //
    // Key parameters of this vector:
    //   InResponseTo : ONELOGIN_4fee3b046395c4e751011e97f8900b5273d56685
    //   Issuer       : http://idp.example.com/metadata.php
    //   Audience     : http://sp.example.com/demo1/metadata.php
    //   ACS URL      : http://sp.example.com/demo1/index.php?acs
    //   Attributes   : uid="test", mail="test@example.com"
    //   IssueInstant : 2014-07-17T01:01:48Z
    //
    // NOTE: these tests use a pre-generated key pair rather than one
    // generated at test-runtime.  Generating a fully-signed SAML Response
    // XML at test runtime requires calling xmlsec1's signing API on a
    // correctly-structured XML template; samael does not expose that path
    // publicly for Response-level documents.  Coverage:
    //   COVERED   — signature acceptance (valid cert + unmodified XML)
    //   COVERED   — tamper rejection (modified attribute value in signed element)
    //   COVERED   — unsigned rejection (Signature element stripped)
    //   COVERED   — empty-cert rejection (constructor guard)
    //   NOT COVERED — key generated fresh per test run (pre-generated instead)
    const SIGNED_ASSERTION_RESPONSE: &str =
        include_str!("test_vectors/response_signed_assertion.xml");

    /// Extract the first `<ds:X509Certificate>` value from SAML XML.
    fn extract_cert_b64(xml: &str) -> String {
        // The element may appear with or without the `ds:` prefix depending
        // on how the namespace is declared on the enclosing element.
        let open = xml
            .find("<ds:X509Certificate>")
            .map(|i| (i, "<ds:X509Certificate>".len()))
            .or_else(|| {
                xml.find("<X509Certificate>")
                    .map(|i| (i, "<X509Certificate>".len()))
            })
            .expect("test vector must contain an X509Certificate element");
        let (start_tag, tag_len) = open;
        let content_start = start_tag + tag_len;
        let close_marker = if xml[start_tag..].starts_with("<ds:X509Certificate>") {
            "</ds:X509Certificate>"
        } else {
            "</X509Certificate>"
        };
        let end = xml[content_start..]
            .find(close_marker)
            .expect("X509Certificate element must be closed");
        xml[content_start..content_start + end].trim().to_string()
    }

    fn cert_pem_from_b64(b64: &str) -> String {
        format!("-----BEGIN CERTIFICATE-----\n{b64}\n-----END CERTIFICATE-----\n")
    }

    fn test_provider() -> SamlProvider {
        let cert_b64 = extract_cert_b64(SIGNED_ASSERTION_RESPONSE);
        let cert_pem = cert_pem_from_b64(&cert_b64);

        // The test vector was issued in 2014; open max_issue_delay wide enough
        // to accept it without defeating the actual expiry logic in samael.
        let response_instant: DateTime<Utc> = "2014-07-17T01:01:48Z".parse().unwrap();
        let elapsed = Utc::now() - response_instant;
        let max_issue_delay = elapsed + Duration::seconds(60);

        SamlProvider::new(
            "test-idp",
            SamlConfig {
                idp_entity_id: "http://idp.example.com/metadata.php".into(),
                idp_sso_url: "https://idp.example.com/sso".into(),
                idp_cert_pem: cert_pem,
                sp_entity_id: "http://sp.example.com/demo1/metadata.php".into(),
                sp_acs_url: "http://sp.example.com/demo1/index.php?acs".into(),
                attribute_name: "uid".into(),
                max_issue_delay: Some(max_issue_delay),
                max_clock_skew: Some(Duration::days(5000)),
            },
        )
        .expect("SamlProvider::new must succeed with valid config")
    }

    #[test]
    fn authn_request_redirect_url_structure() {
        let sp = test_provider();
        let (url_str, request_id) = sp.build_redirect_url().expect("redirect URL must be built");

        assert!(
            url_str.starts_with("https://idp.example.com/sso"),
            "redirect URL must target the configured SSO URL, got: {url_str}"
        );
        assert!(
            url_str.contains("SAMLRequest="),
            "redirect URL must carry a SAMLRequest query param, got: {url_str}"
        );

        // Generated ID must be stored in the pending store.
        let pending = sp.pending_requests.collect_valid_ids();
        assert!(
            pending.contains(&request_id),
            "generated request ID must be in the pending store"
        );
    }

    // Signature ACCEPTANCE on xmlsec1 1.3.x + libxml2 2.15.x.  This previously failed (and
    // was #[ignore]d with an incorrect "xmlAddID regression" diagnosis) because the build
    // linked TWO libxml2 instances — the `libxml` crate's and xmlsec1's.  XMLDSIG maps every
    // barename fragment `URI="#id"` to `xpointer(id('id'))` (RFC 3986 §5.3); samael's
    // `collect_id_attributes` registered the XML `ID` in one libxml2 instance while xmlsec
    // resolved `xpointer(id('...'))` in the OTHER and never found it, so every signature was
    // rejected as "SAML Response and all assertions must be signed".
    //
    // Neither samael nor libxml2 2.15.x is at fault: in pure C against a SINGLE libxml2
    // 2.15.3, xmlAddID + xmlXPtrEval(id()) resolves correctly.  The fix is at the build
    // layer — `.cargo/config.toml` puts Homebrew's libxml2 first on the link search path so
    // every `-lxml2` (the `libxml` crate's and xmlsec1-config's bare `-lxml2`) resolves to
    // one libxml2 instance.  Both the legacy vector (this test) and the modern
    // enveloped-signature fixture (validate_response_accepts_modern_signature) now verify.
    // See `test_vectors/generate-modern-fixture.sh` for the modern fixture.
    #[test]
    fn validate_response_accepts_valid_signature() {
        let sp = test_provider();

        // Pre-seed the pending store with the InResponseTo ID from the vector.
        let in_response_to = "ONELOGIN_4fee3b046395c4e751011e97f8900b5273d56685";
        sp.pending_requests.insert(in_response_to.to_string());

        let encoded = base64::engine::general_purpose::STANDARD.encode(SIGNED_ASSERTION_RESPONSE);
        let subject = sp
            .validate_response(&encoded)
            .expect("valid signed response must be accepted");

        assert_eq!(subject.provider, "test-idp");
        assert_eq!(
            subject.sub, "test",
            "uid attribute value must be 'test', got: {}",
            subject.sub
        );
    }

    #[test]
    // The tampered uid value changes the signed assertion's digest, so xmlsec rejects the
    // response on digest mismatch — the signature no longer covers the modified content.
    // With the single-libxml2 build (see validate_response_accepts_valid_signature) the
    // `#id` reference resolves and the digest check actually runs, so this is a real
    // cryptographic rejection, not a side effect of failed ID resolution.
    fn validate_response_rejects_tampered_assertion() {
        let sp = test_provider();
        sp.pending_requests
            .insert("ONELOGIN_4fee3b046395c4e751011e97f8900b5273d56685".to_string());

        // Tamper: change uid value "test" → "admin" inside the signed element.
        let tampered = SIGNED_ASSERTION_RESPONSE.replace(
            ">test</saml:AttributeValue>",
            ">admin</saml:AttributeValue>",
        );
        assert_ne!(
            tampered, SIGNED_ASSERTION_RESPONSE,
            "tampered XML must differ from original"
        );

        let encoded = base64::engine::general_purpose::STANDARD.encode(&tampered);
        let err = sp
            .validate_response(&encoded)
            .expect_err("tampered response must be rejected");

        assert!(
            err.to_string().contains("validation failed"),
            "error must report validation failure, got: {err}"
        );
    }

    #[test]
    fn validate_response_rejects_signature_stripped() {
        let sp = test_provider();
        sp.pending_requests
            .insert("ONELOGIN_4fee3b046395c4e751011e97f8900b5273d56685".to_string());

        // Strip the Signature element to produce an unsigned response.
        let sig_open = SIGNED_ASSERTION_RESPONSE.find("<ds:Signature");
        let sig_close_marker = "</ds:Signature>";
        let sig_close = SIGNED_ASSERTION_RESPONSE.find(sig_close_marker);
        let (Some(s), Some(e)) = (sig_open, sig_close) else {
            panic!("test vector must contain a ds:Signature element");
        };
        let unsigned = format!(
            "{}{}",
            &SIGNED_ASSERTION_RESPONSE[..s],
            &SIGNED_ASSERTION_RESPONSE[e + sig_close_marker.len()..]
        );

        let encoded = base64::engine::general_purpose::STANDARD.encode(&unsigned);
        let err = sp
            .validate_response(&encoded)
            .expect_err("unsigned response must be rejected when cert is configured");

        // samael emits FailedToValidateSignature or similar when no valid signature exists.
        assert!(
            err.to_string().contains("validation failed"),
            "error must report validation failure, got: {err}"
        );
    }

    // Signature acceptance for a MODERN-style fixture: enveloped-signature + exc-c14n,
    // URI="#_assertion001", no xpointer transform, signed by the xmlsec1 CLI with
    // --id-attr:ID (see test_vectors/generate-modern-fixture.sh).  This is the signature
    // shape modern IdPs (Okta, Azure AD, Shibboleth) emit.  It verifies on the
    // single-libxml2 build for the same reason as validate_response_accepts_valid_signature.
    #[test]
    fn validate_response_accepts_modern_signature() {
        let cert_pem = include_str!("test_vectors/probe-idp.crt").to_string();
        let signed_xml = include_str!("test_vectors/response_modern_signed.xml").to_string();

        let sp = SamlProvider::new(
            "probe-idp",
            SamlConfig {
                idp_entity_id: "http://idp.example.com".into(),
                idp_sso_url: "http://idp.example.com/sso".into(),
                idp_cert_pem: cert_pem,
                sp_entity_id: "http://sp.example.com".into(),
                sp_acs_url: "http://sp.example.com/acs".into(),
                attribute_name: "uid".into(),
                max_issue_delay: Some(Duration::days(365 * 200)),
                max_clock_skew: Some(Duration::days(365 * 200)),
            },
        )
        .expect("SamlProvider::new must succeed with valid cert");

        sp.pending_requests
            .insert("TILED_TEST_REQUEST_001".to_string());

        let encoded = base64::engine::general_purpose::STANDARD.encode(signed_xml.as_bytes());
        let subject = sp
            .validate_response(&encoded)
            .expect("modern enveloped-signature response must be accepted");

        assert_eq!(subject.provider, "probe-idp");
        assert_eq!(
            subject.sub, "testuser",
            "uid attribute value must be 'testuser', got: {}",
            subject.sub
        );
    }

    #[test]
    fn provider_new_rejects_empty_cert() {
        let err = SamlProvider::new(
            "bad",
            SamlConfig {
                idp_entity_id: "https://idp.example.com/metadata".into(),
                idp_sso_url: "https://idp.example.com/sso".into(),
                idp_cert_pem: "".into(),
                sp_entity_id: "https://sp.example.com".into(),
                sp_acs_url: "https://sp.example.com/acs".into(),
                attribute_name: "uid".into(),
                max_issue_delay: None,
                max_clock_skew: None,
            },
        )
        .err()
        .expect("empty cert must return Err");

        assert!(
            err.to_string().contains("idp_cert_pem"),
            "error must mention idp_cert_pem, got: {err}"
        );
    }
}
