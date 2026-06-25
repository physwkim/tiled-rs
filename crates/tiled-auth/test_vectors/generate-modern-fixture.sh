#!/usr/bin/env bash
# Regenerate the modern-style SAML fixture used in saml.rs tests.
#
# Requirements:
#   openssl  (any modern version)
#   xmlsec1  (tested with 1.3.12 via `brew install libxmlsec1`)
#
# Run from any directory; output goes to the same directory as this script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Generate RSA-2048 key + self-signed cert.
openssl genrsa -out "$SCRIPT_DIR/probe-idp.key" 2048 2>/dev/null
openssl req -new -x509 \
  -key "$SCRIPT_DIR/probe-idp.key" \
  -out "$SCRIPT_DIR/probe-idp.crt" \
  -days 36500 \
  -subj "/CN=Probe-IdP" 2>/dev/null
echo "Generated: probe-idp.key  probe-idp.crt"

# 2. Create the SAML Response template with a Signature placeholder.
#    - Assertion-level signature (enveloped-signature + exc-c14n).
#    - <ds:KeyName> is required so xmlsec1 can match the key from the
#      command line; samael ignores it during verification.
cat > "$SCRIPT_DIR/response_modern_template.xml" << 'XML'
<?xml version="1.0" encoding="UTF-8"?>
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
                xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
                ID="_response001"
                Version="2.0"
                IssueInstant="2024-01-01T00:00:00Z"
                Destination="http://sp.example.com/acs"
                InResponseTo="TILED_TEST_REQUEST_001">
  <saml:Issuer>http://idp.example.com</saml:Issuer>
  <samlp:Status>
    <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
  </samlp:Status>
  <saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
                  ID="_assertion001"
                  Version="2.0"
                  IssueInstant="2024-01-01T00:00:00Z">
    <saml:Issuer>http://idp.example.com</saml:Issuer>
    <ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
      <ds:SignedInfo>
        <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
        <ds:Reference URI="#_assertion001">
          <ds:Transforms>
            <ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
            <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
          </ds:Transforms>
          <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
          <ds:DigestValue/>
        </ds:Reference>
      </ds:SignedInfo>
      <ds:SignatureValue/>
      <ds:KeyInfo>
        <ds:KeyName>probe-key</ds:KeyName>
      </ds:KeyInfo>
    </ds:Signature>
    <saml:Subject>
      <saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified">testuser</saml:NameID>
      <saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
        <saml:SubjectConfirmationData NotOnOrAfter="2099-01-01T00:00:00Z"
                                      Recipient="http://sp.example.com/acs"
                                      InResponseTo="TILED_TEST_REQUEST_001"/>
      </saml:SubjectConfirmation>
    </saml:Subject>
    <saml:Conditions NotBefore="2020-01-01T00:00:00Z" NotOnOrAfter="2099-01-01T00:00:00Z">
      <saml:AudienceRestriction>
        <saml:Audience>http://sp.example.com</saml:Audience>
      </saml:AudienceRestriction>
    </saml:Conditions>
    <saml:AttributeStatement>
      <saml:Attribute Name="uid"
                      NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
        <saml:AttributeValue xmlns:xs="http://www.w3.org/2001/XMLSchema"
                             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                             xsi:type="xs:string">testuser</saml:AttributeValue>
      </saml:Attribute>
    </saml:AttributeStatement>
  </saml:Assertion>
</samlp:Response>
XML

# 3. Sign with xmlsec1.
#    - --privkey-pem:probe-key  must match the <ds:KeyName> in the template.
#    - --id-attr:ID             registers the ID attribute so xmlsec1 resolves
#                               URI="#_assertion001" during signing.
xmlsec1 sign \
  --privkey-pem:probe-key "${SCRIPT_DIR}/probe-idp.key,${SCRIPT_DIR}/probe-idp.crt" \
  --id-attr:ID "urn:oasis:names:tc:SAML:2.0:assertion:Assertion" \
  --output "${SCRIPT_DIR}/response_modern_signed.xml" \
  "${SCRIPT_DIR}/response_modern_template.xml"
echo "Signed: response_modern_signed.xml"

# 4. Verify (xmlsec1 CLI, not samael).
xmlsec1 verify \
  --pubkey-cert-pem:probe-key "${SCRIPT_DIR}/probe-idp.crt" \
  --id-attr:ID "urn:oasis:names:tc:SAML:2.0:assertion:Assertion" \
  "${SCRIPT_DIR}/response_modern_signed.xml"
echo "xmlsec1 verify: OK"

# 5. Clean up working files (keep key+cert+signed XML).
rm -f "$SCRIPT_DIR/response_modern_template.xml"
echo
echo "NOTE: probe-idp.key contains the private key — do NOT commit it."
echo "Only probe-idp.crt and response_modern_signed.xml belong in the repo."
