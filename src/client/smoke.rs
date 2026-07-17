//! `smoke` — a recursive whole-tree health check.
//!
//! Port of `tiled/client/smoke.py`'s `read`. Given a node, [`read`] walks it: a
//! container recurses into every child; a leaf is `read()` in full. Every leaf
//! whose read raises is collected (its URI and the error) and returned; an empty
//! result means the whole subtree read cleanly. Errors while *walking* a
//! container (listing keys, fetching a child) propagate — only the per-leaf
//! `read()` is guarded, matching upstream, which catches solely around
//! `node.read()`.
//!
//! ## Deviations from upstream
//!
//! * **Structured return over `verbose`/`strict`.** Upstream's `read(node,
//!   verbose, strict)` prints per-node status to stderr when `verbose` and
//!   re-raises on the first fault when `strict`. Neither is idiomatic for a
//!   library entry point: [`read`] instead returns each fault as a [`FaultyLeaf`]
//!   carrying both the URI and the [`ClientError`], so the caller decides whether
//!   to log, stop, or continue. Upstream only prints the error (collecting bare
//!   URIs); pairing each URI with its error is the one intentional enrichment.
//!
//! * **Custom clients are flagged, not read.** A resolver-substituted
//!   [`AnyClient::Custom`] exposes no uniform `read()` (and no `BaseClient`, hence
//!   no URI), so smoke cannot verify it. Rather than silently pass an unchecked
//!   node — which would misreport it as healthy — it is recorded as a
//!   [`FaultyLeaf`] with an empty URI. The default client stack installs no
//!   resolver, so this case does not arise in a plain `from_uri` walk.

use std::future::Future;
use std::pin::Pin;

use crate::client::any_client::AnyClient;
use crate::client::error::{ClientError, Result};

/// One leaf whose `read()` failed during a [`read`] walk.
///
/// Mirrors one entry of upstream's returned `faulty_docs` list (`smoke.py:26`),
/// which holds bare URIs; the [`error`](Self::error) is the intentional
/// enrichment (upstream only prints it when `verbose`).
#[derive(Debug)]
pub struct FaultyLeaf {
    /// The leaf's `self` URI (upstream's `node.uri`). Empty only when the node
    /// exposes no `BaseClient` — a resolver-substituted [`AnyClient::Custom`].
    pub uri: String,
    /// The error the leaf's `read()` produced (or, for a custom client, the
    /// reason smoke could not verify it).
    pub error: ClientError,
}

/// Walk `node`, reading every leaf, and return the leaves whose read failed.
///
/// Ports `read(node)` (`smoke.py:6-44`): a container recurses into each child; a
/// leaf is `read()` in full and, on failure, recorded as a [`FaultyLeaf`]. An
/// empty result means the whole subtree read cleanly. Failures while *walking* a
/// container (listing its keys, fetching a child) propagate as `Err`; only the
/// per-leaf `read()` is caught, matching upstream.
pub async fn read(node: &AnyClient) -> Result<Vec<FaultyLeaf>> {
    walk(node).await
}

/// Boxed recursive future — `walk` calls itself for sub-containers, so the
/// returned future must be heap-allocated to have a finite size (as in
/// [`crate::client::sync`]).
type WalkFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<FaultyLeaf>>> + Send + 'a>>;

fn walk(node: &AnyClient) -> WalkFuture<'_> {
    Box::pin(async move {
        match node {
            AnyClient::Container(c) => {
                // Upstream `for key, child_node in node.items(): read(child_node)`
                // (`smoke.py:27-29`). A walk failure (keys / get) propagates.
                let mut faulty = Vec::new();
                for key in c.keys().await? {
                    let child = c.get(&key).await?;
                    faulty.extend(walk(&child).await?);
                }
                Ok(faulty)
            }
            // A leaf: read it in full. Only this read is guarded (`smoke.py:32-39`).
            _ => Ok(match read_leaf(node).await {
                Ok(()) => Vec::new(),
                Err(error) => vec![FaultyLeaf {
                    uri: leaf_uri(node),
                    error,
                }],
            }),
        }
    })
}

/// Read one leaf in full, discarding the data — the read either succeeds or
/// surfaces the fault smoke is looking for. Dispatches on family the way
/// upstream's polymorphic `node.read()` does.
async fn read_leaf(node: &AnyClient) -> Result<()> {
    match node {
        AnyClient::Array(a) => a.read().await.map(drop),
        AnyClient::Table(t) => t.read(None).await.map(drop),
        AnyClient::Sparse(s) => s.read().await.map(drop),
        AnyClient::Awkward(a) => a.read().await.map(drop),
        AnyClient::Ragged(r) => r.read().await.map(drop),
        // Container is handled by `walk` and never reaches here. A custom
        // (resolver-substituted) client has no uniform read, so smoke flags it
        // as unverifiable rather than silently passing it (see module docs).
        AnyClient::Container(_) | AnyClient::Custom(_) => Err(ClientError::Invalid(
            "smoke cannot read this node: no uniform read for a custom client".into(),
        )),
    }
}

/// The leaf's `self` URI (upstream's `node.uri`), or an empty string when the
/// node exposes no `BaseClient` (the `Custom` variant).
fn leaf_uri(node: &AnyClient) -> String {
    node.base()
        .and_then(|b| b.uri())
        .unwrap_or_default()
        .to_string()
}
