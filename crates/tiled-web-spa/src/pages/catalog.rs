use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::api;
use crate::auth::use_auth;
use crate::components::array_view::{ArrayInfo, ArrayView};
use crate::settings::{SpecView, render_url, use_settings};

#[component]
pub fn CatalogView() -> impl IntoView {
    let auth = use_auth();
    let params = use_params_map();
    let path = Memo::new(move |_| {
        params
            .read()
            .get("path")
            .map(|s| s.trim_matches('/').to_string())
            .unwrap_or_default()
    });

    let children = LocalResource::new(move || {
        let p = path.get();
        async move { api::fetch_children(&auth, &p).await }
    });
    let metadata = LocalResource::new(move || {
        let p = path.get();
        async move { api::fetch_metadata(&auth, &p).await }
    });

    view! {
        <div class="grid gap-4">
            <Breadcrumb path=path />
            <section class="card">
                <h2 class="text-lg font-semibold mb-2">"Metadata"</h2>
                <Suspense fallback=move || view! { <p class="text-slate-500">"loading..."</p> }>
                    {move || metadata.get().map(|res| match res.take() {
                        Ok(env) => {
                            let family = env.data.attributes.structure_family
                                .clone()
                                .unwrap_or_else(|| "?".into());
                            let array_info = if family == "array" {
                                env.data.attributes.structure
                                    .as_ref()
                                    .and_then(|s| ArrayInfo::from_resource(s, &env.data.links))
                            } else {
                                None
                            };
                            view! {
                                <dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
                                    <dt class="text-slate-500">"id"</dt>
                                    <dd class="font-mono">{env.data.id.clone()}</dd>
                                    <dt class="text-slate-500">"family"</dt>
                                    <dd>{family}</dd>
                                </dl>
                                {array_info.map(|info| view! {
                                    <div class="mt-4">
                                        <ArrayView info=info />
                                    </div>
                                })}
                                {render_spec_views(
                                    env.data.attributes.specs.clone(),
                                    env.data.id.clone(),
                                    env.data.attributes.metadata.clone(),
                                )}
                                <details class="mt-3 text-xs">
                                    <summary class="cursor-pointer text-slate-600">"raw metadata"</summary>
                                    <pre class="mt-2 overflow-x-auto rounded bg-slate-50 p-3 font-mono">
                                        {env.data.attributes.metadata
                                            .map(|m| serde_json::to_string_pretty(&m).unwrap_or_default())
                                            .unwrap_or_default()}
                                    </pre>
                                </details>
                            }.into_any()
                        }
                        Err(e) => view! {
                            <p class="text-red-600 text-sm">{format!("error: {e}")}</p>
                        }.into_any(),
                    })}
                </Suspense>
            </section>
            <section class="card">
                <h2 class="text-lg font-semibold mb-2">"Children"</h2>
                <Suspense fallback=move || view! { <p class="text-slate-500">"loading..."</p> }>
                    {move || children.get().map(|res| match res.take() {
                        Ok(env) => view! {
                            <ChildList path=path entries=env.data />
                        }.into_any(),
                        Err(e) => view! {
                            <p class="text-red-600 text-sm">{format!("error: {e}")}</p>
                        }.into_any(),
                    })}
                </Suspense>
            </section>
        </div>
    }
}

/// Render the configured spec_view links for any spec on this resource
/// that the operator has registered. Mirrors upstream tiled PR #1349 +
/// #1365 — but as outbound links instead of dynamic React components,
/// since we're a WASM SPA. Each link gets `{path}` and `{metadata}`
/// placeholders substituted at render time so the receiving viewer can
/// pick up the resource id and metadata via URL.
fn render_spec_views(
    specs: Vec<serde_json::Value>,
    resource_id: String,
    metadata: Option<serde_json::Value>,
) -> AnyView {
    let settings = use_settings();
    let spec_names: Vec<String> = specs
        .into_iter()
        .filter_map(|s| {
            s.as_str()
                .map(String::from)
                .or_else(|| s.get("name").and_then(|n| n.as_str()).map(String::from))
        })
        .collect();
    if spec_names.is_empty() {
        return ().into_any();
    }
    let metadata_json = metadata
        .map(|m| serde_json::to_string(&m).unwrap_or_default())
        .unwrap_or_default();

    let configured: Vec<SpecView> = settings.spec_views.get();
    let matched: Vec<(String, SpecView)> = spec_names
        .iter()
        .flat_map(|name| {
            configured
                .iter()
                .filter(move |sv| sv.spec == *name)
                .cloned()
                .map(|sv| (name.clone(), sv))
        })
        .collect();
    if matched.is_empty() {
        return ().into_any();
    }

    view! {
        <div class="mt-4 border-t border-slate-200 pt-3">
            <h3 class="text-sm font-semibold text-slate-700 mb-2">
                "Open in external viewer"
            </h3>
            <ul class="space-y-1 text-sm">
                {matched.into_iter().map(|(name, sv)| {
                    let href = render_url(&sv.url, &resource_id, &metadata_json);
                    let label = sv.label.clone()
                        .unwrap_or_else(|| format!("Open as {name}"));
                    view! {
                        <li>
                            <a
                                href=href
                                target="_blank"
                                rel="noopener noreferrer"
                                class="text-blue-700 hover:underline"
                            >
                                {label}
                            </a>
                        </li>
                    }
                }).collect::<Vec<_>>()}
            </ul>
        </div>
    }
    .into_any()
}

#[component]
fn Breadcrumb(path: Memo<String>) -> impl IntoView {
    view! {
        <nav class="text-sm text-slate-600">
            <a href="/catalog/" class="hover:text-slate-900">"/"</a>
            {move || {
                let parts: Vec<String> = path.get().split('/').filter(|s| !s.is_empty()).map(String::from).collect();
                let mut cumulative = String::new();
                parts.iter().enumerate().map(|(i, seg)| {
                    if i > 0 { cumulative.push('/'); }
                    cumulative.push_str(seg);
                    let href = format!("/catalog/{cumulative}");
                    view! {
                        <span class="text-slate-400 mx-1">"/"</span>
                        <a href=href class="hover:text-slate-900">{seg.clone()}</a>
                    }
                }).collect::<Vec<_>>()
            }}
        </nav>
    }
}

#[component]
fn ChildList(
    path: Memo<String>,
    entries: Vec<api::ResourceData>,
) -> impl IntoView {
    if entries.is_empty() {
        return view! { <p class="text-sm text-slate-500">"(no children)"</p> }.into_any();
    }
    view! {
        <ul class="divide-y divide-slate-200 text-sm">
            {entries.into_iter().map(|child| {
                let parent = path.get();
                let href = if parent.is_empty() {
                    format!("/catalog/{}", child.id)
                } else {
                    format!("/catalog/{}/{}", parent, child.id)
                };
                let family = child.attributes.structure_family.unwrap_or_default();
                view! {
                    <li class="py-2 flex items-center justify-between">
                        <a href=href class="text-blue-700 hover:underline font-medium">
                            {child.id}
                        </a>
                        <span class="text-xs uppercase tracking-wide text-slate-500">
                            {family}
                        </span>
                    </li>
                }
            }).collect::<Vec<_>>()}
        </ul>
    }.into_any()
}
