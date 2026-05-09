use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::api;
use crate::auth::use_auth;
use crate::components::array_view::{ArrayInfo, ArrayView};

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
