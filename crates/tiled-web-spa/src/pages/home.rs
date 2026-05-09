use leptos::prelude::*;

use crate::api;

#[component]
pub fn Home() -> impl IntoView {
    let about = LocalResource::new(api::fetch_about);

    view! {
        <div class="grid gap-4">
            <section class="card">
                <h2 class="text-lg font-semibold mb-2">"Server"</h2>
                <Suspense fallback=move || view! { <p class="text-slate-500">"loading..."</p> }>
                    {move || about.get().map(|res| match res.take() {
                        Ok(a) => view! {
                            <dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
                                <dt class="text-slate-500">"api_version"</dt>
                                <dd>{a.api_version}</dd>
                                <dt class="text-slate-500">"library_version"</dt>
                                <dd>{a.library_version}</dd>
                                <dt class="text-slate-500">"queries"</dt>
                                <dd class="font-mono text-xs">{a.queries.join(", ")}</dd>
                            </dl>
                        }.into_any(),
                        Err(e) => view! {
                            <p class="text-red-600 text-sm">{format!("error: {e}")}</p>
                        }.into_any(),
                    })}
                </Suspense>
            </section>
            <section class="card">
                <h2 class="text-lg font-semibold mb-2">"Quick links"</h2>
                <ul class="list-disc pl-5 text-sm text-blue-700 space-y-1">
                    <li><a href="/catalog/">"Browse catalog"</a></li>
                    <li><a href="/admin/api-keys">"API keys"</a></li>
                    <li><a href="/admin/sessions">"Sessions"</a></li>
                    <li><a href="/admin/streaming">"Streaming bus"</a></li>
                </ul>
            </section>
        </div>
    }
}
