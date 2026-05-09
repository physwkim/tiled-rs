//! Login page — mirrors upstream tiled PR #1350's `routes/login.tsx`.
//!
//! Renders one section per provider advertised by the server:
//!   * `mode = "internal"` → username/password form, POSTs JSON to the
//!     provider's `links.auth_endpoint`;
//!   * `mode = "external"` → button that redirects the browser to the
//!     OIDC `authorize` endpoint with `state=<current-url>` for return.
//!
//! Providers are sourced from the auth context, populated by
//! `App::on_mount` after `GET /api/v1/`.

use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_navigate;

use crate::api;
use crate::auth::types::ProviderInfo;
use crate::auth::use_auth;

#[component]
pub fn Login() -> impl IntoView {
    let auth = use_auth();
    let navigate = use_navigate();

    // Already logged in → bounce to home. Done as an effect so we
    // don't fight Leptos's render order if state flips after mount.
    Effect::new({
        let navigate = navigate.clone();
        move |_| {
            if auth.is_authenticated() {
                navigate("/", Default::default());
            }
        }
    });

    let providers = move || auth.providers.get();
    let loaded = move || auth.loaded.get();

    view! {
        <div class="card max-w-md mx-auto">
            <h2 class="text-xl font-semibold mb-4">"Log in"</h2>
            <Show
                when=loaded
                fallback=|| view! { <p class="text-slate-500">"loading providers..."</p> }
            >
                <Show
                    when=move || !providers().is_empty()
                    fallback=|| view! {
                        <p class="text-slate-500 text-sm">
                            "No authentication providers configured on this server."
                        </p>
                    }
                >
                    <ProviderList providers=Signal::derive(providers) />
                </Show>
            </Show>
        </div>
    }
}

#[component]
fn ProviderList(providers: Signal<Vec<ProviderInfo>>) -> impl IntoView {
    view! {
        <div class="grid gap-4">
            {move || providers.get()
                .into_iter()
                .map(|p| match p.mode.as_str() {
                    "external" => view! { <ExternalLogin provider=p /> }.into_any(),
                    _ => view! { <PasswordLogin provider=p /> }.into_any(),
                })
                .collect::<Vec<_>>()}
        </div>
    }
}

#[component]
fn PasswordLogin(provider: ProviderInfo) -> impl IntoView {
    let auth = use_auth();
    let navigate = use_navigate();
    let username = RwSignal::new(String::new());
    let password = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let busy = RwSignal::new(false);

    let endpoint = provider.links.auth_endpoint.clone();
    let label = provider.provider.clone();
    let confirmation = provider.confirmation_message.clone();

    let on_submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        if busy.get_untracked() {
            return;
        }
        busy.set(true);
        error.set(None);
        let endpoint = endpoint.clone();
        let user = username.get_untracked();
        let pass = password.get_untracked();
        let navigate = navigate.clone();
        spawn_local(async move {
            match api::login(&auth, &endpoint, &user, &pass).await {
                Ok(_) => {
                    navigate("/", Default::default());
                }
                Err(e) => error.set(Some(e)),
            }
            busy.set(false);
        });
    };

    view! {
        <form on:submit=on_submit class="space-y-3">
            <p class="text-sm text-slate-500">
                "Provider: " <span class="font-mono">{label}</span>
            </p>
            {confirmation.map(|msg| view! { <p class="text-sm text-slate-600">{msg}</p> })}
            <div>
                <label class="block text-xs text-slate-600 mb-1">"Username"</label>
                <input
                    type="text"
                    autocomplete="username"
                    required
                    autofocus
                    class="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                    on:input=move |ev| username.set(event_target_value(&ev))
                    prop:value=username
                />
            </div>
            <div>
                <label class="block text-xs text-slate-600 mb-1">"Password"</label>
                <input
                    type="password"
                    autocomplete="current-password"
                    required
                    class="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                    on:input=move |ev| password.set(event_target_value(&ev))
                    prop:value=password
                />
            </div>
            <Show when=move || error.get().is_some()>
                <p class="text-red-600 text-sm">{move || error.get().unwrap_or_default()}</p>
            </Show>
            <button
                type="submit"
                disabled=move || busy.get()
                class="btn-primary w-full disabled:opacity-50"
            >
                {move || if busy.get() { "Logging in..." } else { "Log in" }}
            </button>
        </form>
    }
}

#[component]
fn ExternalLogin(provider: ProviderInfo) -> impl IntoView {
    let label = provider.provider.clone();
    let endpoint = provider.links.auth_endpoint.clone();
    let on_click = move |_| {
        if let Some(win) = web_sys::window() {
            let here = win
                .location()
                .href()
                .unwrap_or_else(|_| "/".to_string());
            let target = format!(
                "{endpoint}?state={}",
                js_sys::encode_uri_component(&here)
            );
            let _ = win.location().set_href(&target);
        }
    };
    view! {
        <button on:click=on_click class="btn-primary w-full">
            "Log in with " {label}
        </button>
    }
}
