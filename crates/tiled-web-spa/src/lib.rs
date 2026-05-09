//! tiled-rs SPA.
//!
//! Mounted at `/` by tiled-web. The shell hits `/api/v1/` to surface
//! the live server and discover authentication providers. As we wire
//! richer pages (catalog tree, search, data preview) they live under
//! `pages/`. Auth state is held in `auth::AuthState` (Leptos context).

mod api;
mod auth;
mod pages;

use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;
use wasm_bindgen::prelude::*;

use auth::{provide_auth, use_auth};

#[wasm_bindgen(start)]
pub fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(App);
}

#[component]
pub fn App() -> impl IntoView {
    provide_auth();

    // Bootstrap: fetch /api/v1/ once at mount to populate providers +
    // `required`. After this resolves the login page knows what to
    // render. Done outside any route so the header can also display
    // server info if it wants.
    let auth = use_auth();
    spawn_local(async move {
        // Failure (server unreachable or 5xx) is intentionally
        // swallowed — downstream UI surfaces the error per-request.
        if let Ok(about) = api::fetch_about(&auth).await {
            auth.required.set(about.authentication.required);
            auth.providers.set(about.authentication.providers);
        }
        auth.loaded.set(true);
    });

    view! {
        <Router>
            <div class="min-h-screen flex flex-col">
                <Header />
                <main class="container mx-auto max-w-5xl px-4 py-6 flex-1">
                    <Routes fallback=NotFound>
                        <Route path=path!("/") view=pages::home::Home />
                        <Route path=path!("/login") view=pages::login::Login />
                        <Route path=path!("/catalog/*path") view=pages::catalog::CatalogView />
                    </Routes>
                </main>
                <Footer />
            </div>
        </Router>
    }
}

#[component]
fn Header() -> impl IntoView {
    view! {
        <header class="border-b border-slate-200 bg-white">
            <div class="container mx-auto max-w-5xl px-4 py-3 flex items-center justify-between">
                <a href="/" class="text-lg font-semibold tracking-tight">
                    "tiled-rs"
                </a>
                <nav class="flex items-center gap-4 text-sm text-slate-600">
                    <a href="/" class="hover:text-slate-900">"Home"</a>
                    <a href="/catalog/" class="hover:text-slate-900">"Catalog"</a>
                    <a href="/admin/" class="hover:text-slate-900">"Admin"</a>
                    <AuthButton />
                </nav>
            </div>
        </header>
    }
}

#[component]
fn AuthButton() -> impl IntoView {
    let auth = use_auth();
    let logged_in = move || auth.is_authenticated();
    let username = move || {
        auth.identity
            .get()
            .map(|i| i.id)
            .unwrap_or_else(|| "user".to_string())
    };
    let on_logout = move |_| {
        spawn_local(async move {
            api::logout(&auth).await;
        });
    };
    view! {
        <Show
            when=logged_in
            fallback=|| view! {
                <a href="/login" class="btn-primary text-xs">"Log in"</a>
            }
        >
            <span class="text-xs text-slate-500">{username}</span>
            <button on:click=on_logout class="btn-danger text-xs">"Log out"</button>
        </Show>
    }
}

#[component]
fn Footer() -> impl IntoView {
    view! {
        <footer class="border-t border-slate-200 bg-white py-3 text-center text-xs text-slate-500">
            "tiled-rs · WebUI shell"
        </footer>
    }
}

#[component]
fn NotFound() -> impl IntoView {
    view! {
        <div class="card">
            <h2 class="text-lg font-semibold mb-2">"Not found"</h2>
            <p class="text-slate-600">"The route doesn't exist (yet)."</p>
        </div>
    }
}
