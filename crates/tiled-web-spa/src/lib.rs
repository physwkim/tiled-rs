//! tiled-rs SPA.
//!
//! Mounted at `/` by tiled-web. The shell hits `/api/v1/` to surface
//! the live server + offers links into the rest of the API. As we wire
//! richer pages (catalog tree, search, data preview) they live under
//! `pages/`.

mod api;
mod pages;

use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(App);
}

#[component]
pub fn App() -> impl IntoView {
    view! {
        <Router>
            <div class="min-h-screen flex flex-col">
                <Header />
                <main class="container mx-auto max-w-5xl px-4 py-6 flex-1">
                    <Routes fallback=NotFound>
                        <Route path=path!("/") view=pages::home::Home />
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
                <nav class="flex gap-4 text-sm text-slate-600">
                    <a href="/" class="hover:text-slate-900">"Home"</a>
                    <a href="/catalog/" class="hover:text-slate-900">"Catalog"</a>
                    <a href="/admin/" class="hover:text-slate-900">"Admin"</a>
                </nav>
            </div>
        </header>
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
