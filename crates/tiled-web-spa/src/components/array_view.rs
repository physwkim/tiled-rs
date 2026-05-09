//! Array viewer — port of upstream tiled's `array-nd` component
//! (PR #1374 grayscale canvas + #1367 color heuristic + #1325 slice fix).
//!
//! Modes:
//! * **Color (PNG)**: when the trailing dim is 3 or 4 the array is
//!   treated as `(H, W, C)` and rendered via the server's PNG
//!   serializer. We fetch with bearer auth, blob-URL the response, and
//!   point an `<img>` at it.
//! * **Grayscale (canvas)**: anything else 2D-or-higher. Raw bytes via
//!   `application/octet-stream`, decoded by dtype (`b1`, `u1/2/4`,
//!   `i1/2/4`, `f4/8`), normalised lin or `log1p`, and drawn through a
//!   256-entry colormap LUT into `<canvas>`.
//!
//! Stack dims (anything beyond the image plane) get a slider each; the
//! cut indices are appended to the `slice` query string.

use leptos::ev;
use leptos::html::Canvas;
use leptos::prelude::*;
use leptos::task::spawn_local;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Blob, BlobPropertyBag, CanvasRenderingContext2d, ImageData, Url};

use super::colormaps::{ALL as ALL_CMAPS, ColormapName};
use crate::auth::AuthState;
use crate::auth::use_auth;

const MAX_IMAGE_SIZE: usize = 1024;

/// Subset of the resource structure we actually need. We keep it
/// loose (`shape: Vec<usize>`, optional `data_type`) so unknown fields
/// don't break parsing — `serde_json::Value` would also work, but a
/// typed view is easier to reason about.
#[derive(Clone, Debug)]
pub struct ArrayInfo {
    pub link: String,
    pub shape: Vec<usize>,
    pub kind: char,
    pub itemsize: usize,
}

impl ArrayInfo {
    /// Build from a metadata envelope: `attributes.structure` carries
    /// `shape` and `data_type`; `links.full` (or `links.block` as a
    /// fallback) is the URL we fetch from.
    pub fn from_resource(
        structure: &serde_json::Value,
        links: &serde_json::Value,
    ) -> Option<Self> {
        let shape = structure
            .get("shape")?
            .as_array()?
            .iter()
            .map(|v| v.as_u64().map(|n| n as usize))
            .collect::<Option<Vec<_>>>()?;
        let dt = structure.get("data_type");
        let kind = dt
            .and_then(|d| d.get("kind"))
            .and_then(|k| k.as_str())
            .and_then(|s| s.chars().next())
            .unwrap_or('f');
        let itemsize = dt
            .and_then(|d| d.get("itemsize"))
            .and_then(|v| v.as_u64())
            .unwrap_or(4) as usize;
        let link = links
            .get("full")
            .and_then(|v| v.as_str())
            .or_else(|| links.get("block").and_then(|v| v.as_str()))?
            .to_string();
        Some(Self {
            link,
            shape,
            kind,
            itemsize,
        })
    }

    fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// `true` for shapes ending in `3` or `4` (RGB / RGBA).
    fn is_color(&self) -> bool {
        self.ndim() >= 3
            && matches!(self.shape.last(), Some(3) | Some(4))
    }

    /// How many trailing dims belong to the image plane (2 grayscale, 3 colour).
    fn image_dims(&self) -> usize {
        if self.is_color() { 3 } else { 2 }
    }

    fn stack_dims(&self) -> usize {
        self.ndim().saturating_sub(self.image_dims())
    }

    fn spatial_dims(&self) -> &[usize] {
        let start = self.stack_dims();
        let end = self.stack_dims() + 2; // first 2 spatial dims (H, W)
        &self.shape[start..end]
    }
}

#[component]
pub fn ArrayView(info: ArrayInfo) -> impl IntoView {
    if info.ndim() < 2 {
        return view! {
            <p class="text-slate-600 text-sm">
                "Array viewer currently supports 2-D and higher (got "
                {info.ndim()} "-D)."
            </p>
        }
        .into_any();
    }

    let stack_dims = info.stack_dims();
    let cuts: RwSignal<Vec<usize>> = RwSignal::new(
        info.shape[..stack_dims]
            .iter()
            .map(|d| d / 2)
            .collect(),
    );
    let colormap = RwSignal::new(ColormapName::Gray);
    let log_scale = RwSignal::new(false);

    // Stride: shrink the image so the longest axis fits MAX_IMAGE_SIZE.
    let max_spatial = info.spatial_dims().iter().copied().max().unwrap_or(1);
    let stride = max_spatial.div_ceil(MAX_IMAGE_SIZE).max(1);

    let info_for_url = info.clone();
    let url = Memo::new(move |_| {
        build_slice_url(&info_for_url, &cuts.get(), stride)
    });

    let color = info.is_color();
    let view_for_image = if color {
        view! { <PngView url=Signal::derive(move || url.get()) /> }.into_any()
    } else {
        let info = info.clone();
        view! {
            <CanvasView
                info=info
                url=Signal::derive(move || url.get())
                stride=stride
                colormap=Signal::derive(move || colormap.get())
                log_scale=Signal::derive(move || log_scale.get())
            />
        }
        .into_any()
    };

    let info_for_sliders = info.clone();
    let sliders_view = if stack_dims > 0
        && info.shape[..stack_dims].iter().any(|d| *d > 1)
    {
        view! {
            <StackSliders
                info=info_for_sliders.clone()
                cuts=cuts
            />
        }
        .into_any()
    } else {
        ().into_any()
    };

    let controls_view = if !color {
        view! { <GrayscaleControls colormap=colormap log_scale=log_scale /> }.into_any()
    } else {
        ().into_any()
    };

    view! {
        <div class="space-y-3">
            {view_for_image}
            {controls_view}
            {sliders_view}
        </div>
    }
    .into_any()
}

fn build_slice_url(info: &ArrayInfo, cuts: &[usize], stride: usize) -> String {
    let mut parts: Vec<String> = cuts.iter().map(|c| c.to_string()).collect();
    if stride != 1 {
        parts.push(format!("::{stride}"));
        parts.push(format!("::{stride}"));
        if info.is_color() {
            parts.push(":".to_string());
        }
    }
    let format = if info.is_color() {
        "image/png"
    } else {
        "application/octet-stream"
    };
    if parts.is_empty() {
        format!("{}?format={}", info.link, format)
    } else {
        format!(
            "{}?format={}&slice={}",
            info.link,
            format,
            parts.join(",")
        )
    }
}

#[component]
fn StackSliders(info: ArrayInfo, cuts: RwSignal<Vec<usize>>) -> impl IntoView {
    let stack_dims = info.stack_dims();
    let dims: Vec<(usize, usize)> = (0..stack_dims)
        .map(|i| (i, info.shape[i]))
        .filter(|(_, sz)| *sz > 1)
        .collect();
    view! {
        <div class="space-y-2">
            <p class="text-xs text-slate-500">
                "Choose a planar cut through this " {info.ndim()} "-D array."
            </p>
            {dims.into_iter().map(|(i, sz)| {
                view! {
                    <div class="flex items-center gap-2 text-sm">
                        <span class="text-slate-500 w-12 font-mono">
                            "dim " {i}
                        </span>
                        <input
                            type="range"
                            min="0"
                            max=(sz - 1).to_string()
                            value=move || cuts.with(|c| c.get(i).copied().unwrap_or(0).to_string())
                            on:input=move |ev: ev::Event| {
                                let v: usize = event_target_value(&ev).parse().unwrap_or(0);
                                cuts.update(|c| { c[i] = v; });
                            }
                            class="flex-1"
                        />
                        <span class="font-mono text-slate-700 w-12 text-right">
                            {move || cuts.with(|c| c.get(i).copied().unwrap_or(0))}
                            "/" {sz - 1}
                        </span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

#[component]
fn GrayscaleControls(
    colormap: RwSignal<ColormapName>,
    log_scale: RwSignal<bool>,
) -> impl IntoView {
    view! {
        <div class="flex items-center gap-4 text-sm">
            <label class="flex items-center gap-2">
                <span class="text-slate-500">"Colormap"</span>
                <select
                    on:change=move |ev| {
                        if let Some(c) = ColormapName::from_str(&event_target_value(&ev)) {
                            colormap.set(c);
                        }
                    }
                    class="rounded border border-slate-300 px-2 py-1 text-sm"
                >
                    {ALL_CMAPS.iter().map(|c| {
                        let cur = *c;
                        view! {
                            <option
                                value=cur.as_slug()
                                selected=move || colormap.get() == cur
                            >
                                {cur.label()}
                            </option>
                        }
                    }).collect::<Vec<_>>()}
                </select>
            </label>
            <label class="flex items-center gap-2">
                <input
                    type="checkbox"
                    prop:checked=move || log_scale.get()
                    on:change=move |ev| log_scale.set(event_target_checked(&ev))
                />
                <span class="text-slate-500">"Log scale"</span>
            </label>
        </div>
    }
}

#[component]
fn PngView(url: Signal<String>) -> impl IntoView {
    let auth = use_auth();
    let blob_url = RwSignal::new(None::<String>);

    Effect::new(move |prev: Option<Option<String>>| {
        // Revoke the previous object URL before kicking off a new fetch.
        if let Some(Some(prev_url)) = prev {
            let _ = Url::revoke_object_url(&prev_url);
        }
        let target = url.get();
        spawn_local(fetch_image_blob(auth, target, blob_url));
        blob_url.get_untracked()
    });

    view! {
        <Show
            when=move || blob_url.get().is_some()
            fallback=|| view! { <p class="text-slate-500 text-sm">"loading image..."</p> }
        >
            <img
                src=move || blob_url.get().unwrap_or_default()
                alt="Data rendered"
                class="block w-full max-h-[60vh] object-contain"
            />
        </Show>
    }
}

async fn fetch_image_blob(
    auth: AuthState,
    url: String,
    blob_url: RwSignal<Option<String>>,
) {
    match crate::api::fetch_bytes(&auth, &url).await {
        Ok(bytes) => {
            if let Some(obj) = bytes_to_object_url(&bytes, "image/png") {
                blob_url.set(Some(obj));
            }
        }
        Err(_) => blob_url.set(None),
    }
}

fn bytes_to_object_url(bytes: &[u8], mime: &str) -> Option<String> {
    let arr = js_sys::Uint8Array::from(bytes);
    let parts = js_sys::Array::new();
    parts.push(&arr.buffer());
    let opts = BlobPropertyBag::new();
    opts.set_type(mime);
    let blob = Blob::new_with_u8_array_sequence_and_options(&parts, &opts).ok()?;
    Url::create_object_url_with_blob(&blob).ok()
}

#[component]
fn CanvasView(
    info: ArrayInfo,
    url: Signal<String>,
    stride: usize,
    colormap: Signal<ColormapName>,
    log_scale: Signal<bool>,
) -> impl IntoView {
    let canvas_ref: NodeRef<Canvas> = NodeRef::new();
    let auth = use_auth();
    let info_for_eff = info.clone();

    Effect::new(move |_| {
        let target = url.get();
        let cmap = colormap.get();
        let log = log_scale.get();
        let info = info_for_eff.clone();
        let canvas_ref = canvas_ref;
        spawn_local(async move {
            // Subscribe to canvas readiness — Effect re-runs when the
            // node is mounted because `canvas_ref.get()` reads the
            // signal. But spawn_local runs detached, so re-read via
            // get_untracked below. We rely on the parent Effect to
            // re-trigger on signal updates; on first mount the node
            // is already attached.
            let Some(canvas_node) = canvas_ref.get_untracked() else {
                return;
            };
            let canvas: web_sys::HtmlCanvasElement = match canvas_node
                .clone()
                .unchecked_into::<JsValue>()
                .dyn_into::<web_sys::HtmlCanvasElement>()
            {
                Ok(c) => c,
                Err(_) => return,
            };
            let bytes = match crate::api::fetch_bytes(&auth, &target).await {
                Ok(b) => b,
                Err(_) => return,
            };
            let (h, w) = canvas_dims(&info, stride);
            if h == 0 || w == 0 {
                return;
            }
            canvas.set_width(w as u32);
            canvas.set_height(h as u32);
            let Some(ctx) = canvas
                .get_context("2d")
                .ok()
                .flatten()
                .and_then(|o| o.dyn_into::<CanvasRenderingContext2d>().ok())
            else {
                return;
            };
            if let Some(rgba) = decode_to_rgba(&bytes, &info, cmap, log, w, h) {
                let clamped = wasm_bindgen::Clamped(rgba.as_slice());
                if let Ok(image_data) =
                    ImageData::new_with_u8_clamped_array_and_sh(clamped, w as u32, h as u32)
                {
                    let _ = ctx.put_image_data(&image_data, 0.0, 0.0);
                }
            }
        });
    });

    view! {
        <canvas
            node_ref=canvas_ref
            class="block w-full max-h-[60vh] object-contain bg-slate-100"
        />
    }
}

fn canvas_dims(info: &ArrayInfo, stride: usize) -> (usize, usize) {
    let stack = info.stack_dims();
    let h = info.shape.get(stack).copied().unwrap_or(0).div_ceil(stride.max(1));
    let w = info.shape.get(stack + 1).copied().unwrap_or(0).div_ceil(stride.max(1));
    (h, w)
}

fn decode_to_rgba(
    bytes: &[u8],
    info: &ArrayInfo,
    cmap: ColormapName,
    log_scale: bool,
    w: usize,
    h: usize,
) -> Option<Vec<u8>> {
    let n = w.checked_mul(h)?;
    if n == 0 {
        return None;
    }
    let raw = decode_typed(bytes, info, n)?;
    let (lo, hi) = finite_min_max(&raw);
    let mut hi = hi;
    if (hi - lo).abs() < f64::EPSILON {
        hi = lo + 1.0;
    }
    let lut = cmap.lut();
    let mut rgba = vec![0u8; n * 4];
    let denom = if log_scale {
        (1.0 + (hi - lo)).ln()
    } else {
        hi - lo
    };
    let denom = if denom.abs() < f64::EPSILON { 1.0 } else { denom };
    for i in 0..n {
        let v = raw[i];
        if !v.is_finite() {
            // Skip non-finite values (shows as transparent).
            rgba[i * 4 + 3] = 0;
            continue;
        }
        let normalised = if log_scale {
            (1.0 + (v - lo)).ln() / denom
        } else {
            (v - lo) / denom
        };
        let idx = normalised.clamp(0.0, 1.0) * 255.0;
        let idx = idx.round().clamp(0.0, 255.0) as usize;
        let [r, g, b] = lut[idx];
        rgba[i * 4] = r;
        rgba[i * 4 + 1] = g;
        rgba[i * 4 + 2] = b;
        rgba[i * 4 + 3] = 255;
    }
    Some(rgba)
}

fn decode_typed(bytes: &[u8], info: &ArrayInfo, expected_len: usize) -> Option<Vec<f64>> {
    // Boolean (`b1`) is upstream-emitted as a single byte per element.
    match (info.kind, info.itemsize) {
        ('b', 1) | ('u', 1) => Some(bytes.iter().map(|&b| b as f64).collect()),
        ('i', 1) => Some(bytes.iter().map(|&b| (b as i8) as f64).collect()),
        ('u', 2) => decode_le::<u16>(bytes, expected_len, |x| x as f64),
        ('i', 2) => decode_le::<i16>(bytes, expected_len, |x| x as f64),
        ('u', 4) => decode_le::<u32>(bytes, expected_len, |x| x as f64),
        ('i', 4) => decode_le::<i32>(bytes, expected_len, |x| x as f64),
        ('u', 8) => decode_le::<u64>(bytes, expected_len, |x| x as f64),
        ('i', 8) => decode_le::<i64>(bytes, expected_len, |x| x as f64),
        ('f', 4) => decode_le::<f32>(bytes, expected_len, |x| x as f64),
        ('f', 8) => decode_le::<f64>(bytes, expected_len, |x| x),
        _ => None,
    }
}

fn decode_le<T: LeFromBytes>(
    bytes: &[u8],
    expected_len: usize,
    cast: fn(T) -> f64,
) -> Option<Vec<f64>> {
    let stride = std::mem::size_of::<T>();
    if bytes.len() < expected_len * stride {
        return None;
    }
    let mut out = Vec::with_capacity(expected_len);
    for i in 0..expected_len {
        let chunk = &bytes[i * stride..(i + 1) * stride];
        out.push(cast(T::from_le_bytes(chunk)?));
    }
    Some(out)
}

trait LeFromBytes: Sized {
    fn from_le_bytes(bytes: &[u8]) -> Option<Self>;
}

macro_rules! impl_le {
    ($($t:ty),*) => {$(
        impl LeFromBytes for $t {
            fn from_le_bytes(bytes: &[u8]) -> Option<Self> {
                let arr: [u8; std::mem::size_of::<$t>()] = bytes.try_into().ok()?;
                Some(<$t>::from_le_bytes(arr))
            }
        }
    )*};
}
impl_le!(u16, u32, u64, i16, i32, i64, f32, f64);

fn finite_min_max(values: &[f64]) -> (f64, f64) {
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for &v in values {
        if v.is_finite() {
            if v < lo {
                lo = v;
            }
            if v > hi {
                hi = v;
            }
        }
    }
    if !lo.is_finite() {
        return (0.0, 1.0);
    }
    (lo, hi)
}

fn event_target_checked(ev: &ev::Event) -> bool {
    use wasm_bindgen::JsCast;
    ev.target()
        .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
        .map(|el| el.checked())
        .unwrap_or(false)
}

// Re-export so other modules don't need to know about wasm-bindgen plumbing.
pub use std::convert::TryInto as _TryInto;

// Anchor for `JsValue` when not used directly (kept to silence unused-import
// warnings if the path is referenced under conditional cfg).
#[allow(dead_code)]
fn _js_value_anchor() -> Option<JsValue> {
    None
}

#[allow(dead_code)]
async fn _jsfuture_anchor(p: js_sys::Promise) -> Result<JsValue, JsValue> {
    JsFuture::from(p).await
}
