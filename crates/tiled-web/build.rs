//! Drive the WASM SPA build at host-crate compile time.
//!
//! `assets/spa/` is gitignored — populated fresh on every build:
//!   * if `trunk` is on PATH and the sibling `tiled-web-spa/` crate
//!     exists, run `trunk build --release` and copy `dist/*` into
//!     `assets/spa/`;
//!   * otherwise copy the committed `assets/spa-placeholder/` (a static
//!     HTML/CSS shell) into `assets/spa/`.
//!
//! Either way, `rust-embed` finds a populated directory at compile
//! time. Set `TILED_SKIP_SPA_BUILD=1` to bypass — useful in CI when the
//! bundle is already pre-staged in `assets/spa/`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    if std::env::var_os("TILED_SKIP_SPA_BUILD").is_some() {
        println!("cargo:warning=TILED_SKIP_SPA_BUILD set; reusing existing assets/spa/");
        return;
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let spa_crate = manifest_dir.join("..").join("tiled-web-spa");
    let dst = manifest_dir.join("assets").join("spa");
    let placeholder = manifest_dir.join("assets").join("spa-placeholder");

    println!("cargo:rerun-if-changed=assets/spa-placeholder");
    println!("cargo:rerun-if-env-changed=TILED_SKIP_SPA_BUILD");
    if spa_crate.exists() {
        println!("cargo:rerun-if-changed=../tiled-web-spa/Cargo.toml");
        println!("cargo:rerun-if-changed=../tiled-web-spa/index.html");
        println!("cargo:rerun-if-changed=../tiled-web-spa/Trunk.toml");
        println!("cargo:rerun-if-changed=../tiled-web-spa/style/app.css");
        println!("cargo:rerun-if-changed=../tiled-web-spa/src");
    }

    let trunk_ok = spa_crate.exists()
        && Command::new("trunk")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

    if trunk_ok {
        let status = Command::new("trunk")
            .args(["build", "--release"])
            .current_dir(&spa_crate)
            .status()
            .expect("trunk build");
        if !status.success() {
            panic!("trunk build failed (status={status})");
        }
        let dist = spa_crate.join("dist");
        wipe_and_copy(&dist, &dst).expect("copy dist -> assets/spa");
    } else {
        if spa_crate.exists() {
            println!(
                "cargo:warning=trunk not in PATH; falling back to assets/spa-placeholder/"
            );
        }
        wipe_and_copy(&placeholder, &dst).expect("copy spa-placeholder -> assets/spa");
    }
}

fn wipe_and_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    if dst.exists() {
        fs::remove_dir_all(dst)?;
    }
    fs::create_dir_all(dst)?;
    copy_dir_recursive(src, dst)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}
