#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_kind=${1:-patch}
do_bump=0

if [ "$#" -gt 1 ]; then
    echo "usage: scripts/release.sh [initial|patch|minor|major]" >&2
    exit 2
fi

case "$release_kind" in
    initial)
        package_id=$(cargo pkgid --manifest-path "$repo_dir/Cargo.toml")
        case "$package_id" in
            *"#pipeline_runtime@0.1.0") ;;
            *)
                echo "initial release must remain pipeline_runtime 0.1.0; found $package_id" >&2
                exit 1
                ;;
        esac
        ;;
    patch|minor|major)
        if ! cargo set-version --help >/dev/null 2>&1; then
            echo "release bump requires cargo-edit; install it with: cargo install cargo-edit --locked" >&2
            exit 1
        fi
        do_bump=1
        ;;
    *)
        echo "usage: scripts/release.sh [initial|patch|minor|major]" >&2
        exit 2
        ;;
esac

if [ "$do_bump" -eq 1 ]; then
    cargo set-version --manifest-path "$repo_dir/Cargo.toml" --bump "$release_kind"
fi
cargo fmt --manifest-path "$repo_dir/Cargo.toml" --all -- --check
cargo test --manifest-path "$repo_dir/Cargo.toml" --locked --all-targets
cargo clippy --manifest-path "$repo_dir/Cargo.toml" --locked --all-targets -- -D warnings
cargo build --manifest-path "$repo_dir/Cargo.toml" --locked --release --all-targets
cargo doc --manifest-path "$repo_dir/Cargo.toml" --locked --no-deps
"$repo_dir/scripts/install.sh"

echo "Local release build and install complete: $(cargo pkgid --manifest-path "$repo_dir/Cargo.toml")"
