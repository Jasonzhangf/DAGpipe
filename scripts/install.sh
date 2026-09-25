#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
user_home=${HOME:-${USERPROFILE:-}}
if [ -z "$user_home" ]; then
    echo "cannot determine the user home directory" >&2
    exit 1
fi
skill_source="$repo_dir/.agents/skills/dagpipe-runtime/SKILL.md"
skill_target="$user_home/.agents/skills/dagpipe-runtime/SKILL.md"
sdk_target="$user_home/.local/share/dagpipe/sdk"
if [ -L "$sdk_target" ]; then
    echo "SDK path is a symlink at $sdk_target; refusing to follow it" >&2
    exit 1
fi
if [ -e "$sdk_target" ]; then
    if [ ! -d "$sdk_target" ] || [ ! -f "$sdk_target/Cargo.toml" ] || [ ! -d "$sdk_target/src" ]; then
        echo "existing SDK path is not a DAGpipe SDK directory at $sdk_target; refusing to overwrite it" >&2
        exit 1
    fi
    existing_metadata=$(cargo metadata --no-deps --format-version 1 --manifest-path "$sdk_target/Cargo.toml")
    case "$existing_metadata" in
        *'"name":"pipeline_runtime"'*'"repository":"https://github.com/Jasonzhangf/DAGpipe"'*) ;;
        *)
            echo "existing SDK manifest is not pipeline_runtime at $sdk_target; refusing to overwrite it" >&2
            exit 1
            ;;
    esac
    if [ -L "$sdk_target/Cargo.toml" ] || [ -L "$sdk_target/src" ] || [ -L "$sdk_target/.agents" ]; then
        echo "existing SDK contains a symlinked copy target at $sdk_target; refusing to follow it" >&2
        exit 1
    fi
    if { [ -d "$sdk_target/src" ] && find "$sdk_target/src" -type l -print -quit | grep -q .; } || \
        { [ -d "$sdk_target/.agents" ] && find "$sdk_target/.agents" -type l -print -quit | grep -q .; }; then
        echo "existing SDK contains a symlink in a copy tree at $sdk_target; refusing to follow it" >&2
        exit 1
    fi
fi
if [ -L "$skill_target" ]; then
    echo "skill path is a symlink at $skill_target; refusing to follow it" >&2
    exit 1
fi
if [ -e "$skill_target" ]; then
    if ! cmp -s "$skill_source" "$skill_target"; then
        installed_name=$(head -n 2 "$skill_target" | tail -n 1 || true)
        if [ "$installed_name" != "name: dagpipe-runtime" ]; then
            echo "existing Skill at $skill_target is not DAGpipe Runtime; refusing overwrite" >&2
            exit 1
        fi
    fi
fi
cargo_install_root=${CARGO_INSTALL_ROOT:-${CARGO_HOME:-"$user_home/.cargo"}}
cargo install --path "$repo_dir" --locked --force --root "$cargo_install_root"
mkdir -p "$sdk_target/src" "$sdk_target/.agents/skills/dagpipe-runtime"
cp "$repo_dir/Cargo.toml" "$sdk_target/Cargo.toml"
cp -R "$repo_dir/src/." "$sdk_target/src/"
cp "$skill_source" "$sdk_target/.agents/skills/dagpipe-runtime/SKILL.md"
"$cargo_install_root/bin/dagpipe" skill install
