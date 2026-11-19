#!/bin/sh
# Test runners must pass the real baked version, including reused SHA images.
image_test_version() {
    image_version=$(docker image inspect "$1" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}') || return 1
    if ! printf '%s\n' "$image_version" | LC_ALL=C grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$'; then
        echo 'image_version_label_missing_or_invalid' >&2
        return 1
    fi
    printf '%s\n' "$image_version"
}

# A tracked edit, staged edit or untracked file may change the built source.
image_test_build_version() {
    source_version=$(git -C "$1" rev-parse --short HEAD) || return 1
    source_status=$(git -C "$1" status --porcelain) || return 1
    if [ -n "$source_status" ]; then source_version="${source_version}-dirty"; fi
    printf '%s\n' "$source_version"
}
