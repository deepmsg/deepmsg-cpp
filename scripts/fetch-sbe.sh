#!/usr/bin/env bash
# Download the SBE tool used by Aeron 1.54.0 (gradle/libs.versions.toml: sbe = 1.42.0).
# The jar is not vendored; CMake resolves third_party/sbe-tool/sbe-all-*.jar.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
version=1.42.0
sha256=f60becdb567519a2ed3824ffe285f93adb2159b39ede0b234fc8b1f13736236a
dest="$root/third_party/sbe-tool/sbe-all-$version.jar"
url="https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/$version/sbe-all-$version.jar"

if [ -f "$dest" ] && echo "$sha256  $dest" | sha256sum -c --status -; then
    echo "sbe-all $version already present and verified: $dest"
    exit 0
fi

mkdir -p "$(dirname "$dest")"
echo "downloading $url"
curl -fsSL -o "$dest" "$url"
echo "$sha256  $dest" | sha256sum -c -
