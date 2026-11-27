#!/usr/bin/env bash
# Download the SBE tool used by Aeron 1.54.2 (gradle/libs.versions.toml: sbe = 1.42.0).
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
# Maven Central answers 429 to a share of the CI runners' requests (2026-11-18:
# two gate jobs died here, while the same URL from another runner succeeded
# minutes later). curl treats 429 as transient, so retry it with backoff; the
# workflow also caches the jar, which keeps the cache-hit path off the network.
curl -fsSL --retry 5 --retry-delay 3 --retry-max-time 120 -o "$dest" "$url"
echo "$sha256  $dest" | sha256sum -c -
