#!/usr/bin/env bash
# Fail if the pinned SBE schemas drifted (e.g. the submodule moved off 454da1e4e6).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root/third_party/aeron"

expected=$(cat <<'EOF'
4aabaab96e883bca597a01e62b40822ff8008d327d1b6df554da809ae041cc7d  aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml
bf17f9189303a50b9318d23a0d61dc88a414b059bdc39be8f49e1b1e092f7b01  aeron-archive/src/main/resources/archive/aeron-archive-mark-codecs.xml
3acfed549325f4970064a54d1ea0374768f248e53d66ac5fb7ce54cbade3badb  aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml
05c2df9574ab216da3230f8b66525298de06183dee2b377e28599fd6fdf62df0  aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml
a9223a950405aa0a2ebd4bb06257c6b094a607dd0e2081182028126531e62279  aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml
EOF
)

echo "$expected" | sha256sum -c -

pin=$(git rev-parse HEAD)
if [ "$pin" != "454da1e4e6c7edf3dd6d34878512d3243384992a" ]; then
    echo "verify-schemas: submodule HEAD is $pin, expected 454da1e4e6" >&2
    exit 1
fi
echo "verify-schemas: schemas match the pinned commit"
