#!/usr/bin/env bash
# Fail if the pinned SBE schemas drifted (e.g. the submodule moved off 481d4271ad).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root/third_party/aeron"

expected=$(cat <<'EOF'
4aabaab96e883bca597a01e62b40822ff8008d327d1b6df554da809ae041cc7d  aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml
bf17f9189303a50b9318d23a0d61dc88a414b059bdc39be8f49e1b1e092f7b01  aeron-archive/src/main/resources/archive/aeron-archive-mark-codecs.xml
856fc6a99cdb7a7562ea04d2161cec900c1778f47b119cf3bcb71a16ea3f73a7  aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml
05c2df9574ab216da3230f8b66525298de06183dee2b377e28599fd6fdf62df0  aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml
a9223a950405aa0a2ebd4bb06257c6b094a607dd0e2081182028126531e62279  aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml
EOF
)

echo "$expected" | sha256sum -c -

pin=$(git rev-parse HEAD)
if [ "$pin" != "481d4271ad5d52db1ae080bfb29d6259f10566fb" ]; then
    echo "verify-schemas: submodule HEAD is $pin, expected 481d4271ad" >&2
    exit 1
fi
echo "verify-schemas: schemas match the pinned commit"
