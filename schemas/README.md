# SBE schemas

The five schemas are **not copied** into this repo. The single source of truth is
the pinned submodule; `src/sbe/CMakeLists.txt` generates C++ codecs from these
paths at build time (output: `build/<preset>/generated/axc/sbe/`, never committed).

| Schema | sha256 (commit `454da1e4e6`) |
|---|---|
| `third_party/aeron/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml` | `4aabaab96e883bca597a01e62b40822ff8008d327d1b6df554da809ae041cc7d` |
| `third_party/aeron/aeron-archive/src/main/resources/archive/aeron-archive-mark-codecs.xml` | `bf17f9189303a50b9318d23a0d61dc88a414b059bdc39be8f49e1b1e092f7b01` |
| `third_party/aeron/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` | `3acfed549325f4970064a54d1ea0374768f248e53d66ac5fb7ce54cbade3badb` |
| `third_party/aeron/aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml` | `05c2df9574ab216da3230f8b66525298de06183dee2b377e28599fd6fdf62df0` |
| `third_party/aeron/aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml` | `a9223a950405aa0a2ebd4bb06257c6b094a607dd0e2081182028126531e62279` |

`scripts/verify-schemas.sh` re-hashes them and fails if the submodule moved off
`454da1e4e6`.
