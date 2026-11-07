# third_party

| Dependency | Kind | Pin | License | Notes |
|---|---|---|---|---|
| aeron | git submodule | `454da1e4e6` (tag `1.54.0`) | Apache-2.0 | Our fork `github.com/deepmsg/aeron` (upstream mirror). **Never patched**; the gitlink is the contract. The fork carries no `1.54.0` tag, so the commit id is the pin. |
| sbe-tool | downloaded jar | `sbe-all-1.42.0.jar` | Apache-2.0 | The version in Aeron 1.54.0's `gradle/libs.versions.toml`. Not committed; `scripts/fetch-sbe.sh` verifies sha256 `f60becdb567519a2ed3824ffe285f93adb2159b39ede0b234fc8b1f13736236a`. |
| googletest | FetchContent | tag `v1.14.0` | BSD-3-Clause | test/ |
| google benchmark | FetchContent | tag `v1.8.3` | Apache-2.0 | bench/ (only when `AXC_BUILD_BENCHMARKS=ON`) |
| liburing | system package | `liburing-dev` | MIT | Optional; `AXC_WITH_URING=OFF` builds without it. |

Adding a dependency requires a line here and an ADR under `docs/adr/`.
