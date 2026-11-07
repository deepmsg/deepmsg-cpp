# Public headers

Only two things are exported/installed; everything else under `src/` is internal.

- `aeron_cluster_client.h` — C ABI for the cluster client (shape modelled on
  `aeron_archive_*`), delivered by C3b.
- `aeron/cluster/service/` — C++ business API: `clustered_service.h` (concept),
  `cluster.h` and `client_session.h`, delivered by C2a.

They are forwarding headers over the internal implementations; signatures are
frozen by `analysis/cpp/modules-2026-11-07-cpp-interfaces.md` §5.4–§5.5.
