# Fuzz targets (nightly)

libFuzzer targets for the inbound protocol surfaces. Every decode must go
through `axc::sbe::decode_guarded` (src/sbe/decode_guarded.h).

Planned: `archive_control_fuzz` (A2), `cluster_ingress_fuzz` (C4),
`recording_log_fuzz` (C1).
