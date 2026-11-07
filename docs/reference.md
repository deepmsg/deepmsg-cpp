# Binaries and their Java equivalents

| Binary | Java main | Notes |
|---|---|---|
| `aeron-archive-server --embedded-driver` | `io.aeron.archive.ArchivingMediaDriver` | embedded C driver + archive |
| `aeron-archive-server` | `io.aeron.archive.Archive` | standalone archive |
| `aeron-cluster-node` | `io.aeron.cluster.ClusteredMediaDriver` | driver + archive + CM (+ container) |
| `aeron-consensus-module` | `io.aeron.cluster.ConsensusModule` | CM only |
| `aeron-clustered-service-container` | `io.aeron.cluster.service.ClusteredServiceContainer` | container only |
| `aeron-cluster-backup` | `io.aeron.cluster.ClusterBackup` / `ClusterBackupMediaDriver` | XC-5/C9 |
| `cluster-tool` | `io.aeron.cluster.ClusterTool` | 18 commands (T2) |
| `archive-tool-ro` | `io.aeron.archive.ArchiveTool` read-only subset | T1 |

Same process never means a shortcut: archive, CM and container interact only
through Aeron IPC and counters, exactly as in the reference.
