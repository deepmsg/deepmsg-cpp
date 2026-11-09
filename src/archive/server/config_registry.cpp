#include "archive/server/config_registry.h"

#include <array>

namespace deepmsg::archive::server
{

namespace
{

constexpr ConfigDisposition HONOURED = ConfigDisposition::honoured;
constexpr ConfigDisposition IGNORED = ConfigDisposition::accepted_ignored;

/// The X0-6 table (`records/x0-6/config-names.tsv`), 1.54.0: 48 names, in it
/// alphabetical order.
constexpr std::array<ConfigEntry, 48> ENTRIES{{
    {"aeron.archive.authenticator.supplier", HONOURED, "built-in authenticator names"},
    {"aeron.archive.authorisation.service.supplier", IGNORED,
     "no AuthorisationService; the authenticator is the only gate"},
    {"aeron.archive.catalog.capacity", HONOURED, "catalog capacity"},
    {"aeron.archive.catalog.file.sync.level", HONOURED, "catalog fsync level"},
    {"aeron.archive.client.name", IGNORED,
     "AeronArchive client setting; the archive's own client name derives from the archive id"},
    {"aeron.archive.conductor.cpu.affinity", HONOURED, "DEDICATED conductor thread"},
    {"aeron.archive.conductor.cycle.threshold", HONOURED, "duty-cycle counter 104"},
    {"aeron.archive.connect.timeout", HONOURED, "control session connect timeout"},
    {"aeron.archive.control.channel", HONOURED, "control subscription"},
    {"aeron.archive.control.channel.enabled", HONOURED, "false leaves only the local control channel"},
    {"aeron.archive.control.mtu.length", HONOURED, "control response channel mtu"},
    {"aeron.archive.control.response.channel", HONOURED, "default response channel for replication"},
    {"aeron.archive.control.response.stream.id", HONOURED, "response stream for replication"},
    {"aeron.archive.control.stream.id", HONOURED, "control stream"},
    {"aeron.archive.control.term.buffer.length", HONOURED, "control response channel term length"},
    {"aeron.archive.control.term.buffer.sparse", HONOURED, "control response channel sparse"},
    {"aeron.archive.dir", HONOURED, "archive directory"},
    {"aeron.archive.dir.delete.on.start", HONOURED, "delete on start"},
    {"aeron.archive.error.buffer.length", HONOURED, "mark file error buffer"},
    {"aeron.archive.file.io.max.length", HONOURED, "recording block length and replay buffer"},
    {"aeron.archive.file.sync.level", HONOURED, "recording fsync level"},
    {"aeron.archive.id", HONOURED, "archive id"},
    {"aeron.archive.idle.strategy", HONOURED, "conductor (and the recorder/replayer fallback)"},
    {"aeron.archive.local.control.channel", HONOURED, "local control subscription"},
    {"aeron.archive.local.control.stream.id", HONOURED, "local control stream"},
    {"aeron.archive.low.storage.space.threshold", HONOURED, "recording start guard"},
    {"aeron.archive.mark.file.dir", HONOURED, "mark file directory"},
    {"aeron.archive.max.catalog.entries", IGNORED,
     "deprecated alias from before 1.0; Archive reads catalog.capacity only"},
    {"aeron.archive.max.concurrent.recordings", HONOURED, "recording limit"},
    {"aeron.archive.max.concurrent.replays", HONOURED, "replay limit"},
    {"aeron.archive.message.retry.attempts", HONOURED, "the archive's archive-client retries"},
    {"aeron.archive.message.timeout", HONOURED, "the archive's archive-client timeout"},
    {"aeron.archive.record.checksum", IGNORED,
     "record-side checksum is not implemented (replay.checksum is); recorded in docs/compat.md"},
    {"aeron.archive.recorder.cpu.affinity", HONOURED, "DEDICATED recorder thread"},
    {"aeron.archive.recorder.cycle.threshold", HONOURED, "duty-cycle counter 104"},
    {"aeron.archive.recorder.idle.strategy", HONOURED, "DEDICATED recorder thread"},
    {"aeron.archive.recording.events.channel", HONOURED, "recording events publication"},
    {"aeron.archive.recording.events.enabled", HONOURED, "recording events publication"},
    {"aeron.archive.recording.events.stream.id", HONOURED, "recording events publication"},
    {"aeron.archive.replay.checksum", HONOURED, "replay verification"},
    {"aeron.archive.replay.linger.timeout", HONOURED, "replay publication linger"},
    {"aeron.archive.replayer.cpu.affinity", HONOURED, "DEDICATED replayer thread"},
    {"aeron.archive.replayer.cycle.threshold", HONOURED, "duty-cycle counter 104"},
    {"aeron.archive.replayer.idle.strategy", HONOURED, "DEDICATED replayer thread"},
    {"aeron.archive.replication.channel", HONOURED, "replicate request fallback channel"},
    {"aeron.archive.segment.file.length", HONOURED, "segment length"},
    {"aeron.archive.session.liveness.check.interval", HONOURED, "control session liveness"},
    {"aeron.archive.threading.mode", HONOURED, "INVOKER / SHARED / DEDICATED"},
}};

}  // namespace

std::span<const ConfigEntry> archive_config_names() noexcept
{
    return ENTRIES;
}

const ConfigEntry* find_archive_config(std::string_view name) noexcept
{
    for (const ConfigEntry& entry : ENTRIES)
    {
        if (entry.name == name)
        {
            return &entry;
        }
    }
    return nullptr;
}

std::string_view disposition_name(ConfigDisposition disposition) noexcept
{
    return ConfigDisposition::honoured == disposition ? "honoured" : "accepted_ignored";
}

}  // namespace deepmsg::archive::server
