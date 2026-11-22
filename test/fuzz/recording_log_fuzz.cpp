// Cluster persistent state: recording.log, node-state.dat, cluster-mark.dat.
//
// Every one of these files is mapped and parsed at startup, before any network
// traffic arrives. A corrupt file has to come back as a std::expected error (or
// a safe skip), never as an out-of-bounds read or a throw across the startup
// path.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/node_state_file.h"
#include "cluster/common/recording_log.h"
#include "core/io/distinct_error_log.h"
#include "fuzz_common.h"
#include "io_aeron_cluster_codecs_mark/MarkFileHeader.h"

namespace
{

using axc::fuzz::ShmSandbox;
using deepmsg::cluster::common::ClusterMarkFile;
using deepmsg::cluster::common::NodeStateFile;
using deepmsg::cluster::common::RecordingLog;
using RecordingExtent = RecordingLog::RecordingExtent;

constexpr std::size_t MAX_LOG_BYTES = 256U * 1024U;
constexpr std::size_t MAX_FILE_BYTES = 4U * 1024U * 1024U;

/// The golden mark file starts with the 8-byte SBE message header.
constexpr std::size_t MARK_MESSAGE_HEADER_LENGTH = 8;

std::vector<std::uint8_t> read_seed(const std::filesystem::path& path)
{
    std::ifstream stream{path, std::ios::binary};
    return std::vector<std::uint8_t>{std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

void write_seed(const std::filesystem::path& dir, const char* name, const std::vector<std::uint8_t>& bytes)
{
    std::FILE* file = std::fopen((dir / name).c_str(), "wb");
    if (nullptr != file)
    {
        (void)std::fwrite(bytes.data(), 1, bytes.size(), file);
        (void)std::fclose(file);
    }
}

/// A golden cluster-mark.dat whose 32-bit field at `offset` says INT32_MAX.
void emit_corrupt_mark(const std::filesystem::path& dir, const char* name, std::size_t offset)
{
    std::vector<std::uint8_t> bytes = read_seed(std::filesystem::path{AXC_PROJECT_SOURCE_DIR} /
                                                "test/golden/cluster/mark-file/consensus-module/cluster-mark.dat");
    if (bytes.size() < offset + sizeof(std::uint32_t))
    {
        return;
    }
    const std::uint32_t huge = 0x7FFFFFFFU;
    std::memcpy(bytes.data() + offset, &huge, sizeof(huge));
    write_seed(dir, name, bytes);
}

void exercise_recording_log(const ShmSandbox& sandbox, std::span<const std::uint8_t> bytes)
{
    if (bytes.size() > MAX_LOG_BYTES)
    {
        return;
    }
    (void)sandbox.write_file("recording.log", bytes);

    auto log = RecordingLog::open(sandbox.dir().string(), false);
    if (!log)
    {
        return;
    }

    for (const auto& entry : log->entries())
    {
        (void)entry.to_string();
    }
    (void)log->get_latest_snapshot(-1);

    // Both extent-source shapes: a plan that resolves every recording, and
    // one that resolves none (the "segment missing" path).
    auto present = [](std::int64_t) -> std::optional<RecordingExtent> { return RecordingExtent{}; };
    auto missing = [](std::int64_t) -> std::optional<RecordingExtent> { return std::nullopt; };
    (void)log->create_recovery_plan(present, 1, -1);
    (void)log->create_recovery_plan(missing, 1, -1);
    (void)log->invalidate_latest_snapshot();
}

void exercise_node_state(const ShmSandbox& sandbox, std::span<const std::uint8_t> bytes)
{
    if (bytes.size() < NodeStateFile::MINIMUM_FILE_LENGTH)
    {
        return;
    }
    (void)sandbox.write_file("node-state.dat", bytes);

    auto node_state = NodeStateFile::open(sandbox.dir().string(), false, 0);
    if (!node_state)
    {
        return;
    }
    (void)node_state->candidate_term();
    (void)node_state->semantic_version();
}

void exercise_cluster_mark(const ShmSandbox& sandbox, std::span<const std::uint8_t> bytes)
{
    if (bytes.size() < ClusterMarkFile::HEADER_LENGTH)
    {
        return;
    }
    const auto path = sandbox.write_file("cluster-mark.dat", bytes);

    auto mark = ClusterMarkFile::open_existing(path.string(), 0);
    if (!mark)
    {
        return;
    }
    (void)mark->pid();
    (void)mark->component_type();
    (void)mark->semantic_version();
    (void)mark->candidate_term_id();
    (void)mark->member_id();
    (void)mark->service_id();
    (void)mark->cluster_id();
    (void)mark->control_channel();
    (void)mark->aeron_directory();
    (void)axc::core::render_distinct_error_log(mark->error_buffer());
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    if (size == 0 || size > MAX_FILE_BYTES)
    {
        return 0;
    }
    static ShmSandbox sandbox{};
    const std::span<const std::uint8_t> bytes{data, size};

    exercise_recording_log(sandbox, bytes);
    exercise_node_state(sandbox, bytes);
    exercise_cluster_mark(sandbox, bytes);
    return 0;
}

namespace axc::fuzz
{

void emit_seeds(const std::filesystem::path& dir)
{
    // The golden files under test/golden/cluster are the corpus; these two add
    // the corrupt lengths the first ASan runs found.
    using MarkFileHeader = ::io::aeron::cluster::codecs::mark::MarkFileHeader;
    emit_corrupt_mark(dir, "cluster-mark-huge-var-string.bin",
                      MARK_MESSAGE_HEADER_LENGTH + static_cast<std::size_t>(MarkFileHeader::SBE_BLOCK_LENGTH));
    emit_corrupt_mark(dir, "cluster-mark-huge-error-length.bin",
                      MARK_MESSAGE_HEADER_LENGTH + MarkFileHeader::errorBufferLengthEncodingOffset());
}

}  // namespace axc::fuzz

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
{
    return axc::fuzz::seed_emitter_initialize(argc, argv);
}
