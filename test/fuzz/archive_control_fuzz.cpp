// Archive control plane: schema 101 request decoding and channel URI parsing.
//
// The adapter is the first code that sees a control request; a truncated block,
// a var-data length that runs past the fragment, or a hostile channel URI has
// to be answered, never read past the fragment.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include "archive/server/channel_uri.h"
#include "archive/server/control_session_adapter.h"
#include "fuzz_common.h"

#include <io_aeron_archive_codecs/ArchiveIdRequest.h>
#include <io_aeron_archive_codecs/AuthConnectRequest.h>
#include <io_aeron_archive_codecs/BoundedReplayRequest.h>
#include <io_aeron_archive_codecs/ChallengeResponse.h>
#include <io_aeron_archive_codecs/CloseSessionRequest.h>
#include <io_aeron_archive_codecs/DetachSegmentsRequest.h>
#include <io_aeron_archive_codecs/KeepAliveRequest.h>
#include <io_aeron_archive_codecs/ListRecordingsForUriRequest.h>
#include <io_aeron_archive_codecs/ListRecordingsRequest.h>
#include <io_aeron_archive_codecs/MigrateSegmentsRequest.h>
#include <io_aeron_archive_codecs/ReplayRequest.h>
#include <io_aeron_archive_codecs/ReplicateRequest2.h>
#include <io_aeron_archive_codecs/StartRecordingRequest.h>
#include <io_aeron_archive_codecs/StartRecordingRequest2.h>
#include <io_aeron_archive_codecs/StopAllReplaysRequest.h>
#include <io_aeron_archive_codecs/StopRecordingRequest.h>
#include <io_aeron_archive_codecs/StopReplayRequest.h>
#include <io_aeron_archive_codecs/StopReplicationRequest.h>
#include <io_aeron_archive_codecs/TruncateRecordingRequest.h>
#include <io_aeron_archive_codecs/UpdateChannelRequest.h>

namespace
{

namespace archive_codecs = ::io::aeron::archive::codecs;
namespace server = deepmsg::archive::server;

using server::ControlSession;
using server::ControlSessionAdapter;
using server::ControlSessionHandler;

/// The adapter never owns a session here: connect is refused, so every template
/// decodes its fixed block and is then turned away by the session gate.
class NoopControlSessionHandler final : public ControlSessionHandler
{
public:
    ControlSession* on_connect_request(const server::ConnectRequest&) override
    {
        return nullptr;
    }
    void on_start_recording(const server::StartRecordingRequest&) override {}
    void on_stop_recording(const server::StopRecordingRequest&) override {}
    void on_stop_recording_subscription(const server::StopRecordingSubscriptionRequest&) override {}
    void on_stop_recording_by_identity(const server::StopRecordingByIdentityRequest&) override {}
    void on_get_recording_position(const server::RecordingIdRequest&) override {}
    void on_get_stop_position(const server::RecordingIdRequest&) override {}
    void on_get_start_position(const server::RecordingIdRequest&) override {}
    void on_get_max_recorded_position(const server::RecordingIdRequest&) override {}
    void on_find_last_matching_recording(const server::FindLastMatchingRecordingRequest&) override {}
    void on_list_recording(const server::RecordingIdRequest&) override {}
    void on_list_recording_subscriptions(const server::ListRecordingSubscriptionsRequest&) override {}
    void on_extend_recording(const server::ExtendRecordingRequest&) override {}
    void on_truncate_recording(const server::TruncateRecordingRequest&) override {}
    void on_purge_recording(const server::PurgeRecordingRequest&) override {}
    void on_detach_segments(const server::SegmentStartPositionRequest&) override {}
    void on_delete_detached_segments(const server::RecordingIdRequest&) override {}
    void on_purge_segments(const server::SegmentStartPositionRequest&) override {}
    void on_attach_segments(const server::RecordingIdRequest&) override {}
    void on_list_recordings(const server::ListRecordingsRequest&) override {}
    void on_list_recordings_for_uri(const server::ListRecordingsForUriRequest&) override {}
    void on_replicate(const server::ReplicateRequest&) override {}
    void on_stop_replication(const server::StopReplicationRequest&) override {}
    void on_update_channel(const server::UpdateChannelRequest&) override {}
    void on_migrate_segments(const server::MigrateSegmentsRequest&) override {}
    void on_start_replay(const server::ReplayRequest&) override {}
    void on_start_bounded_replay(const server::BoundedReplayRequest&) override {}
    void on_stop_replay(const server::StopReplayRequest&) override {}
    void on_stop_all_replays(const server::StopAllReplaysRequest&) override {}
    void on_replay_token(const server::ReplayTokenRequest&) override {}
    std::int64_t generate_replay_token(ControlSession&, std::int64_t) override
    {
        return -1;
    }
    ControlSession* replay_session_for_token(std::int64_t, std::int64_t) override
    {
        return nullptr;
    }
    void log_warning(std::string_view) override {}
    [[nodiscard]] std::int64_t archive_id() const override
    {
        return 1;
    }
};

constexpr std::uint16_t ARCHIVE_SCHEMA_ID = 101;

void write_bytes(const std::filesystem::path& dir, const char* name, std::span<const std::uint8_t> bytes)
{
    const std::filesystem::path path = dir / name;
    std::FILE* file = std::fopen(path.c_str(), "wb");
    if (nullptr != file)
    {
        (void)std::fwrite(bytes.data(), 1, bytes.size(), file);
        (void)std::fclose(file);
    }
}

/// One structurally valid frame: selector 0, the 8-byte SBE header with the
/// codec's own block length / template id / schema version, a zeroed block and
/// room for zero-length var-data fields.
template <class Codec>
void emit_frame(const std::filesystem::path& dir, const char* name)
{
    constexpr std::uint16_t block_length = static_cast<std::uint16_t>(Codec::SBE_BLOCK_LENGTH);
    constexpr std::uint16_t template_id = static_cast<std::uint16_t>(Codec::SBE_TEMPLATE_ID);
    constexpr std::uint16_t schema_version = static_cast<std::uint16_t>(Codec::sbeSchemaVersion());

    std::vector<std::uint8_t> bytes(1 + 8 + block_length + 64, 0x00);
    bytes[0] = 0;  // selector: control adapter
    std::memcpy(bytes.data() + 1, &block_length, sizeof(block_length));
    std::memcpy(bytes.data() + 3, &template_id, sizeof(template_id));
    std::memcpy(bytes.data() + 5, &ARCHIVE_SCHEMA_ID, sizeof(ARCHIVE_SCHEMA_ID));
    std::memcpy(bytes.data() + 7, &schema_version, sizeof(schema_version));

    write_bytes(dir, name, bytes);
}

void emit_uri_seed(const std::filesystem::path& dir, const char* name, std::string_view uri)
{
    std::vector<std::uint8_t> bytes(1 + uri.size(), 0x00);
    bytes[0] = 1;  // selector: channel URI parser
    std::memcpy(bytes.data() + 1, uri.data(), uri.size());
    write_bytes(dir, name, bytes);
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    axc::fuzz::ByteCursor cursor{std::span<const std::uint8_t>{data, size}};
    const std::uint8_t selector = cursor.consume_u8() % 3;
    const std::span<const std::uint8_t> rest = cursor.remaining();

    NoopControlSessionHandler handler;
    ControlSessionAdapter adapter{handler};

    if (0 == selector || 2 == selector)
    {
        const std::int32_t length = static_cast<std::int32_t>(std::min<std::size_t>(rest.size(), INT32_MAX));
        (void)adapter.on_fragment(rest.data(), length, nullptr);
    }

    if (1 == selector || 2 == selector)
    {
        const std::string_view uri{reinterpret_cast<const char*>(rest.data()), rest.size()};
        (void)server::stripped_channel(uri);
    }
    return 0;
}

namespace axc::fuzz
{

void emit_seeds(const std::filesystem::path& dir)
{
    // The two inputs the first libFuzzer runs found (an under-declared block
    // and a var-data length of INT32_MAX) stay in the PR regression.
    const std::filesystem::path artifacts = std::filesystem::path{AXC_PROJECT_SOURCE_DIR} / "test/fuzz/artifacts";
    axc::fuzz::copy_seed_file(artifacts / "archive-control-under-declared-block.bin", dir);
    axc::fuzz::copy_seed_file(artifacts / "archive-control-huge-var-data.bin", dir);

    emit_frame<archive_codecs::CloseSessionRequest>(dir, "close-session.bin");
    emit_frame<archive_codecs::StartRecordingRequest>(dir, "start-recording.bin");
    emit_frame<archive_codecs::StartRecordingRequest2>(dir, "start-recording-2.bin");
    emit_frame<archive_codecs::StopRecordingRequest>(dir, "stop-recording.bin");
    emit_frame<archive_codecs::ReplayRequest>(dir, "replay.bin");
    emit_frame<archive_codecs::BoundedReplayRequest>(dir, "bounded-replay.bin");
    emit_frame<archive_codecs::StopReplayRequest>(dir, "stop-replay.bin");
    emit_frame<archive_codecs::StopAllReplaysRequest>(dir, "stop-all-replays.bin");
    emit_frame<archive_codecs::ListRecordingsRequest>(dir, "list-recordings.bin");
    emit_frame<archive_codecs::ListRecordingsForUriRequest>(dir, "list-recordings-for-uri.bin");
    emit_frame<archive_codecs::TruncateRecordingRequest>(dir, "truncate-recording.bin");
    emit_frame<archive_codecs::DetachSegmentsRequest>(dir, "detach-segments.bin");
    emit_frame<archive_codecs::MigrateSegmentsRequest>(dir, "migrate-segments.bin");
    emit_frame<archive_codecs::StopReplicationRequest>(dir, "stop-replication.bin");
    emit_frame<archive_codecs::ReplicateRequest2>(dir, "replicate-2.bin");
    emit_frame<archive_codecs::UpdateChannelRequest>(dir, "update-channel.bin");
    emit_frame<archive_codecs::ArchiveIdRequest>(dir, "archive-id.bin");
    emit_frame<archive_codecs::AuthConnectRequest>(dir, "auth-connect.bin");
    emit_frame<archive_codecs::ChallengeResponse>(dir, "challenge-response.bin");
    emit_frame<archive_codecs::KeepAliveRequest>(dir, "keep-alive.bin");

    emit_uri_seed(dir, "uri-ipc.bin", "aeron:ipc");
    emit_uri_seed(dir, "uri-udp.bin", "aeron:udp?endpoint=localhost:9000");
    emit_uri_seed(dir, "uri-nested.bin", "aeron:udp?endpoint=localhost:9000|term-length=64k|sparse=true");
}

}  // namespace axc::fuzz

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
{
    return axc::fuzz::seed_emitter_initialize(argc, argv);
}
