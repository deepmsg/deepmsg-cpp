// Cluster ingress: every adapter that decodes bytes from the network.
//
// The first byte picks the adapter; the rest is one complete message. All
// handlers are no-op: the oracle is that decoding hostile bytes never reads out
// of bounds, never loops forever and never throws across the C callback
// boundary.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include "cluster/client/cluster_client.h"
#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/cm_snapshot.h"
#include "cluster/consensus/consensus_adapter.h"
#include "cluster/consensus/consensus_module_adapter.h"
#include "cluster/consensus/ingress_adapter.h"
#include "cluster/service/service_adapter.h"
#include "cluster/service/snapshot.h"
#include "fuzz_common.h"

#include <io_aeron_cluster_codecs/AdminRequest.h>
#include <io_aeron_cluster_codecs/AdminResponse.h>
#include <io_aeron_cluster_codecs/AppendPosition.h>
#include <io_aeron_cluster_codecs/BackupQuery.h>
#include <io_aeron_cluster_codecs/CancelTimer.h>
#include <io_aeron_cluster_codecs/CanvassPosition.h>
#include <io_aeron_cluster_codecs/CatchupPosition.h>
#include <io_aeron_cluster_codecs/Challenge.h>
#include <io_aeron_cluster_codecs/ChallengeResponse.h>
#include <io_aeron_cluster_codecs/CloseSession.h>
#include <io_aeron_cluster_codecs/ClusterMembersQuery.h>
#include <io_aeron_cluster_codecs/ClusterSession.h>
#include <io_aeron_cluster_codecs/CommitPosition.h>
#include <io_aeron_cluster_codecs/ConsensusModule.h>
#include <io_aeron_cluster_codecs/JoinLog.h>
#include <io_aeron_cluster_codecs/NewLeaderEvent.h>
#include <io_aeron_cluster_codecs/NewLeadershipTerm.h>
#include <io_aeron_cluster_codecs/PendingMessageTracker.h>
#include <io_aeron_cluster_codecs/RequestServiceAck.h>
#include <io_aeron_cluster_codecs/RequestVote.h>
#include <io_aeron_cluster_codecs/ScheduleTimer.h>
#include <io_aeron_cluster_codecs/ServiceAck.h>
#include <io_aeron_cluster_codecs/ServiceTerminationPosition.h>
#include <io_aeron_cluster_codecs/SessionCloseRequest.h>
#include <io_aeron_cluster_codecs/SessionConnectRequest.h>
#include <io_aeron_cluster_codecs/SessionEvent.h>
#include <io_aeron_cluster_codecs/SessionKeepAlive.h>
#include <io_aeron_cluster_codecs/SessionMessageHeader.h>
#include <io_aeron_cluster_codecs/SnapshotMarker.h>
#include <io_aeron_cluster_codecs/StopCatchup.h>
#include <io_aeron_cluster_codecs/Vote.h>

namespace
{

namespace cluster_codecs = ::io::aeron::cluster::codecs;
namespace client = deepmsg::cluster::client;
namespace common = deepmsg::cluster::common;
namespace consensus = deepmsg::cluster::consensus;
namespace service = deepmsg::cluster::service;

constexpr std::uint16_t CLUSTER_SCHEMA_ID = 111;

class NoopIngressHandler final : public consensus::IngressHandler
{
public:
    bool on_ingress_message(std::int64_t, std::int64_t, const std::uint8_t*, std::size_t) override
    {
        return true;
    }
    void on_session_connect(std::int64_t, std::int32_t, std::int32_t, std::string_view, std::span<const std::uint8_t>,
                            std::string_view, const consensus::IngressImageInfo&) override
    {
    }
    void on_session_close(std::int64_t, std::int64_t) override {}
    void on_session_keep_alive(std::int64_t, std::int64_t, std::int64_t) override {}
    void on_challenge_response(std::int64_t, std::int64_t, std::span<const std::uint8_t>) override {}
    void on_admin_request(std::int64_t, std::int64_t, std::int64_t, std::int32_t,
                          std::span<const std::uint8_t>) override
    {
    }
    void on_extension_message(std::int32_t, std::int32_t, std::span<const std::uint8_t>) override {}
};

class NoopConsensusHandler final : public consensus::ConsensusMessageHandler
{
public:
    void on_canvass_position(std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t) override {}
    void on_request_vote(std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t) override {}
    void on_vote(std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t, bool) override {}
    void on_new_leadership_term(std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int64_t,
                                std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t,
                                std::int32_t, bool) override
    {
    }
    void on_append_position(std::int64_t, std::int64_t, std::int32_t, std::int16_t) override {}
    void on_commit_position(std::int64_t, std::int64_t, std::int32_t) override {}
    void on_catchup_position(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}
    void on_stop_catchup(std::int64_t, std::int32_t) override {}
    void on_termination_position(std::int64_t, std::int64_t) override {}
    void on_termination_ack(std::int64_t, std::int64_t, std::int32_t) override {}
    void on_consensus_challenge_response(std::int64_t, std::int64_t, std::string_view) override {}
    void on_backup_query(std::int64_t, std::int32_t, std::int32_t, std::int64_t, std::string_view,
                         std::span<const std::uint8_t>) override
    {
    }
    void on_unhandled_template(std::int32_t) override {}
};

class NoopServiceControlHandler final : public consensus::ServiceControlHandler
{
public:
    std::expected<void, axc::core::Error> on_service_message(std::int64_t, std::span<const std::uint8_t>) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_service_close_session(std::int64_t) override
    {
        return {};
    }
    void on_schedule_timer(std::int64_t, std::int64_t) override {}
    void on_cancel_timer(std::int64_t) override {}
    std::expected<void, axc::core::Error> on_service_ack(std::int64_t, std::int64_t, std::int64_t, std::int64_t,
                                                         std::int32_t) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_cluster_members_query(std::int64_t, bool) override
    {
        return {};
    }
};

class NoopEgressListener final : public client::EgressListener
{
public:
    void on_message(std::int64_t, std::int64_t, std::span<const std::uint8_t>) override {}
    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, common::codecs::EventCode::Value,
                          std::string_view) override
    {
    }
    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}
    void on_admin_response(std::int64_t, std::int64_t, common::codecs::AdminRequestType::Value,
                           common::codecs::AdminResponseCode::Value, std::string_view,
                           std::span<const std::uint8_t>) override
    {
    }
};

class NoopControlledEgressListener final : public client::ControlledEgressListener
{
public:
    aeron_controlled_fragment_handler_action_t on_message(std::int64_t, std::int64_t,
                                                          std::span<const std::uint8_t>) override
    {
        return AERON_ACTION_CONTINUE;
    }
    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, common::codecs::EventCode::Value,
                          std::string_view) override
    {
    }
    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}
    void on_admin_response(std::int64_t, std::int64_t, common::codecs::AdminRequestType::Value,
                           common::codecs::AdminResponseCode::Value, std::string_view,
                           std::span<const std::uint8_t>) override
    {
    }
};

class NoopConsensusSnapshotHandler final : public consensus::SnapshotLoadHandler
{
public:
    std::expected<void, axc::core::Error> on_load_begin_snapshot(std::int32_t, service::TimeUnit) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_end_snapshot() override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_cluster_session(std::int64_t, std::int64_t, std::int64_t,
                                                                  std::int64_t, cluster_codecs::CloseReason::Value,
                                                                  std::int32_t, std::string_view) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_consensus_module_state(std::int64_t, std::int64_t, std::int64_t,
                                                                         std::int32_t) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_pending_message_tracker(std::int64_t, std::int64_t, std::int32_t,
                                                                          std::int32_t) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_pending_message(std::int64_t, std::span<const std::uint8_t>) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_timer(std::int64_t, std::int64_t) override
    {
        return {};
    }
};

class NoopServiceSnapshotHandler final : public service::SnapshotLoadHandler
{
public:
    std::expected<void, axc::core::Error> on_load_begin_snapshot(std::int32_t, service::TimeUnit) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_cluster_session(std::int64_t, std::int32_t, std::string_view,
                                                                  std::span<const std::uint8_t>) override
    {
        return {};
    }
    std::expected<void, axc::core::Error> on_load_end_snapshot() override
    {
        return {};
    }
};

void dispatch_ingress(std::span<const std::uint8_t> message)
{
    NoopIngressHandler handler;
    consensus::IngressAdapter adapter{handler, 50};
    (void)adapter.dispatch(message);
}

void dispatch_consensus(std::span<const std::uint8_t> message)
{
    NoopConsensusHandler handler;
    consensus::ConsensusAdapter adapter{consensus::CONSENSUS_FRAGMENT_LIMIT};
    adapter.handler(handler);
    (void)adapter.dispatch(message);
}

void dispatch_consensus_module(std::span<const std::uint8_t> message)
{
    NoopServiceControlHandler handler;
    consensus::ConsensusModuleAdapter adapter{handler};
    (void)adapter.dispatch_message(message);
}

void dispatch_egress(std::span<const std::uint8_t> message)
{
    NoopEgressListener listener;
    client::EgressAdapter adapter{listener, 1, 10};
    adapter.dispatch(message);
    (void)adapter.take_error();
}

void dispatch_controlled_egress(std::span<const std::uint8_t> message)
{
    NoopControlledEgressListener listener;
    client::ControlledEgressAdapter adapter{listener, 1, 10};
    (void)adapter.dispatch(message);
    (void)adapter.take_error();
}

void dispatch_service(std::span<const std::uint8_t> message)
{
    service::ServiceEvents events{};
    (void)service::ServiceAdapter::dispatch_message(events, message);
}

void dispatch_consensus_snapshot(std::span<const std::uint8_t> message)
{
    NoopConsensusSnapshotHandler handler;
    consensus::ConsensusModuleSnapshotAdapter adapter{};
    adapter.handler(handler);
    (void)adapter.dispatch(message);
}

void dispatch_service_snapshot(std::span<const std::uint8_t> message)
{
    NoopServiceSnapshotHandler handler;
    service::ServiceSnapshotLoader loader{};
    loader.handler(handler);
    (void)loader.dispatch(message);
}

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

/// selector byte + SBE header + zeroed fixed block + room for empty var-data.
template <class Codec>
void emit_frame(const std::filesystem::path& dir, const char* name, std::uint8_t selector)
{
    constexpr std::uint16_t block_length = static_cast<std::uint16_t>(Codec::SBE_BLOCK_LENGTH);
    constexpr std::uint16_t template_id = static_cast<std::uint16_t>(Codec::SBE_TEMPLATE_ID);
    constexpr std::uint16_t schema_version = static_cast<std::uint16_t>(Codec::sbeSchemaVersion());

    std::vector<std::uint8_t> bytes(1 + 8 + block_length + 64, 0x00);
    bytes[0] = selector;
    std::memcpy(bytes.data() + 1, &block_length, sizeof(block_length));
    std::memcpy(bytes.data() + 3, &template_id, sizeof(template_id));
    std::memcpy(bytes.data() + 5, &CLUSTER_SCHEMA_ID, sizeof(CLUSTER_SCHEMA_ID));
    std::memcpy(bytes.data() + 7, &schema_version, sizeof(schema_version));

    write_bytes(dir, name, bytes);
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    axc::fuzz::ByteCursor cursor{std::span<const std::uint8_t>{data, size}};
    const std::uint8_t selector = cursor.consume_u8() % 8;
    const std::span<const std::uint8_t> message = cursor.remaining();

    switch (selector)
    {
        case 0:
            dispatch_ingress(message);
            break;
        case 1:
            dispatch_consensus(message);
            break;
        case 2:
            dispatch_consensus_module(message);
            break;
        case 3:
            dispatch_egress(message);
            break;
        case 4:
            dispatch_controlled_egress(message);
            break;
        case 5:
            dispatch_service(message);
            break;
        case 6:
            dispatch_consensus_snapshot(message);
            break;
        default:
            dispatch_service_snapshot(message);
            break;
    }
    return 0;
}

namespace axc::fuzz
{

void emit_seeds(const std::filesystem::path& dir)
{
    // The input the first libFuzzer run found (an under-declared block).
    const std::filesystem::path artifacts = std::filesystem::path{AXC_PROJECT_SOURCE_DIR} / "test/fuzz/artifacts";
    axc::fuzz::copy_seed_file(artifacts / "cluster-under-declared-block.bin", dir);

    // 0: ingress
    emit_frame<cluster_codecs::SessionConnectRequest>(dir, "ingress-session-connect.bin", 0);
    emit_frame<cluster_codecs::SessionMessageHeader>(dir, "ingress-session-message.bin", 0);
    emit_frame<cluster_codecs::SessionCloseRequest>(dir, "ingress-session-close.bin", 0);
    emit_frame<cluster_codecs::SessionKeepAlive>(dir, "ingress-session-keep-alive.bin", 0);
    emit_frame<cluster_codecs::AdminRequest>(dir, "ingress-admin-request.bin", 0);
    emit_frame<cluster_codecs::ChallengeResponse>(dir, "ingress-challenge-response.bin", 0);

    // 1: consensus
    emit_frame<cluster_codecs::CanvassPosition>(dir, "consensus-canvass.bin", 1);
    emit_frame<cluster_codecs::RequestVote>(dir, "consensus-request-vote.bin", 1);
    emit_frame<cluster_codecs::Vote>(dir, "consensus-vote.bin", 1);
    emit_frame<cluster_codecs::NewLeadershipTerm>(dir, "consensus-new-leadership-term.bin", 1);
    emit_frame<cluster_codecs::AppendPosition>(dir, "consensus-append-position.bin", 1);
    emit_frame<cluster_codecs::CommitPosition>(dir, "consensus-commit-position.bin", 1);
    emit_frame<cluster_codecs::CatchupPosition>(dir, "consensus-catchup-position.bin", 1);
    emit_frame<cluster_codecs::StopCatchup>(dir, "consensus-stop-catchup.bin", 1);
    emit_frame<cluster_codecs::BackupQuery>(dir, "consensus-backup-query.bin", 1);

    // 2: consensus module adapter
    emit_frame<cluster_codecs::ServiceAck>(dir, "cm-service-ack.bin", 2);
    emit_frame<cluster_codecs::CloseSession>(dir, "cm-close-session.bin", 2);
    emit_frame<cluster_codecs::ScheduleTimer>(dir, "cm-schedule-timer.bin", 2);
    emit_frame<cluster_codecs::CancelTimer>(dir, "cm-cancel-timer.bin", 2);
    emit_frame<cluster_codecs::ClusterMembersQuery>(dir, "cm-members-query.bin", 2);

    // 3/4: egress
    emit_frame<cluster_codecs::SessionMessageHeader>(dir, "egress-session-message.bin", 3);
    emit_frame<cluster_codecs::SessionEvent>(dir, "egress-session-event.bin", 3);
    emit_frame<cluster_codecs::NewLeaderEvent>(dir, "egress-new-leader.bin", 3);
    emit_frame<cluster_codecs::Challenge>(dir, "egress-challenge.bin", 3);
    emit_frame<cluster_codecs::AdminResponse>(dir, "egress-admin-response.bin", 3);
    emit_frame<cluster_codecs::NewLeaderEvent>(dir, "egress-controlled-new-leader.bin", 4);

    // 5: service adapter
    emit_frame<cluster_codecs::JoinLog>(dir, "service-join-log.bin", 5);
    emit_frame<cluster_codecs::ServiceTerminationPosition>(dir, "service-termination-position.bin", 5);
    emit_frame<cluster_codecs::RequestServiceAck>(dir, "service-request-ack.bin", 5);

    // 6: consensus module snapshot
    emit_frame<cluster_codecs::SnapshotMarker>(dir, "cm-snapshot-marker.bin", 6);
    emit_frame<cluster_codecs::ClusterSession>(dir, "cm-snapshot-cluster-session.bin", 6);
    emit_frame<cluster_codecs::ScheduleTimer>(dir, "cm-snapshot-timer.bin", 6);
    emit_frame<cluster_codecs::ConsensusModule>(dir, "cm-snapshot-consensus-module.bin", 6);
    emit_frame<cluster_codecs::PendingMessageTracker>(dir, "cm-snapshot-pending-tracker.bin", 6);

    // 7: service snapshot
    emit_frame<cluster_codecs::SnapshotMarker>(dir, "service-snapshot-marker.bin", 7);
    emit_frame<cluster_codecs::ClusterSession>(dir, "service-snapshot-cluster-session.bin", 7);
}

}  // namespace axc::fuzz

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
{
    return axc::fuzz::seed_emitter_initialize(argc, argv);
}
