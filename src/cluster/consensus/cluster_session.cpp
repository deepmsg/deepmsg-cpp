#include "cluster/consensus/cluster_session.h"

#include <cstdint>
#include <cstring>
#include <utility>

#include "cluster/common/cluster_counters.h"

namespace deepmsg::cluster::consensus
{
namespace
{

void set_counter_reference_id(aeron_counter_t* counter, std::int64_t reference_id) noexcept
{
    if (nullptr == counter)
    {
        return;
    }
    if (std::int64_t* address = aeron_counter_addr(counter); nullptr != address)
    {
        auto* reference = reinterpret_cast<std::int64_t*>(reinterpret_cast<std::uint8_t*>(address) +
                                                          ClusterSession::REFERENCE_ID_OFFSET);
        __atomic_store_n(reference, reference_id, __ATOMIC_RELEASE);
    }
}

}  // namespace

std::string_view session_state_name(SessionState state) noexcept
{
    switch (state)
    {
        case SessionState::init:
            return "INIT";
        case SessionState::connecting:
            return "CONNECTING";
        case SessionState::connected:
            return "CONNECTED";
        case SessionState::challenged:
            return "CHALLENGED";
        case SessionState::authenticated:
            return "AUTHENTICATED";
        case SessionState::rejected:
            return "REJECTED";
        case SessionState::open:
            return "OPEN";
        case SessionState::closing:
            return "CLOSING";
        case SessionState::invalid:
            return "INVALID";
        case SessionState::closed:
            return "CLOSED";
    }
    return "UNKNOWN";
}

ClusterSession::ClusterSession(std::int32_t cluster_member_id, std::int64_t session_id, std::int32_t response_stream_id,
                               std::string response_channel, std::string session_info)
    : cluster_member_id_(cluster_member_id),
      id_(session_id),
      response_stream_id_(response_stream_id),
      response_channel_(std::move(response_channel)),
      session_info_(std::move(session_info))
{
}

void ClusterSession::state(SessionState new_state, std::string_view /*reason*/)
{
    // `ClusterTracing.traceClusterSessionStateChange` is C1's CFB logging, not
    // transcribed yet; the state itself is the contract here.
    state_ = new_state;
}

void ClusterSession::closing(codecs::CloseReason::Value close_reason)
{
    close_reason_ = close_reason;
    has_open_event_pending_ = false;
    has_new_leader_event_pending_ = false;
    time_of_last_activity_ns_ = 0;
    state(SessionState::closing, "closing");
}

void ClusterSession::authenticate(std::span<const std::uint8_t> encoded_principal)
{
    if (encoded_principal.size() > MAX_ENCODED_PRINCIPAL_LENGTH)
    {
        state(SessionState::invalid, "encoded principal too long");
        return;
    }
    encoded_principal_.assign(encoded_principal.begin(), encoded_principal.end());
    state(SessionState::authenticated, "authenticated");
}

void ClusterSession::open(std::int64_t opened_log_position)
{
    opened_log_position_ = opened_log_position;
    state(SessionState::open, "opened");
}

void ClusterSession::load_snapshot_state(std::int64_t correlation_id, std::int64_t opened_log_position,
                                         std::int64_t time_of_last_activity_ns, codecs::CloseReason::Value close_reason)
{
    opened_log_position_ = opened_log_position;
    time_of_last_activity_ns_ = time_of_last_activity_ns;
    correlation_id_ = correlation_id;
    close_reason_ = close_reason;

    if (codecs::CloseReason::NULL_VALUE != close_reason)
    {
        state(SessionState::closing, "closing");
    }
    else
    {
        state(SessionState::open, "opened");
    }
}

void ClusterSession::reject(codecs::EventCode::Value code, std::string response_detail)
{
    event_code_ = code;
    response_detail_ = std::move(response_detail);
    state(SessionState::rejected, "rejected");
}

void ClusterSession::redirect(std::string ingress_endpoints)
{
    event_code_ = codecs::EventCode::REDIRECT;
    response_detail_ = std::move(ingress_endpoints);
}

void ClusterSession::async_connect(axc::core::DeepmsgClient& client, std::int32_t cluster_id)
{
    const auto key = common::session_counter_key(cluster_id, id_);
    const std::string label = common::session_counter_label(session_info_, cluster_id);
    counter_add_ = client.add_counter(common::CLUSTER_SESSION_TYPE_ID, {key.data(), key.size()}, label);
    publication_add_ = client.add_publication(response_channel_, response_stream_id_);
}

bool ClusterSession::is_response_publication_connected(axc::core::DeepmsgClient& client, std::int64_t now_ns)
{
    if (publication_add_.pending())
    {
        auto polled = publication_add_.poll();
        if (!polled)
        {
            state(SessionState::invalid, "response publication failed");
            return false;
        }
        if (!polled->has_value())
        {
            return false;
        }
        response_publication_ = std::move(**polled);
    }

    if (counter_add_.pending())
    {
        auto polled = counter_add_.poll();
        if (polled && polled->has_value())
        {
            session_counter_ = std::move(**polled);
        }
    }

    if (nullptr == response_publication_)
    {
        return false;
    }

    if (SessionState::init == state_)
    {
        if (session_counter_)
        {
            aeron_publication_constants_t constants{};
            if (0 == aeron_publication_constants(response_publication_.get(), &constants))
            {
                set_counter_reference_id(session_counter_.get(), constants.registration_id);
            }
            set_counter_value(id_);
        }
        time_of_last_activity_ns_ = now_ns;
        state(SessionState::connecting, "connecting");
    }

    (void)client;
    return aeron_publication_is_connected(response_publication_.get());
}

void ClusterSession::link_ingress_image(std::int64_t image_correlation_id) noexcept
{
    if (NULL_VALUE == ingress_image_correlation_id_)
    {
        ingress_image_correlation_id_ = image_correlation_id;
    }
}

void ClusterSession::close(axc::core::DeepmsgClient& client, std::string_view reason)
{
    disconnect(client);
    state(SessionState::closed, reason);
}

void ClusterSession::disconnect(axc::core::DeepmsgClient& client)
{
    (void)client;
    publication_add_ = {};
    counter_add_ = {};
    response_publication_.reset();
    session_counter_.reset();
}

std::int64_t ClusterSession::max_payload_length() const noexcept
{
    if (nullptr == response_publication_)
    {
        return 0;
    }
    aeron_publication_constants_t constants{};
    if (0 != aeron_publication_constants(response_publication_.get(), &constants))
    {
        return 0;
    }
    return static_cast<std::int64_t>(constants.max_payload_length);
}

long ClusterSession::try_claim(std::size_t length, aeron_buffer_claim_t& claim) noexcept
{
    if (nullptr == response_publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }
    return static_cast<long>(aeron_publication_try_claim(response_publication_.get(), length, &claim));
}

long ClusterSession::offer(const std::uint8_t* buffer, std::size_t length) noexcept
{
    if (nullptr == response_publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }
    return static_cast<long>(aeron_publication_offer(response_publication_.get(), buffer, length, nullptr, nullptr));
}

void ClusterSession::set_counter_reference(std::int64_t reference_id) noexcept
{
    set_counter_reference_id(session_counter_.get(), reference_id);
}

void ClusterSession::set_counter_value(std::int64_t value) noexcept
{
    if (!session_counter_)
    {
        return;
    }
    if (std::int64_t* address = aeron_counter_addr(session_counter_.get()); nullptr != address)
    {
        __atomic_store_n(address, value, __ATOMIC_RELEASE);
    }
}

}  // namespace deepmsg::cluster::consensus
