// C6c-2 F4b: the reference's `TestNode.MessageTrackingService` for the
// service-IPC scenarios. A client message records its id, sends three service
// messages through `Cluster.offer`, schedules two timers and echoes the client
// payload; a service message (session == nullptr) records its id; timers are
// recorded too. The three lists go into the snapshot and come back in
// `on_start` (the reference's four snapshot records).
//
// The scenario reads the state from outside the container: every event is
// appended to `AXC_TEST_TRACKING_FILE` (`M <id>`, `S <id>`, `T <correlation>`),
// and a snapshot restore rewrites the restored lists.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <fstream>
#include <span>
#include <string>
#include <vector>

#include "cluster/service/cluster.h"
#include "cluster/service/clustered_service.h"
#include "cluster/service/dynamic_service.h"
#include "cluster/service/snapshot.h"

#if defined(__GNUC__)
#define AXC_DYNAMIC_EXPORT extern "C" __attribute__((visibility("default")))
#else
#define AXC_DYNAMIC_EXPORT extern "C"
#endif

namespace
{

namespace service = deepmsg::cluster::service;

constexpr std::int32_t SERVICE_MESSAGES_PER_INGRESS = 3;
constexpr std::int32_t TIMER_MESSAGES_PER_INGRESS = 2;
constexpr std::uint8_t SNAPSHOT_COUNTERS = 1;
constexpr std::uint8_t SNAPSHOT_CLIENT_MESSAGES = 2;
constexpr std::uint8_t SNAPSHOT_SERVICE_MESSAGES = 3;
constexpr std::uint8_t SNAPSHOT_TIMERS = 4;

class TrackingService
{
public:
    TrackingService()
    {
        if (const char* id = std::getenv("AXC_TEST_TRACKING_SERVICE_ID"); nullptr != id)
        {
            service_id_ = std::atoi(id);
        }
        next_service_message_number_ = 1'000'000 * service_id_;
        next_timer_correlation_id_ = -1'000'000LL * service_id_;
        file_ = std::getenv("AXC_TEST_TRACKING_FILE");
    }

    std::expected<void, service::Error> on_start(service::Cluster& cluster, service::SnapshotImage* snapshot)
    {
        client_messages_.clear();
        service_messages_.clear();
        timers_.clear();
        message_count_ = 0;
        timer_count_ = 0;
        next_service_message_number_ = 1'000'000 * service_id_;
        next_timer_correlation_id_ = -1'000'000LL * service_id_;

        // The new process starts a fresh file; a restored snapshot is written
        // back so the scenario sees the state the service reloaded.
        if (nullptr != file_)
        {
            std::ofstream out{file_, std::ios::trunc};
        }
        if (nullptr == snapshot)
        {
            return {};
        }

        Loader loader{*this};
        while (!snapshot->is_end_of_stream())
        {
            const int fragments = snapshot->poll(loader, 1);
            if (fragments < 0)
            {
                return std::unexpected(
                    axc::core::make_error(axc::core::ErrorCode::internal_error, "the tracking snapshot poll failed"));
            }
            auto idled = cluster.idle(fragments);
            if (!idled)
            {
                return std::unexpected(idled.error());
            }
        }
        if (!loader.done())
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                         "the tracking snapshot carried no counters"));
        }

        for (const std::int32_t id : client_messages_)
        {
            write_line('M', id);
        }
        for (const std::int32_t id : service_messages_)
        {
            write_line('S', id);
        }
        for (const std::int64_t correlation_id : timers_)
        {
            write_line('T', correlation_id);
        }
        return {};
    }

    std::expected<void, service::Error> on_session_open(service::ClientSession&, std::int64_t)
    {
        return {};
    }

    std::expected<void, service::Error> on_session_close(service::ClientSession&, std::int64_t, service::CloseReason)
    {
        return {};
    }

    std::expected<void, service::Error> on_session_message(service::Cluster& cluster, service::ClientSession* session,
                                                           std::int64_t timestamp,
                                                           std::span<const std::uint8_t> payload,
                                                           const service::LogHeader&)
    {
        std::int32_t id = 0;
        std::memcpy(&id, payload.data(), sizeof(id));

        if (nullptr == session)
        {
            service_messages_.push_back(id);
            write_line('S', id);
            ++message_count_;
            return {};
        }

        client_messages_.push_back(id);
        write_line('M', id);

        for (std::int32_t i = 0; i < SERVICE_MESSAGES_PER_INGRESS; ++i)
        {
            const std::int32_t service_message = ++next_service_message_number_;
            const std::span<const std::uint8_t> bytes{reinterpret_cast<const std::uint8_t*>(&service_message),
                                                      sizeof(service_message)};
            while (true)
            {
                auto offered = cluster.offer(bytes);
                if (!offered)
                {
                    return std::unexpected(offered.error());
                }
                if (*offered > 0)
                {
                    break;
                }
                auto idled = cluster.idle(0);
                if (!idled)
                {
                    return std::unexpected(idled.error());
                }
            }
        }

        for (std::int32_t i = 0; i < TIMER_MESSAGES_PER_INGRESS; ++i)
        {
            const std::int64_t correlation_id = --next_timer_correlation_id_;
            while (true)
            {
                auto scheduled = cluster.schedule_timer(correlation_id, timestamp + 0);
                if (!scheduled)
                {
                    return std::unexpected(scheduled.error());
                }
                if (*scheduled)
                {
                    break;
                }
                auto idled = cluster.idle(0);
                if (!idled)
                {
                    return std::unexpected(idled.error());
                }
            }
        }

        auto* view = session;
        while (true)
        {
            auto offered = view->offer(payload);
            if (!offered)
            {
                return std::unexpected(offered.error());
            }
            if (*offered > 0)
            {
                break;
            }
            auto idled = cluster.idle(0);
            if (!idled)
            {
                return std::unexpected(idled.error());
            }
        }

        ++message_count_;
        return {};
    }

    std::expected<void, service::Error> on_timer_event(service::Cluster&, std::int64_t correlation_id, std::int64_t)
    {
        timers_.push_back(correlation_id);
        write_line('T', correlation_id);
        ++timer_count_;
        return {};
    }

    std::expected<void, service::Error> on_take_snapshot(service::SnapshotPublication& writer)
    {
        std::vector<std::uint8_t> counters;
        counters.push_back(SNAPSHOT_COUNTERS);
        append(counters, service_id_);
        append(counters, message_count_);
        append(counters, timer_count_);
        append(counters, next_service_message_number_);
        append(counters, next_timer_correlation_id_);
        if (!offer(writer, counters))
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::no_capacity, "the tracking snapshot was refused"));
        }

        if (!snapshot_messages(writer, SNAPSHOT_CLIENT_MESSAGES, client_messages_) ||
            !snapshot_messages(writer, SNAPSHOT_SERVICE_MESSAGES, service_messages_) || !snapshot_timers(writer))
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::no_capacity, "the tracking snapshot was refused"));
        }
        return {};
    }

    std::expected<void, service::Error> on_role_change(service::Role)
    {
        return {};
    }

    std::expected<void, service::Error> on_terminate(service::Cluster&)
    {
        return {};
    }

private:
    class Loader final : public service::SnapshotFragmentHandler
    {
    public:
        explicit Loader(TrackingService& owner) : owner_(owner) {}

        [[nodiscard]] bool done() const noexcept
        {
            return loaded_counters_;
        }

        void on_snapshot_fragment(const std::uint8_t* buffer, std::size_t length) override
        {
            if (length < 1)
            {
                return;
            }
            const std::uint8_t type = buffer[0];
            const std::uint8_t* data = buffer + 1;
            std::size_t remaining = length - 1;
            if (SNAPSHOT_COUNTERS == type)
            {
                std::int32_t stored_service_id = 0;
                take(data, remaining, stored_service_id);
                take(data, remaining, owner_.message_count_);
                take(data, remaining, owner_.timer_count_);
                take(data, remaining, owner_.next_service_message_number_);
                take(data, remaining, owner_.next_timer_correlation_id_);
                loaded_counters_ = true;
            }
            else if (SNAPSHOT_CLIENT_MESSAGES == type)
            {
                load_ints(data, remaining, owner_.client_messages_);
            }
            else if (SNAPSHOT_SERVICE_MESSAGES == type)
            {
                load_ints(data, remaining, owner_.service_messages_);
            }
            else if (SNAPSHOT_TIMERS == type)
            {
                load_longs(data, remaining, owner_.timers_);
            }
        }

    private:
        static void load_ints(const std::uint8_t*& data, std::size_t& remaining, std::vector<std::int32_t>& out)
        {
            std::int32_t count = 0;
            take(data, remaining, count);
            for (std::int32_t i = 0; i < count; ++i)
            {
                std::int32_t id = 0;
                take(data, remaining, id);
                out.push_back(id);
            }
        }

        static void load_longs(const std::uint8_t*& data, std::size_t& remaining, std::vector<std::int64_t>& out)
        {
            std::int32_t count = 0;
            take(data, remaining, count);
            for (std::int32_t i = 0; i < count; ++i)
            {
                std::int64_t id = 0;
                take(data, remaining, id);
                out.push_back(id);
            }
        }

        TrackingService& owner_;
        bool loaded_counters_{false};
    };

    template <class T>
    static void append(std::vector<std::uint8_t>& out, const T& value)
    {
        const auto* bytes = reinterpret_cast<const std::uint8_t*>(&value);
        out.insert(out.end(), bytes, bytes + sizeof(T));
    }

    template <class T>
    static void take(const std::uint8_t*& data, std::size_t& remaining, T& value)
    {
        if (remaining < sizeof(T))
        {
            return;
        }
        std::memcpy(&value, data, sizeof(T));
        data += sizeof(T);
        remaining -= sizeof(T);
    }

    [[nodiscard]] static bool offer(service::SnapshotPublication& writer, const std::vector<std::uint8_t>& record)
    {
        while (true)
        {
            const std::int64_t position = writer.offer(record);
            if (position > 0)
            {
                return true;
            }
            if (service::OFFER_BACK_PRESSURED == position || service::OFFER_ADMIN_ACTION == position)
            {
                writer.idle();
                continue;
            }
            return false;
        }
    }

    [[nodiscard]] static bool snapshot_messages(service::SnapshotPublication& writer, std::uint8_t type,
                                                const std::vector<std::int32_t>& messages)
    {
        std::vector<std::uint8_t> record;
        record.push_back(type);
        append(record, static_cast<std::int32_t>(messages.size()));
        for (const std::int32_t id : messages)
        {
            append(record, id);
        }
        return offer(writer, record);
    }

    [[nodiscard]] bool snapshot_timers(service::SnapshotPublication& writer)
    {
        std::vector<std::uint8_t> record;
        record.push_back(SNAPSHOT_TIMERS);
        append(record, static_cast<std::int32_t>(timers_.size()));
        for (const std::int64_t correlation_id : timers_)
        {
            append(record, correlation_id);
        }
        return offer(writer, record);
    }

    void write_line(char type, std::int64_t value)
    {
        if (nullptr == file_)
        {
            return;
        }
        std::ofstream out{file_, std::ios::app};
        out << type << ' ' << value << '\n';
        out.flush();
    }

    const char* file_{nullptr};
    std::int32_t service_id_{1};
    std::int32_t message_count_{0};
    std::int32_t timer_count_{0};
    std::int32_t next_service_message_number_{0};
    std::int64_t next_timer_correlation_id_{0};
    std::vector<std::int32_t> client_messages_{};
    std::vector<std::int32_t> service_messages_{};
    std::vector<std::int64_t> timers_{};
};

}  // namespace

AXC_DYNAMIC_EXPORT deepmsg::cluster::service::ClusteredServiceBase* deepmsg_cluster_service_create()
{
    return new deepmsg::cluster::service::DynamicServiceAdapter<TrackingService>{};
}
