#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "cluster/service/clustered_service.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

/// The virtual form of a business, for a `.so` (Q8).
///
/// A container in the binary is selected by name at run time, so a compiled-in
/// business instantiated through this base loses only the inlining; the static
/// `ClusteredServiceAgent<MyService>` path is unchanged for embedders.
class ClusteredServiceBase
{
public:
    virtual ~ClusteredServiceBase() = default;

    virtual std::expected<void, Error> on_start(Cluster& cluster, SnapshotImage* snapshot) = 0;
    virtual std::expected<void, Error> on_session_open(ClientSession& session, std::int64_t timestamp) = 0;
    virtual std::expected<void, Error> on_session_close(ClientSession& session, std::int64_t timestamp,
                                                        CloseReason reason) = 0;
    virtual std::expected<void, Error> on_session_message(Cluster& cluster, ClientSession* session,
                                                          std::int64_t timestamp, std::span<const std::uint8_t> message,
                                                          const LogHeader& header) = 0;
    virtual std::expected<void, Error> on_timer_event(Cluster& cluster, std::int64_t correlation_id,
                                                      std::int64_t timestamp) = 0;
    virtual std::expected<void, Error> on_take_snapshot(SnapshotPublication& writer) = 0;
    virtual std::expected<void, Error> on_role_change(Role role) = 0;
    virtual std::expected<void, Error> on_terminate(Cluster& cluster) = 0;

    virtual std::expected<void, Error> on_new_leadership_term_event(Cluster& cluster,
                                                                    const NewLeadershipTermEvent& event)
    {
        (void)cluster;
        (void)event;
        return {};
    }

    virtual int do_background_work(Cluster& cluster, std::int64_t now_ns)
    {
        (void)cluster;
        (void)now_ns;
        return 0;
    }
};

/// The factory symbol a business library exports (`extern "C"`).
using ServiceFactory = ClusteredServiceBase* (*)();

inline constexpr std::string_view SERVICE_FACTORY_SYMBOL = "deepmsg_cluster_service_create";

/// A statically-known business adapted to the virtual base (the `.so` side:
/// the library's factory returns one of these).
template <class S>
class DynamicServiceAdapter final : public ClusteredServiceBase
{
public:
    std::expected<void, Error> on_start(Cluster& cluster, SnapshotImage* snapshot) override
    {
        return service_.on_start(cluster, snapshot);
    }

    std::expected<void, Error> on_session_open(ClientSession& session, std::int64_t timestamp) override
    {
        return service_.on_session_open(session, timestamp);
    }

    std::expected<void, Error> on_session_close(ClientSession& session, std::int64_t timestamp,
                                                CloseReason reason) override
    {
        return service_.on_session_close(session, timestamp, reason);
    }

    std::expected<void, Error> on_session_message(Cluster& cluster, ClientSession* session, std::int64_t timestamp,
                                                  std::span<const std::uint8_t> message,
                                                  const LogHeader& header) override
    {
        return service_.on_session_message(cluster, session, timestamp, message, header);
    }

    std::expected<void, Error> on_timer_event(Cluster& cluster, std::int64_t correlation_id,
                                              std::int64_t timestamp) override
    {
        return service_.on_timer_event(cluster, correlation_id, timestamp);
    }

    std::expected<void, Error> on_take_snapshot(SnapshotPublication& writer) override
    {
        return service_.on_take_snapshot(writer);
    }

    std::expected<void, Error> on_role_change(Role role) override
    {
        return service_.on_role_change(role);
    }

    std::expected<void, Error> on_terminate(Cluster& cluster) override
    {
        return service_.on_terminate(cluster);
    }

private:
    S service_{};
};

/// A business behind the virtual base, adapted to the concept the agent uses.
class DynamicService
{
public:
    explicit DynamicService(std::unique_ptr<ClusteredServiceBase> service) : service_(std::move(service)) {}

    std::expected<void, Error> on_start(Cluster& cluster, SnapshotImage* snapshot)
    {
        return service_->on_start(cluster, snapshot);
    }

    std::expected<void, Error> on_session_open(ClientSession& session, std::int64_t timestamp)
    {
        return service_->on_session_open(session, timestamp);
    }

    std::expected<void, Error> on_session_close(ClientSession& session, std::int64_t timestamp, CloseReason reason)
    {
        return service_->on_session_close(session, timestamp, reason);
    }

    std::expected<void, Error> on_session_message(Cluster& cluster, ClientSession* session, std::int64_t timestamp,
                                                  std::span<const std::uint8_t> message, const LogHeader& header)
    {
        return service_->on_session_message(cluster, session, timestamp, message, header);
    }

    std::expected<void, Error> on_timer_event(Cluster& cluster, std::int64_t correlation_id, std::int64_t timestamp)
    {
        return service_->on_timer_event(cluster, correlation_id, timestamp);
    }

    std::expected<void, Error> on_take_snapshot(SnapshotPublication& writer)
    {
        return service_->on_take_snapshot(writer);
    }

    std::expected<void, Error> on_role_change(Role role)
    {
        return service_->on_role_change(role);
    }

    std::expected<void, Error> on_terminate(Cluster& cluster)
    {
        return service_->on_terminate(cluster);
    }

    std::expected<void, Error> on_new_leadership_term_event(Cluster& cluster, const NewLeadershipTermEvent& event)
    {
        return service_->on_new_leadership_term_event(cluster, event);
    }

    int do_background_work(Cluster& cluster, std::int64_t now_ns)
    {
        return service_->do_background_work(cluster, now_ns);
    }

private:
    std::unique_ptr<ClusteredServiceBase> service_;
};

/// A loaded `.so`, kept open for the life of the process.
class ServiceLibrary
{
public:
    static std::expected<ServiceLibrary, Error> open(std::string_view path);
    ServiceLibrary() = default;
    ServiceLibrary(const ServiceLibrary&) = delete;
    ServiceLibrary& operator=(const ServiceLibrary&) = delete;
    ServiceLibrary(ServiceLibrary&& other) noexcept;
    ServiceLibrary& operator=(ServiceLibrary&& other) noexcept;
    ~ServiceLibrary();

    [[nodiscard]] ServiceFactory factory() const noexcept
    {
        return factory_;
    }

private:
    void* handle_{nullptr};
    ServiceFactory factory_{nullptr};
};

}  // namespace deepmsg::cluster::service
