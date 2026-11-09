#pragma once

#include <atomic>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>

#include "cluster/common/cluster_mark_file.h"
#include "core/agent/idle_strategy.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// How often the container's clock tick runs (`CSA.ONE_MILLISECOND_NS`).
inline constexpr std::int64_t CLOCK_TICK_INTERVAL_NS = 1'000'000;
/// How often the mark file is stamped (`CSA.MARK_FILE_UPDATE_INTERVAL_NS`).
inline constexpr std::int64_t MARK_FILE_UPDATE_INTERVAL_NS = 1'000'000'000;

/// Which callback the container is inside, for `checkForValidInvocation`
/// (`CSA:1297-1304`).
enum class LifecycleCallback
{
    none,
    on_start,
    on_terminate,
    on_role_change,
    do_background_work
};

[[nodiscard]] std::string_view lifecycle_name(LifecycleCallback callback) noexcept;

/// The commit-position counter, remembered by id and registration id so a
/// reclaimed counter is recognised (`CSA:1136-1142`).
struct CommitPosition
{
    std::int32_t counter_id{-1};
    std::int64_t registration_id{-1};
};

/// The state a business may not see directly but `Cluster::idle` must drive.
class ContainerContext
{
public:
    ContainerContext(axc::core::IdleStrategy idle, std::string_view idle_name, common::ClusterMarkFile mark_file);

    ContainerContext(const ContainerContext&) = delete;
    ContainerContext& operator=(const ContainerContext&) = delete;
    ContainerContext(ContainerContext&&) noexcept = delete;
    ContainerContext& operator=(ContainerContext&&) noexcept = delete;
    ~ContainerContext() = default;

    [[nodiscard]] common::ClusterMarkFile& mark_file() noexcept
    {
        return mark_file_;
    }

    [[nodiscard]] const common::ClusterMarkFile& mark_file() const noexcept
    {
        return mark_file_;
    }

    void set_commit_position(std::int32_t counter_id, std::int64_t registration_id) noexcept
    {
        commit_position_ = CommitPosition{counter_id, registration_id};
    }

    [[nodiscard]] const std::optional<CommitPosition>& commit_position() const noexcept
    {
        return commit_position_;
    }

    void set_lifecycle(LifecycleCallback callback) noexcept
    {
        lifecycle_ = callback;
    }

    [[nodiscard]] LifecycleCallback lifecycle() const noexcept
    {
        return lifecycle_;
    }

    void request_shutdown() noexcept
    {
        shutdown_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool is_shutdown_requested() const noexcept
    {
        return shutdown_.load(std::memory_order_acquire);
    }

    void abort() noexcept
    {
        abort_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool is_aborted() const noexcept
    {
        return abort_.load(std::memory_order_acquire);
    }

    /// `checkForClockTick` (`CSA:1114-1155`). Returns true when the tick ran
    /// (so the caller should poll the service stream), or an error that ends
    /// the agent.
    [[nodiscard]] std::expected<bool, Error> check_for_clock_tick(axc::core::DeepmsgClient& client,
                                                                  std::int64_t now_ns);

    /// `idle(workCount)` (`CSA:423-430`).
    [[nodiscard]] std::expected<void, Error> idle(axc::core::DeepmsgClient& client, int work);

    void reset_idle() noexcept
    {
        idle_ = fresh_idle_;
    }

private:
    axc::core::IdleStrategy idle_{};
    axc::core::IdleStrategy fresh_idle_{};
    common::ClusterMarkFile mark_file_;
    std::optional<CommitPosition> commit_position_{};
    std::int64_t last_slow_tick_ns_{0};
    std::int64_t mark_file_update_deadline_ms_{0};
    std::atomic<bool> shutdown_{false};
    std::atomic<bool> abort_{false};
    LifecycleCallback lifecycle_{LifecycleCallback::none};
};

}  // namespace deepmsg::cluster::service
