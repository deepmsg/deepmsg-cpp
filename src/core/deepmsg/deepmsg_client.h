#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "core/deepmsg/async_add.h"
#include "core/deepmsg/handles.h"
#include "core/deepmsg/image_handlers.h"
#include "core/error/error.h"

namespace axc::core
{

struct ClientConfig
{
    std::string_view dir;
    bool use_conductor_agent_invoker{true};
};

/// RAII client over the C Aeron API.
///
/// The conductor runs in invoker mode by default, so the owning agent calls
/// `do_work()` once per pass; this is how archive and CM drive the client in the
/// reference. Resource creation is two-phase through `AsyncAdd` (never blocks).
class DeepmsgClient
{
public:
    static constexpr std::size_t MAX_DIR_LENGTH = 4096;
    static constexpr std::size_t MAX_CHANNEL_LENGTH = 4096;

    static std::expected<DeepmsgClient, Error> connect(const ClientConfig& config) noexcept;

    DeepmsgClient() = default;
    DeepmsgClient(DeepmsgClient&&) noexcept = default;

    /// Released **client first**: `aeron_close` writes its close command to the
    /// driver ring buffer, which lives in the CnC map `context_` owns. The
    /// defaulted member-wise assignment would reset `context_` first (members
    /// are assigned in declaration order) and then `aeron_close` would deref a
    /// ring descriptor in unmapped memory.
    DeepmsgClient& operator=(DeepmsgClient&& other) noexcept
    {
        if (this != &other)
        {
            client_ = std::move(other.client_);
            context_ = std::move(other.context_);
        }
        return *this;
    }

    ~DeepmsgClient() = default;

    /// Same ordering rule as move assignment, for an explicit teardown.
    void close() noexcept
    {
        client_.reset();
        context_.reset();
    }

    DeepmsgClient(const DeepmsgClient&) = delete;
    DeepmsgClient& operator=(const DeepmsgClient&) = delete;

    /// Drives the client conductor once. Returns the C API result.
    [[nodiscard]] int do_work() noexcept;

    [[nodiscard]] aeron_counters_reader_t* counters_reader() noexcept;

    [[nodiscard]] aeron_t* handle() noexcept
    {
        return client_.get();
    }

    [[nodiscard]] AsyncAdd<CounterPtr> add_counter(std::int32_t type_id, std::span<const std::uint8_t> key,
                                                   std::string_view label) noexcept;

    [[nodiscard]] AsyncAdd<ExPubPtr> add_exclusive_publication(std::string_view channel,
                                                               std::int32_t stream_id) noexcept;

    [[nodiscard]] AsyncAdd<PubPtr> add_publication(std::string_view channel, std::int32_t stream_id) noexcept;

    [[nodiscard]] AsyncAdd<SubPtr> add_subscription(std::string_view channel, std::int32_t stream_id,
                                                    ImageHandlers handlers = {}) noexcept;

private:
    // Declared context first so it is destroyed *after* the client (members are
    // destroyed in reverse order and the client must be closed before its context).
    ContextPtr context_{};
    ClientPtr client_{};
};

}  // namespace axc::core
