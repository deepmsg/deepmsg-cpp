#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "core/aeron/async_add.h"
#include "core/aeron/handles.h"
#include "core/aeron/image_handlers.h"
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
class AeronClient
{
public:
    static constexpr std::size_t MAX_DIR_LENGTH = 4096;
    static constexpr std::size_t MAX_CHANNEL_LENGTH = 4096;

    static std::expected<AeronClient, Error> connect(const ClientConfig& config) noexcept;

    AeronClient() = default;
    AeronClient(AeronClient&&) noexcept = default;
    AeronClient& operator=(AeronClient&&) noexcept = default;
    ~AeronClient() = default;

    AeronClient(const AeronClient&) = delete;
    AeronClient& operator=(const AeronClient&) = delete;

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
    AeronContextPtr context_{};
    AeronPtr client_{};
};

}  // namespace axc::core
