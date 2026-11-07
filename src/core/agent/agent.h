#pragma once

#include <concepts>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core
{

/// An agent is the unit of work run by AgentRunner / AgentInvoker.
///
/// `do_work()` returns the amount of work done in this pass (>= 0); a negative
/// return is not allowed -- errors go through the error handler so one bad pass
/// does not tear down the thread silently.
template <class A>
concept Agent = requires(A agent) {
    { agent.on_start() } -> std::same_as<std::expected<void, Error>>;
    { agent.do_work() } noexcept -> std::same_as<int>;
    { agent.on_close() } noexcept;
    { agent.role_name() } noexcept -> std::convertible_to<std::string_view>;
};

}  // namespace axc::core
