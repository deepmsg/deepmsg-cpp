#pragma once

#include <exception>
#include <functional>

#include "core/error/error.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

/// `ExtendedTerminationHook.run(AgentTerminationException)`
/// (`ExtendedTerminationHook.java:26-35`) without the Java exception: C++ has
/// no `AgentTerminationException`, so the cause is the expected flag plus the
/// error that ended the agent.
struct TerminationCause
{
    bool expected{true};
    Error error{};
};

using ExtendedTerminationHook = std::function<void(const TerminationCause&)>;
using TerminationHook = std::function<void()>;

/// `runTerminationHooks` (`ConsensusModuleAgent.java:3457-3470`,
/// `ClusteredServiceAgent.java:1302-1315`): the extended hook first, then the
/// termination hook; a throwing hook is counted and the other still runs.
inline void run_termination_hooks(const ExtendedTerminationHook& extended, const TerminationHook& termination,
                                  const TerminationCause& cause,
                                  const std::function<void(const Error&)>& report) noexcept
{
    if (extended)
    {
        try
        {
            extended(cause);
        }
        catch (const std::exception& ex)
        {
            report(axc::core::make_error(axc::core::ErrorCode::internal_error, ex.what()));
        }
        catch (...)
        {
            report(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                         "extended termination hook threw a foreign exception"));
        }
    }

    if (termination)
    {
        try
        {
            termination();
        }
        catch (const std::exception& ex)
        {
            report(axc::core::make_error(axc::core::ErrorCode::internal_error, ex.what()));
        }
        catch (...)
        {
            report(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                         "termination hook threw a foreign exception"));
        }
    }
}

}  // namespace deepmsg::cluster::common
