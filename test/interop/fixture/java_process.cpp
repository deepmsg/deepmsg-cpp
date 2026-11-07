#include "fixture/java_process.h"

#include <cstdlib>

namespace axc::interop
{

JavaProcess::JavaProcess(std::string executable) : executable_(std::move(executable)) {}

std::string JavaProcess::default_executable()
{
    const char* configured = std::getenv("AXC_REAL_JAVA");
    return nullptr == configured ? std::string{"java"} : std::string{configured};
}

std::expected<ProcessResult, Error> JavaProcess::run(const std::vector<std::string>& arguments,
                                                     std::chrono::milliseconds timeout) const noexcept
{
    ProcessOptions options;
    options.arguments = arguments;
    options.timeout = timeout;
    return run_process(executable_, options);
}

}  // namespace axc::interop
