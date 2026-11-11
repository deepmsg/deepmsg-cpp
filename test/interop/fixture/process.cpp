#include "fixture/process.h"

#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

namespace axc::interop
{

using core::ErrorCode;
using core::make_error;

namespace
{

void apply_environment(const std::vector<std::string>& environment) noexcept
{
    for (const auto& entry : environment)
    {
        const std::size_t equals = entry.find('=');
        if (std::string::npos == equals)
        {
            continue;
        }
        ::setenv(entry.substr(0, equals).c_str(), entry.substr(equals + 1).c_str(), 1);
    }
}

std::vector<char*> to_argv(const std::string_view executable, const std::vector<std::string>& arguments)
{
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 2);
    argv.push_back(const_cast<char*>(executable.data()));
    for (const auto& argument : arguments)
    {
        argv.push_back(const_cast<char*>(argument.c_str()));
    }
    argv.push_back(nullptr);
    return argv;
}

}  // namespace

std::expected<ProcessResult, Error> run_process(std::string_view executable, const ProcessOptions& options) noexcept
{
    const std::string executable_name{executable};
    const auto arguments = options.arguments;
    auto argv = to_argv(executable_name, arguments);

    int pipe_fds[2] = {-1, -1};
    if (0 != ::pipe(pipe_fds))
    {
        return std::unexpected(make_error(ErrorCode::io_error, "pipe failed", errno));
    }
    int input_fds[2] = {-1, -1};
    if (0 != ::pipe(input_fds))
    {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return std::unexpected(make_error(ErrorCode::io_error, "stdin pipe failed", errno));
    }

    const pid_t pid = ::fork();
    if (pid < 0)
    {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        ::close(input_fds[0]);
        ::close(input_fds[1]);
        return std::unexpected(make_error(ErrorCode::io_error, "fork failed", errno));
    }

    if (0 == pid)
    {
        ::close(pipe_fds[0]);
        ::dup2(pipe_fds[1], STDOUT_FILENO);
        ::dup2(pipe_fds[1], STDERR_FILENO);
        ::close(pipe_fds[1]);
        ::close(input_fds[1]);
        ::dup2(input_fds[0], STDIN_FILENO);
        ::close(input_fds[0]);
        apply_environment(options.environment);
        ::execvp(executable_name.c_str(), argv.data());
        ::_exit(127);
    }

    ::close(pipe_fds[1]);
    ::close(input_fds[0]);
    if (!options.input.empty())
    {
        std::size_t written = 0;
        while (written < options.input.size())
        {
            const ssize_t count = ::write(input_fds[1], options.input.data() + written, options.input.size() - written);
            if (count <= 0)
            {
                break;
            }
            written += static_cast<std::size_t>(count);
        }
    }
    ::close(input_fds[1]);

    ProcessResult result;
    const auto deadline = std::chrono::steady_clock::now() + options.timeout;

    for (;;)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            result.timed_out = true;
            ::kill(pid, SIGKILL);
            break;
        }

        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
        pollfd descriptor{pipe_fds[0], POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, static_cast<int>(std::max<std::int64_t>(remaining, 1)));
        if (ready < 0)
        {
            if (EINTR == errno)
            {
                continue;
            }
            break;
        }
        if (0 == ready)
        {
            result.timed_out = true;
            ::kill(pid, SIGKILL);
            break;
        }

        char buffer[4096];
        const ssize_t count = ::read(pipe_fds[0], buffer, sizeof(buffer));
        if (0 == count)
        {
            break;
        }
        if (count < 0)
        {
            if (EINTR == errno)
            {
                continue;
            }
            break;
        }
        if (result.output.size() < options.output_limit)
        {
            const auto room = options.output_limit - result.output.size();
            result.output.append(buffer, std::min<std::size_t>(static_cast<std::size_t>(count), room));
        }
    }

    ::close(pipe_fds[0]);

    int status = 0;
    while (0 != ::waitpid(pid, &status, 0) && EINTR == errno)
    {
    }

    if (WIFEXITED(status))
    {
        result.exit_code = WEXITSTATUS(status);
    }

    return result;
}

ChildProcess::ChildProcess(ChildProcess&& other) noexcept : pid_(other.pid_), exit_code_(other.exit_code_)
{
    other.pid_ = -1;
}

ChildProcess& ChildProcess::operator=(ChildProcess&& other) noexcept
{
    if (this != &other)
    {
        terminate();
        pid_ = other.pid_;
        exit_code_ = other.exit_code_;
        other.pid_ = -1;
    }
    return *this;
}

ChildProcess::~ChildProcess()
{
    terminate();
}

std::expected<ChildProcess, Error> ChildProcess::spawn(std::string_view executable,
                                                       const std::vector<std::string>& arguments,
                                                       const std::vector<std::string>& environment) noexcept
{
    const std::string executable_name{executable};
    const auto arguments_copy = arguments;
    auto argv = to_argv(executable_name, arguments_copy);

    const pid_t pid = ::fork();
    if (pid < 0)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "fork failed", errno));
    }

    if (0 == pid)
    {
        apply_environment(environment);
        ::execvp(executable_name.c_str(), argv.data());
        ::_exit(127);
    }

    ChildProcess process;
    process.pid_ = pid;
    return process;
}

bool ChildProcess::running() const noexcept
{
    if (pid_ < 0)
    {
        return false;
    }

    int status = 0;
    const pid_t result = ::waitpid(pid_, &status, WNOHANG);
    if (0 == result)
    {
        return true;
    }

    if (result == pid_ && WIFEXITED(status))
    {
        exit_code_ = WEXITSTATUS(status);
    }
    pid_ = -1;
    return false;
}

void ChildProcess::terminate(std::chrono::milliseconds grace) noexcept
{
    if (pid_ < 0)
    {
        return;
    }

    ::kill(pid_, SIGTERM);

    const auto deadline = std::chrono::steady_clock::now() + grace;
    while (std::chrono::steady_clock::now() < deadline)
    {
        int status = 0;
        const pid_t result = ::waitpid(pid_, &status, WNOHANG);
        if (result == pid_)
        {
            if (WIFEXITED(status))
            {
                exit_code_ = WEXITSTATUS(status);
            }
            pid_ = -1;
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ::kill(pid_, SIGKILL);
    int status = 0;
    ::waitpid(pid_, &status, 0);
    if (WIFEXITED(status))
    {
        exit_code_ = WEXITSTATUS(status);
    }
    pid_ = -1;
}

}  // namespace axc::interop
