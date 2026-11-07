#include "core/io/fs.h"

#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

namespace axc::core
{

bool file_exists(std::string_view path) noexcept
{
    const std::string name{path};
    struct stat info{};
    return 0 == ::stat(name.c_str(), &info) && S_ISREG(info.st_mode);
}

bool directory_exists(std::string_view path) noexcept
{
    const std::string name{path};
    struct stat info{};
    return 0 == ::stat(name.c_str(), &info) && S_ISDIR(info.st_mode);
}

std::expected<std::size_t, Error> file_size(std::string_view path) noexcept
{
    const std::string name{path};
    struct stat info{};
    if (0 != ::stat(name.c_str(), &info))
    {
        return std::unexpected(make_error(ErrorCode::io_error, "stat failed", errno));
    }
    return static_cast<std::size_t>(info.st_size);
}

std::expected<void, Error> remove_file(std::string_view path) noexcept
{
    const std::string name{path};
    if (0 != ::unlink(name.c_str()) && ENOENT != errno)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "unlink failed", errno));
    }
    return {};
}

std::expected<void, Error> create_directories(std::string_view path) noexcept
{
    if (path.empty())
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "empty directory path"));
    }

    std::string current;
    current.reserve(path.size());

    std::size_t cursor = 0;
    if ('/' == path[0])
    {
        current.push_back('/');
        cursor = 1;
    }

    while (cursor <= path.size())
    {
        const std::size_t slash = path.find('/', cursor);
        const std::size_t end = slash == std::string_view::npos ? path.size() : slash;
        const std::string_view component = path.substr(cursor, end - cursor);

        if (!component.empty())
        {
            if (!current.empty() && '/' != current.back())
            {
                current.push_back('/');
            }
            current.append(component);

            if (0 != ::mkdir(current.c_str(), 0755) && EEXIST != errno)
            {
                return std::unexpected(make_error(ErrorCode::io_error, "mkdir failed", errno));
            }
        }

        if (slash == std::string_view::npos)
        {
            break;
        }
        cursor = slash + 1;
    }

    return {};
}

}  // namespace axc::core
