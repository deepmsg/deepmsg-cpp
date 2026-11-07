#include "core/io/mapped_file.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

namespace axc::core
{
namespace
{

std::expected<int, Error> open_file(std::string_view path, int flags, bool create, std::size_t length) noexcept
{
    const std::string name{path};
    const int fd = ::open(name.c_str(), flags, 0644);
    if (fd < 0)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "open failed", errno));
    }

    if (create && 0 != ::ftruncate(fd, static_cast<off_t>(length)))
    {
        const int error = errno;
        ::close(fd);
        return std::unexpected(make_error(ErrorCode::io_error, "ftruncate failed", error));
    }

    return fd;
}

std::expected<std::uint8_t*, Error> map_fd(int fd, std::size_t length) noexcept
{
    void* address = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (MAP_FAILED == address)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "mmap failed", errno));
    }
    return static_cast<std::uint8_t*>(address);
}

}  // namespace

MappedFile::MappedFile(MappedFile&& other) noexcept : fd_(other.fd_), address_(other.address_), length_(other.length_)
{
    other.fd_ = -1;
    other.address_ = nullptr;
    other.length_ = 0;
}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept
{
    if (this != &other)
    {
        close();
        fd_ = other.fd_;
        address_ = other.address_;
        length_ = other.length_;
        other.fd_ = -1;
        other.address_ = nullptr;
        other.length_ = 0;
    }
    return *this;
}

MappedFile::~MappedFile()
{
    close();
}

void MappedFile::close() noexcept
{
    if (nullptr != address_)
    {
        ::munmap(address_, length_);
        address_ = nullptr;
    }
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
    length_ = 0;
}

std::expected<MappedFile, Error> MappedFile::create(std::string_view path, std::size_t length) noexcept
{
    if (0 == length)
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "mapped file length must be > 0"));
    }

    auto fd = open_file(path, O_RDWR | O_CREAT, true, length);
    if (!fd)
    {
        return std::unexpected(fd.error());
    }

    auto address = map_fd(*fd, length);
    if (!address)
    {
        ::close(*fd);
        return std::unexpected(address.error());
    }

    MappedFile file;
    file.fd_ = *fd;
    file.address_ = *address;
    file.length_ = length;
    return file;
}

std::expected<MappedFile, Error> MappedFile::open(std::string_view path) noexcept
{
    auto fd = open_file(path, O_RDWR, false, 0);
    if (!fd)
    {
        return std::unexpected(fd.error());
    }

    struct stat info{};
    if (0 != ::fstat(*fd, &info) || info.st_size <= 0)
    {
        const int error = errno;
        ::close(*fd);
        return std::unexpected(make_error(ErrorCode::io_error, "fstat failed or file is empty", error));
    }

    const auto length = static_cast<std::size_t>(info.st_size);
    auto address = map_fd(*fd, length);
    if (!address)
    {
        ::close(*fd);
        return std::unexpected(address.error());
    }

    MappedFile file;
    file.fd_ = *fd;
    file.address_ = *address;
    file.length_ = length;
    return file;
}

std::expected<void, Error> MappedFile::flush() noexcept
{
    if (nullptr == address_)
    {
        return {};
    }
    if (0 != ::msync(address_, length_, MS_SYNC))
    {
        return std::unexpected(make_error(ErrorCode::io_error, "msync failed", errno));
    }
    return {};
}

std::expected<void, Error> MappedFile::grow(std::size_t new_length) noexcept
{
    if (new_length <= length_)
    {
        return {};
    }

    if (0 != ::ftruncate(fd_, static_cast<off_t>(new_length)))
    {
        return std::unexpected(make_error(ErrorCode::io_error, "ftruncate failed", errno));
    }

    void* remapped = ::mremap(address_, length_, new_length, MREMAP_MAYMOVE);
    if (MAP_FAILED == remapped)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "mremap failed", errno));
    }

    address_ = static_cast<std::uint8_t*>(remapped);
    length_ = new_length;
    return {};
}

}  // namespace axc::core
