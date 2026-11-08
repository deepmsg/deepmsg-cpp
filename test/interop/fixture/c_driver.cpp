#include "fixture/c_driver.h"

#include "core/io/fs.h"

namespace axc::interop
{

std::expected<CDriver, Error> CDriver::start(std::string_view aeronmd_path, std::string_view aeron_dir) noexcept
{
    return start(aeronmd_path, aeron_dir, {});
}

std::expected<CDriver, Error> CDriver::start(std::string_view aeronmd_path, std::string_view aeron_dir,
                                             const std::vector<std::string>& environment) noexcept
{
    auto created = core::create_directories(aeron_dir);
    if (!created)
    {
        return std::unexpected(created.error());
    }

    const std::string dir{aeron_dir};
    auto process = ChildProcess::spawn(aeronmd_path, {"-Daeron.dir=" + dir}, environment);
    if (!process)
    {
        return std::unexpected(process.error());
    }

    CDriver driver;
    driver.process_ = std::move(*process);
    driver.aeron_dir_ = dir;
    return driver;
}

}  // namespace axc::interop
