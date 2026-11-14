#include "core/driver/embedded_driver.h"

#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace axc::core::driver
{

namespace
{

[[nodiscard]] Error driver_error(std::string_view what) noexcept
{
    std::string message{what};
    message += ": ";
    message += aeron_errmsg();
    return make_error(ErrorCode::internal_error, message);
}

}  // namespace

std::expected<EmbeddedDriver, Error> EmbeddedDriver::create(const PropertySource& properties, TerminationHook hook,
                                                            void* state) noexcept
{
    aeron_driver_context_t* context = nullptr;
    if (0 != aeron_driver_context_init(&context))
    {
        return std::unexpected(driver_error("driver context init"));
    }

    if (nullptr != hook)
    {
        (void)aeron_driver_context_set_driver_termination_hook(context, hook, state);
    }

    auto media = media::dpdk::install_if_configured(context, properties);
    if (!media)
    {
        (void)aeron_driver_context_close(context);
        return std::unexpected(media.error());
    }

    aeron_driver_t* driver = nullptr;
    if (0 != aeron_driver_init(&driver, context))
    {
        (void)media->close();
        (void)aeron_driver_context_close(context);
        return std::unexpected(driver_error("driver init"));
    }

    if (0 != aeron_driver_start(driver, false))
    {
        (void)aeron_driver_close(driver);
        (void)media->close();
        (void)aeron_driver_context_close(context);
        return std::unexpected(driver_error("driver start"));
    }

    return EmbeddedDriver(context, driver, std::move(*media));
}

EmbeddedDriver::EmbeddedDriver(aeron_driver_context_t* context, aeron_driver_t* driver,
                               media::dpdk::Media media) noexcept
    : context_{context}, driver_{driver}, media_{std::move(media)}
{
}

EmbeddedDriver::~EmbeddedDriver()
{
    (void)close();
}

EmbeddedDriver::EmbeddedDriver(EmbeddedDriver&& other) noexcept
    : context_{other.context_}, driver_{other.driver_}, media_{std::move(other.media_)}
{
    other.context_ = nullptr;
    other.driver_ = nullptr;
}

EmbeddedDriver& EmbeddedDriver::operator=(EmbeddedDriver&& other) noexcept
{
    if (this != &other)
    {
        (void)close();
        context_ = other.context_;
        driver_ = other.driver_;
        media_ = std::move(other.media_);
        other.context_ = nullptr;
        other.driver_ = nullptr;
    }
    return *this;
}

std::expected<void, Error> EmbeddedDriver::close() noexcept
{
    std::optional<Error> failure;

    if (nullptr != driver_)
    {
        if (0 != aeron_driver_close(driver_))
        {
            failure = driver_error("driver close");
        }
        driver_ = nullptr;
    }

    if (auto media_result = media_.close(); !media_result && !failure)
    {
        failure = media_result.error();
    }

    if (nullptr != context_)
    {
        if (0 != aeron_driver_context_close(context_) && !failure)
        {
            failure = driver_error("driver context close");
        }
        context_ = nullptr;
    }

    if (failure)
    {
        return std::unexpected(*failure);
    }
    return {};
}

}  // namespace axc::core::driver
