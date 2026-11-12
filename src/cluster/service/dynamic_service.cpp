#include "cluster/service/dynamic_service.h"

#include <dlfcn.h>

#include <string>

namespace deepmsg::cluster::service
{

std::expected<ServiceLibrary, Error> ServiceLibrary::open(std::string_view path)
{
    const std::string library_path{path};
    void* handle = ::dlopen(library_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (nullptr == handle)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                     "dlopen failed for " + library_path + ": " + ::dlerror()));
    }

    auto* symbol = ::dlsym(handle, std::string{SERVICE_FACTORY_SYMBOL}.c_str());
    if (nullptr == symbol)
    {
        (void)::dlclose(handle);
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error, library_path + " exports no " + std::string{SERVICE_FACTORY_SYMBOL}));
    }

    ServiceLibrary library;
    library.handle_ = handle;
    library.factory_ = reinterpret_cast<ServiceFactory>(symbol);
    return library;
}

ServiceLibrary::ServiceLibrary(ServiceLibrary&& other) noexcept : handle_(other.handle_), factory_(other.factory_)
{
    other.handle_ = nullptr;
    other.factory_ = nullptr;
}

ServiceLibrary& ServiceLibrary::operator=(ServiceLibrary&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != handle_)
        {
            (void)::dlclose(handle_);
        }
        handle_ = other.handle_;
        factory_ = other.factory_;
        other.handle_ = nullptr;
        other.factory_ = nullptr;
    }
    return *this;
}

ServiceLibrary::~ServiceLibrary()
{
    if (nullptr != handle_)
    {
        (void)::dlclose(handle_);
    }
}

}  // namespace deepmsg::cluster::service
