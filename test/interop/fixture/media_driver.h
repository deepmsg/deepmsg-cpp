// Which media driver a case spawns.
//
// `AXC_AERONMD_DEFAULT` is the reference's `aeronmd` built from the pinned
// submodule. `AXC_MEDIA_DRIVER` lets a run substitute our own
// `deepmsg-media-driver` without rebuilding the test binary — the same arguments
// (`-Daeron.dir=...`, `AERON_*` environment) and the same job.

#pragma once

#include <cstdlib>
#include <string>
#include <string_view>

namespace axc::interop
{

inline std::string media_driver_path()
{
#ifdef AXC_AERONMD_DEFAULT
    constexpr std::string_view fallback{AXC_AERONMD_DEFAULT};
#else
    constexpr std::string_view fallback{"aeronmd"};
#endif

    const char* const override_path = std::getenv("AXC_MEDIA_DRIVER");
    if (nullptr != override_path && '\0' != override_path[0])
    {
        return std::string{override_path};
    }
    return std::string{fallback};
}

}  // namespace axc::interop
