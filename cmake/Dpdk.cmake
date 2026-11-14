# DPDK is optional: the media is compiled in only when the build asks for it
# AND `libdpdk` is installed. A default build never needs DPDK.
#
# The media is linked as shared libraries (not static): a statically linked
# DPDK drops PMDs that register themselves through constructors in plugin
# objects unless the linker is told to keep them; the system's shared libraries
# do not have that problem.

set(AXC_DPDK_AVAILABLE FALSE CACHE INTERNAL "")

if (AXC_WITH_DPDK)
    find_package(PkgConfig REQUIRED)

    pkg_check_modules(AXC_LIBDPDK QUIET IMPORTED_TARGET libdpdk)

    if (NOT AXC_LIBDPDK_FOUND)
        message(FATAL_ERROR
            "AXC_WITH_DPDK=ON but `pkg-config libdpdk` was not found. "
            "Install the DPDK development files (Debian/Ubuntu: libdpdk-dev).")
    endif ()

    add_library(axc_dpdk INTERFACE)
    target_link_libraries(axc_dpdk INTERFACE PkgConfig::AXC_LIBDPDK)

    # The one translation unit that includes a DPDK header. It is C, compiled
    # against the installed headers; the C++ side sees only `shim.h`.
    add_library(axc_dpdk_shim STATIC "${PROJECT_SOURCE_DIR}/src/core/media/dpdk/shim/shim.c")
    target_link_libraries(axc_dpdk_shim PUBLIC PkgConfig::AXC_LIBDPDK)
    target_include_directories(axc_dpdk_shim PUBLIC
        "${PROJECT_SOURCE_DIR}/src/core/media/dpdk/shim"
        "${AXC_SRC_DIR}")
    set_target_properties(axc_dpdk_shim PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)

    set(AXC_DPDK_AVAILABLE TRUE CACHE INTERNAL "")
    message(STATUS "libdpdk ${AXC_LIBDPDK_VERSION}: DPDK media compiled in")
endif ()
