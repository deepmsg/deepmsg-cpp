find_package(Threads REQUIRED)

axc_add_library(axc_core
    SOURCES
        version.cpp
        deepmsg/deepmsg_client.cpp
        agent/cpu_affinity.cpp
        agent/idle_strategy.cpp
        collections/expandable_ring_buffer.cpp
        collections/long2long_hash_map.cpp
        config/name_table.cpp
        config/property_source.cpp
        counters/counter_directory.cpp
        io/fs.cpp
        io/distinct_error_log.cpp
        io/mapped_file.cpp
        io/mark_file.cpp
        time/clock.cpp
        trace/trace.cpp
    DEPS
        aeron_static
        Threads::Threads)

target_compile_definitions(axc_core PRIVATE "AXC_VERSION_STRING=\"${PROJECT_VERSION}\"")

# The driver seam: only the binaries that own a media driver link this.
axc_add_library(axc_driver
    SOURCES
        driver/embedded_driver.cpp
        media/dpdk/media.cpp
    DEPS
        axc_core
        aeron_driver_static)

if (AXC_DPDK_AVAILABLE)
    target_link_libraries(axc_driver PUBLIC axc_dpdk)
    target_compile_definitions(axc_driver PUBLIC AXC_WITH_DPDK=1)
endif ()
