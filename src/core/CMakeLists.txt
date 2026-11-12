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
