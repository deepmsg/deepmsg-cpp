find_package(Threads REQUIRED)

axc_add_library(axc_core
    SOURCES
        version.cpp
        agent/cpu_affinity.cpp
        agent/idle_strategy.cpp
        collections/long2long_hash_map.cpp
        time/clock.cpp
    DEPS
        aeron_static
        Threads::Threads)

target_compile_definitions(axc_core PRIVATE "AXC_VERSION_STRING=\"${PROJECT_VERSION}\"")
