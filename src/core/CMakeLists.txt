axc_add_library(axc_core
    SOURCES
        version.cpp
    DEPS
        aeron_static)

target_compile_definitions(axc_core PRIVATE "AXC_VERSION_STRING=\"${PROJECT_VERSION}\"")
