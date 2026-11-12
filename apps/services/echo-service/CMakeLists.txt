axc_add_library(axc_echo_service
    SOURCES
        echo_service.cpp
    DEPS
        axc_cluster_service)

axc_add_executable(echo-service
    SOURCES
        main.cpp
    DEPS
        axc_echo_service
        axc_cluster_service)

target_include_directories(axc_echo_service PUBLIC "${PROJECT_SOURCE_DIR}")

# The `.so` business for the dynamic-loading criterion. It links no axc library:
# its undefined symbols resolve from the container binary at dlopen time (the
# binary is linked with -rdynamic), which is what a third-party plugin sees.
add_library(echo_service_dynamic SHARED
    echo_service.cpp
    echo_service_dynamic.cpp)
axc_target_defaults(echo_service_dynamic)
target_include_directories(echo_service_dynamic PRIVATE
    "${PROJECT_SOURCE_DIR}"
    "${PROJECT_BINARY_DIR}/generated/axc/sbe"
    "${PROJECT_SOURCE_DIR}/third_party/aeron/aeron-client/src/main/c"
    "${PROJECT_SOURCE_DIR}/third_party/aeron/aeron-archive/src/main/c")
add_dependencies(echo_service_dynamic axc_sbe_codecs)
set_target_properties(echo_service_dynamic PROPERTIES PREFIX "" OUTPUT_NAME "echo-service")
target_link_options(echo_service_dynamic PRIVATE "-Wl,--unresolved-symbols=ignore-all")
