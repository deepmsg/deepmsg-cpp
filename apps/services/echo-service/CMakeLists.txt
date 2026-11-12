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
