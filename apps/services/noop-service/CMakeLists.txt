axc_add_library(axc_noop_service
    SOURCES
        module.cpp
    DEPS
        axc_cluster_service)

target_include_directories(axc_noop_service PUBLIC "${PROJECT_SOURCE_DIR}")
