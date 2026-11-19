axc_add_library(axc_cluster_tool
    SOURCES
        tool.cpp
        format.cpp
    DEPS
        axc_cluster_common
        axc_cluster_consensus
        axc_cluster_service
        axc_core
        aeron_static
        aeron_archive_c_client_static)

# The unit tests include `tools/cluster-tool/tool.h` from the project root.
target_include_directories(axc_cluster_tool PUBLIC "${PROJECT_SOURCE_DIR}")

axc_add_executable(cluster-tool
    SOURCES
        main.cpp
    DEPS
        axc_cluster_tool)
