axc_add_library(axc_cluster_common
    SOURCES
        module.cpp
        recording_log.cpp
        node_state_file.cpp
    DEPS
        axc_core
        axc_sbe)
