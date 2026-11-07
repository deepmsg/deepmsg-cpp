axc_add_library(axc_cluster_backup
    SOURCES
        module.cpp
    DEPS
        axc_cluster_common
        axc_cluster_consensus
        aeron_static)
