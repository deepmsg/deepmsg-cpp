axc_add_library(axc_cluster_consensus
    SOURCES
        module.cpp
    DEPS
        axc_cluster_common
        axc_cluster_election
        aeron_static
        aeron_archive_c_client_static)
