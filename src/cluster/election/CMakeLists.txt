axc_add_library(axc_cluster_election
    SOURCES
        module.cpp
        election.cpp
    DEPS
        axc_cluster_common
        axc_core
        aeron_static)
