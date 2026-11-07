axc_add_library(axc_cluster_client
    SOURCES
        module.cpp
    DEPS
        axc_cluster_common
        aeron_static)
