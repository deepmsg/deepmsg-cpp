axc_add_library(axc_cluster_service
    SOURCES
        module.cpp
    DEPS
        axc_cluster_common
        aeron_static)
