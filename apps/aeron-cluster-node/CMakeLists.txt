axc_add_executable(aeron-cluster-node
    SOURCES
        main.cpp
    DEPS
        axc_cluster_consensus
        axc_cluster_service
        axc_archive_server
        aeron_driver_static)
