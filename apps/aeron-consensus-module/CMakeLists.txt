axc_add_executable(aeron-consensus-module
    SOURCES
        main.cpp
    DEPS
        axc_cluster_consensus
        aeron_driver_static)
