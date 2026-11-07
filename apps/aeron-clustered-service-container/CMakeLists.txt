axc_add_executable(aeron-clustered-service-container
    SOURCES
        main.cpp
    DEPS
        axc_cluster_service
        aeron_driver_static)
