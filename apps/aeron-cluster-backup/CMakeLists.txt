axc_add_executable(aeron-cluster-backup
    SOURCES
        main.cpp
    DEPS
        axc_cluster_backup
        aeron_driver_static)
