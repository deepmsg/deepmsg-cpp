axc_add_executable(deepmsg-cluster-backup
    SOURCES
        main.cpp
    DEPS
        axc_cluster_backup
        aeron_driver_static)
