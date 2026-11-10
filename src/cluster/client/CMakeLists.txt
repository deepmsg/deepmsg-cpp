axc_add_library(axc_cluster_client
    SOURCES
        module.cpp
        client_config.cpp
        channel_util.cpp
        egress_poller.cpp
        async_connect.cpp
        cluster_client.cpp
        c_abi.cpp
    DEPS
        axc_cluster_common
        axc_core
        axc_sbe
        aeron_static)

target_include_directories(axc_cluster_client PUBLIC "${PROJECT_SOURCE_DIR}/include")
