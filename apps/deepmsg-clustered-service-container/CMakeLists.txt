axc_add_executable(deepmsg-clustered-service-container
    SOURCES
        main.cpp
    DEPS
        axc_cluster_service
        axc_echo_service
        axc_basic_auction_service
        axc_noop_service)

# A `.so` business resolves its axc symbols from this binary.
target_link_options(deepmsg-clustered-service-container PRIVATE "-rdynamic")
