axc_add_library(axc_basic_auction_service
    SOURCES
        basic_auction_service.cpp
    DEPS
        axc_cluster_service)

axc_add_executable(basic-auction-service
    SOURCES
        main.cpp
    DEPS
        axc_basic_auction_service
        axc_cluster_service)

target_include_directories(axc_basic_auction_service PUBLIC "${PROJECT_SOURCE_DIR}")
