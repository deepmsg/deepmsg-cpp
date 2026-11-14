axc_add_executable(deepmsg-archive-server
    SOURCES
        main.cpp
    DEPS
        axc_archive_server
        axc_driver
        aeron_driver_static)
