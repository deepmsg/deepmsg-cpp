axc_add_executable(aeron-archive-server
    SOURCES
        main.cpp
    DEPS
        axc_archive_server
        aeron_driver_static)
