axc_add_library(axc_archive_io
    SOURCES
        module.cpp
        sync_writer.cpp
        mmap_reader.cpp
        pread_reader.cpp
    DEPS
        axc_core)

if (AXC_URING_FOUND)
    target_link_libraries(axc_archive_io PUBLIC axc_uring)
    target_compile_definitions(axc_archive_io PUBLIC AXC_HAVE_LIBURING=1)
endif ()
