# liburing is optional: the io_uring write backend falls back to the sync backend
# at runtime when the kernel/seccomp forbids it. Here we only decide whether the
# backend can be compiled at all.
set(AXC_URING_FOUND FALSE CACHE INTERNAL "")

if (AXC_WITH_URING)
    find_path(AXC_URING_INCLUDE_DIR NAMES liburing.h)
    find_library(AXC_URING_LIBRARY NAMES uring)

    if (AXC_URING_INCLUDE_DIR AND AXC_URING_LIBRARY)
        add_library(axc_uring INTERFACE)
        target_include_directories(axc_uring INTERFACE "${AXC_URING_INCLUDE_DIR}")
        target_link_libraries(axc_uring INTERFACE "${AXC_URING_LIBRARY}")
        set(AXC_URING_FOUND TRUE CACHE INTERNAL "")
        message(STATUS "liburing found: io_uring backend available")
    else ()
        message(STATUS "liburing not found: io_uring backend disabled (sync backend only)")
    endif ()
endif ()
