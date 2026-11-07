# Build Aeron 1.54.0 from the pinned submodule (github.com/deepmsg/aeron @ 454da1e4e6).
#
# Rules:
#  * never patch the submodule; the gitlink is the whole contract;
#  * never set CMAKE_C_STANDARD / CMAKE_CXX_STANDARD globally -- aeron only sets
#    C++11/C11 when the variables are undefined, and our own C++23 must stay
#    target-scoped (see cmake/AxcTarget.cmake);
#  * only the targets we consume are requested.

set(AXC_AERON_DIR "${PROJECT_SOURCE_DIR}/third_party/aeron")

if (NOT EXISTS "${AXC_AERON_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
        "third_party/aeron is not initialized.\n"
        "Run: git submodule update --init --recursive")
endif ()

set(AERON_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_UNIT_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_SLOW_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
set(AERON_INSTALL_TARGETS OFF CACHE BOOL "" FORCE)
set(BUILD_AERON_DRIVER ON CACHE BOOL "" FORCE)
set(BUILD_AERON_ARCHIVE_API ON CACHE BOOL "" FORCE)

add_subdirectory("${AXC_AERON_DIR}" "${PROJECT_BINARY_DIR}/aeron" EXCLUDE_FROM_ALL)
