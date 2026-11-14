# Aeron 1.54.0 itself requires 3.30 (third_party/aeron/CMakeLists.txt:16).
cmake_minimum_required(VERSION 3.30)

project(deepmsg_cpp VERSION 0.1.0 LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Include root for our own targets. Nothing here sets a global language standard:
# third_party/aeron must keep building as C11/C++11 (see cmake/Aeron.cmake).
set(AXC_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")

include(AxcOptions)
include(AxcWarnings)
include(AxcTarget)
include(Sanitizers)
include(Pgo)
include(Uring)
include(Dpdk)
include(Aeron)
include(Sbe)

add_subdirectory(src)
add_subdirectory(apps)
add_subdirectory(tools)

if (AXC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif ()

if (AXC_BUILD_BENCHMARKS)
    add_subdirectory(bench)
endif ()

message(STATUS "deepmsg-cpp ${PROJECT_VERSION}: build type '${CMAKE_BUILD_TYPE}', "
               "sanitizer '${AXC_SANITIZER}', march '${AXC_MARCH}', "
               "tests ${AXC_BUILD_TESTS}, benchmarks ${AXC_BUILD_BENCHMARKS}")
