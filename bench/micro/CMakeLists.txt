include(FetchContent)

FetchContent_Declare(
    google_benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.3
    GIT_SHALLOW TRUE)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(google_benchmark)

axc_add_executable(axc_micro_bench
    SOURCES
        smoke_bench.cpp
    DEPS
        axc_core
        benchmark::benchmark)

axc_add_executable(axc_catalog_list_for_uri_bench
    SOURCES
        catalog_list_for_uri.cpp
    DEPS
        axc_archive_server
        benchmark::benchmark)
