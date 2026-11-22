# Fuzz targets. Two modes, one source file per target:
#   * AXC_FUZZ=ON  -- libFuzzer + ASan/UBSan (clang only, the `fuzz` preset);
#   * AXC_FUZZ=OFF -- the standalone runner, registered with ctest so every
#     `scripts/check.sh` run replays the goldens and their truncations.

if (AXC_FUZZ AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "AXC_FUZZ=ON requires clang (libFuzzer)")
endif ()

function(axc_add_fuzz_target TARGET_NAME)
    cmake_parse_arguments(FUZZ "" "SEED_SUBDIR" "SOURCES;DEPS" ${ARGN})

    if (AXC_FUZZ)
        add_executable(${TARGET_NAME} ${FUZZ_SOURCES})
        axc_target_defaults(${TARGET_NAME})
        target_compile_options(${TARGET_NAME} PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
        target_link_options(${TARGET_NAME} PRIVATE -fsanitize=fuzzer,address,undefined)
    else ()
        add_executable(${TARGET_NAME} ${FUZZ_SOURCES} standalone_fuzz_main.cpp)
        axc_target_defaults(${TARGET_NAME})
    endif ()

    target_include_directories(${TARGET_NAME} PRIVATE
        "${PROJECT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}")
    target_compile_definitions(${TARGET_NAME} PRIVATE
        AXC_PROJECT_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
        AXC_FUZZ_SEED_SUBDIR="${FUZZ_SEED_SUBDIR}")
    target_link_libraries(${TARGET_NAME} PRIVATE ${FUZZ_DEPS})

    if (NOT AXC_FUZZ)
        add_test(NAME ${TARGET_NAME}_regression COMMAND ${TARGET_NAME})
        set_tests_properties(${TARGET_NAME}_regression PROPERTIES LABELS "unit;fuzz" TIMEOUT 180)
    endif ()
endfunction()

axc_add_fuzz_target(archive_control_fuzz
    SEED_SUBDIR ""
    SOURCES archive_control_fuzz.cpp
    DEPS axc_archive_server axc_core)

axc_add_fuzz_target(cluster_ingress_fuzz
    SEED_SUBDIR ""
    SOURCES cluster_ingress_fuzz.cpp
    DEPS axc_cluster_consensus axc_cluster_service axc_cluster_client axc_cluster_common axc_core)

axc_add_fuzz_target(recording_log_fuzz
    SEED_SUBDIR "test/golden/cluster"
    SOURCES recording_log_fuzz.cpp
    DEPS axc_cluster_common axc_core)

axc_add_fuzz_target(archive_catalog_fuzz
    SEED_SUBDIR "test/golden/archive"
    SOURCES archive_catalog_fuzz.cpp
    DEPS axc_archive_server axc_core)

axc_add_fuzz_target(dpdk_frame_fuzz
    SEED_SUBDIR "test/golden/dpdk/frame"
    SOURCES dpdk_frame_fuzz.cpp
    DEPS axc_driver axc_core)
