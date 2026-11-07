# axc_add_library(<name> SOURCES <src...> [DEPS <dep...>])
# axc_add_executable(<name> SOURCES <src...> [DEPS <dep...>])
#
# Every axc target gets: C++23 (target-scoped only), include root = src/,
# -march baseline, warnings, sanitizer and PGO flags. No global CMake language
# standard is ever set, so third_party/aeron keeps its own C++11/C11 settings.

function(axc_target_defaults target)
    target_include_directories(${target} PUBLIC "${AXC_SRC_DIR}")
    target_compile_features(${target} PUBLIC cxx_std_23)

    if (AXC_MARCH)
        target_compile_options(${target} PRIVATE "-march=${AXC_MARCH}")
    endif ()

    if (AXC_TRACING)
        target_compile_definitions(${target} PUBLIC AXC_TRACING=1)
    endif ()

    if (AXC_STAGE_TIMING)
        target_compile_definitions(${target} PUBLIC AXC_STAGE_TIMING=1)
    endif ()

    axc_apply_warnings(${target})
    axc_apply_sanitizer(${target})
    axc_apply_pgo(${target})
endfunction()

function(axc_add_library name)
    cmake_parse_arguments(ARG "" "" "SOURCES;DEPS" ${ARGN})

    if (NOT ARG_SOURCES)
        message(FATAL_ERROR "axc_add_library(${name}) requires SOURCES")
    endif ()

    add_library(${name} STATIC ${ARG_SOURCES})
    axc_target_defaults(${name})

    if (ARG_DEPS)
        target_link_libraries(${name} PUBLIC ${ARG_DEPS})
    endif ()
endfunction()

function(axc_add_executable name)
    cmake_parse_arguments(ARG "" "" "SOURCES;DEPS" ${ARGN})

    if (NOT ARG_SOURCES)
        message(FATAL_ERROR "axc_add_executable(${name}) requires SOURCES")
    endif ()

    add_executable(${name} ${ARG_SOURCES})
    axc_target_defaults(${name})
    # Allows binaries to share headers living outside src/ (e.g. apps/skeleton_main.h).
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}")

    if (ARG_DEPS)
        target_link_libraries(${name} PRIVATE ${ARG_DEPS})
    endif ()
endfunction()
