option(AXC_BUILD_TESTS "Build unit and interop tests" ON)
option(AXC_BUILD_BENCHMARKS "Build micro benchmarks" OFF)

option(AXC_TRACING "Compile in binary tracing events (AXC_TRACING)" OFF)
option(AXC_STAGE_TIMING "Compile in rdtsc stage timing (AXC_STAGE_TIMING)" OFF)
option(AXC_WITH_URING "Use liburing for the archive io_uring write backend when found" ON)
option(AXC_WITH_SBE "Generate SBE C++ codecs from the pinned schemas at build time" ON)

set(AXC_SANITIZER "" CACHE STRING "Sanitizer: address | thread | undefined | (empty)")
set_property(CACHE AXC_SANITIZER PROPERTY STRINGS "" address thread undefined)

set(AXC_MARCH "x86-64-v2" CACHE STRING "Baseline -march for axc targets ('' to disable)")

# OFF | generate | use. See cmake/Pgo.cmake.
set(AXC_PGO "OFF" CACHE STRING "PGO mode")
set_property(CACHE AXC_PGO PROPERTY STRINGS OFF generate use)

# Empty means "resolve from third_party/sbe-tool/sbe-all-*.jar" (see scripts/fetch-sbe.sh).
set(AXC_SBE_JAR "" CACHE FILEPATH "Path to sbe-all-<version>.jar")
