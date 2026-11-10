axc_add_library(axc_sbe
    SOURCES
        module.cpp)

if (AXC_WITH_SBE)
    set(AXC_SBE_SCHEMA_ROOT "${PROJECT_SOURCE_DIR}/third_party/aeron")
    set(AXC_SBE_GEN_DIR "${PROJECT_BINARY_DIR}/generated/axc/sbe")
    file(MAKE_DIRECTORY "${AXC_SBE_GEN_DIR}")

    set(axc_sbe_schemas
        "${AXC_SBE_SCHEMA_ROOT}/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml"
        "${AXC_SBE_SCHEMA_ROOT}/aeron-archive/src/main/resources/archive/aeron-archive-mark-codecs.xml"
        "${AXC_SBE_SCHEMA_ROOT}/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml"
        "${AXC_SBE_SCHEMA_ROOT}/aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml"
        "${AXC_SBE_SCHEMA_ROOT}/aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml")

    foreach (schema IN LISTS axc_sbe_schemas)
        if (NOT EXISTS "${schema}")
            message(FATAL_ERROR
                "SBE schema not found: ${schema}\n"
                "third_party/aeron must be pinned at 454da1e4e6 (tag 1.54.0).")
        endif ()
        axc_sbe_generate("${schema}" "${AXC_SBE_GEN_DIR}")
    endforeach ()

    get_property(axc_sbe_targets GLOBAL PROPERTY AXC_SBE_TARGETS)
    add_custom_target(axc_sbe_codecs DEPENDS ${axc_sbe_targets})
    add_dependencies(axc_sbe axc_sbe_codecs)
    target_include_directories(axc_sbe PUBLIC "${AXC_SBE_GEN_DIR}")

    # C3c: `SessionEventCodecCompatibilityTest` encodes with a newer schema
    # version and decodes with an older one. The reference generates
    # `io.aeron.cluster.codecs.v5` / `v12` for its test source set
    # (`build.gradle`, `generateTestCodecsV5`); this is the same transform,
    # C++-side, used only by the unit tests.
    set(AXC_SBE_V5_GEN_DIR "${PROJECT_BINARY_DIR}/generated/axc/sbe-v5")
    file(MAKE_DIRECTORY "${AXC_SBE_V5_GEN_DIR}")
    axc_sbe_generate_with(
        "${AXC_SBE_SCHEMA_ROOT}/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml"
        "${AXC_SBE_V5_GEN_DIR}"
        "axc_sbe_cluster_codecs_v5"
        "-Dsbe.target.namespace=io.aeron.cluster.codecs.v5"
        "-Dsbe.schema.transform.version=*:5")
endif ()
