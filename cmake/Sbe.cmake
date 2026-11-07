# SBE C++ codec generation.
#
# The five schemas live in the pinned aeron submodule (single source of truth);
# generated headers go to ${PROJECT_BINARY_DIR}/generated/axc/sbe and are never
# committed. sbe-tool is not vendored: scripts/fetch-sbe.sh downloads
# sbe-all-1.42.0.jar (the version aeron 1.54.0's gradle catalog uses) and checks
# its sha256.

if (AXC_WITH_SBE)
    if (NOT AXC_SBE_JAR)
        file(GLOB AXC_SBE_JAR_CANDIDATES "${PROJECT_SOURCE_DIR}/third_party/sbe-tool/sbe-all-*.jar")
        list(SORT AXC_SBE_JAR_CANDIDATES)
        list(LENGTH AXC_SBE_JAR_CANDIDATES AXC_SBE_JAR_COUNT)
        if (AXC_SBE_JAR_COUNT GREATER 0)
            list(GET AXC_SBE_JAR_CANDIDATES 0 AXC_SBE_JAR)
        endif ()
    endif ()

    if (NOT AXC_SBE_JAR OR NOT EXISTS "${AXC_SBE_JAR}")
        message(FATAL_ERROR
            "AXC_WITH_SBE=ON but no sbe-all jar found.\n"
            "Run scripts/fetch-sbe.sh, or configure with -DAXC_SBE_JAR=<path>, "
            "or -DAXC_WITH_SBE=OFF.")
    endif ()

    find_package(Java COMPONENTS Runtime REQUIRED)
endif ()

# axc_sbe_generate(<schema-file> <out-dir>) defines target axc_sbe_<name>.
function(axc_sbe_generate schema out_dir)
    get_filename_component(schema_name "${schema}" NAME_WE)
    set(stamp "${out_dir}/${schema_name}.stamp")

    add_custom_command(
        OUTPUT "${stamp}"
        COMMAND "${Java_JAVA_EXECUTABLE}"
                -Dsbe.target.language=Cpp
                -Dsbe.output.dir=${out_dir}
                -Dsbe.generate.ir=false
                -Dsbe.validation.stop.on.error=true
                -jar "${AXC_SBE_JAR}"
                "${schema}"
        COMMAND "${CMAKE_COMMAND}" -E touch "${stamp}"
        DEPENDS "${schema}" "${AXC_SBE_JAR}"
        COMMENT "SBE C++ codecs: ${schema_name}"
        VERBATIM)

    add_custom_target(axc_sbe_${schema_name} DEPENDS "${stamp}")

    set_property(GLOBAL APPEND PROPERTY AXC_SBE_TARGETS axc_sbe_${schema_name})
endfunction()
