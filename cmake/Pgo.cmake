# Two-stage PGO.
#   1) cmake --preset release-pgo           (AXC_PGO=generate)
#   2) run the training workloads (bench grid); profiles land in AXC_PGO_DIR
#   3) cmake --preset release-pgo-use       (AXC_PGO=use)
set(AXC_PGO_DIR "${PROJECT_BINARY_DIR}/pgo" CACHE PATH "PGO profile directory")

function(axc_apply_pgo target)
    if (AXC_PGO STREQUAL "generate")
        target_compile_options(${target} PRIVATE "-fprofile-generate=${AXC_PGO_DIR}")
        target_link_options(${target} PRIVATE "-fprofile-generate=${AXC_PGO_DIR}")
    elseif (AXC_PGO STREQUAL "use")
        target_compile_options(${target} PRIVATE
            "-fprofile-use=${AXC_PGO_DIR}" -fprofile-correction -Wno-missing-profile)
        target_link_options(${target} PRIVATE "-fprofile-use=${AXC_PGO_DIR}" -fprofile-correction)
    elseif (NOT AXC_PGO STREQUAL "OFF")
        message(FATAL_ERROR "AXC_PGO='${AXC_PGO}' is not one of OFF|generate|use")
    endif ()
endfunction()
