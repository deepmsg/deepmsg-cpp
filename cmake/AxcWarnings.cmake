# Warnings and error propagation policy. Applied only to axc_* targets, never to
# third_party/aeron (its build must stay byte-for-byte upstream).
function(axc_apply_warnings target)
    target_compile_options(${target} PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Werror
        -Werror=unused-result
        -Wshadow
        -Wno-unused-parameter
        -Wno-missing-field-initializers)
endfunction()
