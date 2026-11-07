#pragma once

#include <cstdio>

#include "core/version.h"

// X0-1 placeholder entry point. Replaced by the real launch code in A5a/C4f.
inline int skeleton_main(const char* binary_name)
{
    std::printf("%s: skeleton (X0-1), deepmsg-cpp %s\n", binary_name, axc::core::version_string());
    return 0;
}
