#pragma once

#include "core/config/name_table.h"

namespace deepmsg::cluster::common
{

/// The `aeron.cluster.*` name table (XC-6 baseline).
///
/// Transcribed from the 1.54.0 extraction in
/// `analysis/cpp/records/x0-6/config-names.tsv`: 82 names, 79 honoured and 3
/// accepted-but-ignored with a stated reason. Unknown `aeron.cluster.*` names
/// are reported by `NameTable::unknown_names`, never silently ignored.
[[nodiscard]] axc::core::NameTable cluster_name_table();

}  // namespace deepmsg::cluster::common
