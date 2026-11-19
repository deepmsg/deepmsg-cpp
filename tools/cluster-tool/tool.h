#pragma once

// T2: the complete `io.aeron.cluster.ClusterTool` (`ClusterTool.java`,
// `ClusterToolOperator.java`, `ClusterToolCommand.java`) as one binary.
//
// The command line is the reference's: `<cluster-dir> <command> [args]`.
// The four `aeron.cluster.tool.*` JVM system properties arrive as
// `-Dname=value` arguments, anywhere on the line, the way every other binary
// in this workspace takes a property. `System.exit(-1)` becomes process status
// 255.

#include <string>
#include <vector>

namespace deepmsg::tools
{

/// `ClusterToolOperator.SUCCESS`.
inline constexpr int TOOL_SUCCESS = 0;
/// `ClusterToolOperator.FAILURE`; the reference's `System.exit(-1)` is 255.
inline constexpr int TOOL_FAILURE = -1;

/// Runs one command line, appending what the reference writes to `out`/`err`.
[[nodiscard]] int run_cluster_tool(const std::vector<std::string>& args, std::string& out, std::string& err);

/// `main`: the process shell around `run_cluster_tool`.
[[nodiscard]] int run_cluster_tool(int argc, char* argv[]);

}  // namespace deepmsg::tools
