#pragma once

// T1: the read-only subset of the reference `io.aeron.archive.ArchiveTool`
// (`third_party/aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveTool.java`).
//
// The command line is the reference's: `archive-tool-ro <archive-dir> <command>
// [args]`. Exit codes match: `-1` (255) for a missing directory / invalid
// command / a failed `verify`, `0` otherwise.

namespace deepmsg::tools
{

/// `ArchiveTool.main` (`ArchiveTool.java:112`). `argc` includes the program
/// name, so the reference's `args.length` is `argc - 1`.
[[nodiscard]] int run_archive_tool_ro(int argc, char* argv[]);

}  // namespace deepmsg::tools
