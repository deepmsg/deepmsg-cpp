#pragma once

/// The SBE-generated archive codecs live in `io::aeron::archive::codecs`
/// (schemas `aeron-archive-codecs.xml` and `aeron-archive-mark-codecs.xml`).
/// Inside `deepmsg::archive::server` the name `io` would otherwise resolve to our
/// own `server::io` namespace, so the aliases below are the way to reach them.
namespace io::aeron::archive::codecs
{
}
namespace io::aeron::archive::codecs::mark
{
}

namespace deepmsg::archive::server
{

namespace archive_codecs = ::io::aeron::archive::codecs;
namespace archive_mark_codecs = ::io::aeron::archive::codecs::mark;

}  // namespace deepmsg::archive::server
