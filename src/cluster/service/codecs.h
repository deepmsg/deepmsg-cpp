#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "cluster/common/cluster_codecs.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

namespace codecs = ::io::aeron::cluster::codecs;

// The guarded header and body wrappers are shared with the cluster client
// (C3a); the definition lives in `cluster/common/cluster_codecs.h`.
using common::message_header;
using common::MESSAGE_HEADER_LENGTH;
using common::SBE_SCHEMA_ID;
using common::wrap_body;

}  // namespace deepmsg::cluster::service
