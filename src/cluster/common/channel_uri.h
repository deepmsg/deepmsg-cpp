#pragma once

#include <string>
#include <string_view>

namespace deepmsg::cluster::common
{

/// The channel a response publication must use for a client's response channel.
///
/// `rejoin` is a subscription option: the Java and C++ cluster clients append
/// `|rejoin=false` to a UDP egress channel (`AeronCluster.java:1497-1501`).
/// The C driver keeps a publication that carries it permanently unconnected,
/// so it is stripped before the publication is added — on both sides that
/// publish to a client: the consensus module's `ClusterSession` and the
/// service container's `ContainerClientSession`.
[[nodiscard]] inline std::string publication_channel_from_response_channel(std::string_view response_channel)
{
    std::string channel{response_channel};
    const std::string rejoin = "rejoin=";
    const std::size_t at = channel.find(rejoin);
    if (std::string::npos == at || (0 != at && '|' != channel[at - 1] && '?' != channel[at - 1]))
    {
        return channel;
    }

    const std::size_t end = channel.find('|', at);
    if (std::string::npos == end)
    {
        channel.erase(0 == at ? at : at - 1);
    }
    else
    {
        channel.erase(at, end - at + 1);
    }
    return channel;
}

}  // namespace deepmsg::cluster::common
