#include "cluster/common/cluster_counters.h"

#include <cstring>
#include <string>

namespace deepmsg::cluster::common
{
namespace
{

void put_le32(std::uint8_t* data, std::int32_t value) noexcept
{
    const auto value_u = static_cast<std::uint32_t>(value);
    data[0] = static_cast<std::uint8_t>(value_u);
    data[1] = static_cast<std::uint8_t>(value_u >> 8);
    data[2] = static_cast<std::uint8_t>(value_u >> 16);
    data[3] = static_cast<std::uint8_t>(value_u >> 24);
}

struct FindContext
{
    std::int32_t type_id;
    std::int32_t cluster_id;
    std::int32_t found{AERON_NULL_COUNTER_ID};
};

void on_counter(int64_t /*value*/, int32_t id, int32_t type_id, const uint8_t* key, size_t key_length,
                const char* /*label*/, size_t /*label_length*/, void* clientd)
{
    auto* context = static_cast<FindContext*>(clientd);
    if (AERON_NULL_COUNTER_ID != context->found || type_id != context->type_id || key_length < sizeof(std::int32_t))
    {
        return;
    }

    std::int32_t counter_cluster_id = 0;
    std::memcpy(&counter_cluster_id, key, sizeof(counter_cluster_id));
    if (counter_cluster_id == context->cluster_id)
    {
        context->found = id;
    }
}

}  // namespace

std::array<std::uint8_t, sizeof(std::int32_t)> cluster_counter_key(std::int32_t cluster_id) noexcept
{
    std::array<std::uint8_t, sizeof(std::int32_t)> key{};
    put_le32(key.data(), cluster_id);
    return key;
}

std::array<std::uint8_t, 2 * sizeof(std::int32_t)> service_counter_key(std::int32_t cluster_id,
                                                                       std::int32_t service_id) noexcept
{
    std::array<std::uint8_t, 2 * sizeof(std::int32_t)> key{};
    put_le32(key.data(), cluster_id);
    put_le32(key.data() + sizeof(std::int32_t), service_id);
    return key;
}

std::array<std::uint8_t, sizeof(std::int32_t) + sizeof(std::int64_t)> session_counter_key(
    std::int32_t cluster_id, std::int64_t session_id) noexcept
{
    std::array<std::uint8_t, sizeof(std::int32_t) + sizeof(std::int64_t)> key{};
    put_le32(key.data(), cluster_id);
    std::memcpy(key.data() + sizeof(std::int32_t), &session_id, sizeof(session_id));
    return key;
}

std::string cluster_counter_label(std::string_view name, std::int32_t cluster_id)
{
    std::string label{name};
    label.append(CLUSTER_ID_LABEL_SUFFIX);
    label.append(std::to_string(cluster_id));
    return label;
}

std::string service_counter_label(std::string_view name, std::int32_t cluster_id, std::int32_t service_id)
{
    std::string label = cluster_counter_label(name, cluster_id);
    label.append(SERVICE_ID_LABEL_SUFFIX);
    label.append(std::to_string(service_id));
    return label;
}

std::string session_counter_label(std::string_view session_info, std::int32_t cluster_id)
{
    std::string label{"cluster-session: "};
    label.append(session_info);
    return cluster_counter_label(label, cluster_id);
}

std::string version_info(std::string_view version, std::string_view commit_hash)
{
    std::string info{" version="};
    info.append(version);
    info.append(" commit=");
    info.append(commit_hash);
    return info;
}

axc::core::AsyncAdd<axc::core::CounterPtr> allocate(axc::core::DeepmsgClient& client, std::string_view name,
                                                    std::int32_t type_id, std::int32_t cluster_id)
{
    const auto key = cluster_counter_key(cluster_id);
    const std::string label = cluster_counter_label(name, cluster_id);
    return client.add_counter(type_id, {key.data(), key.size()}, label);
}

axc::core::AsyncAdd<axc::core::CounterPtr> allocate_versioned(axc::core::DeepmsgClient& client, std::string_view name,
                                                              std::int32_t type_id, std::int32_t cluster_id,
                                                              std::string_view version, std::string_view commit_hash)
{
    const auto key = cluster_counter_key(cluster_id);
    std::string label = cluster_counter_label(name, cluster_id);
    label.append(version_info(version, commit_hash));
    return client.add_counter(type_id, {key.data(), key.size()}, label);
}

axc::core::AsyncAdd<axc::core::CounterPtr> allocate_service_counter(axc::core::DeepmsgClient& client,
                                                                    std::string_view name, std::int32_t type_id,
                                                                    std::int32_t cluster_id, std::int32_t service_id)
{
    const auto key = service_counter_key(cluster_id, service_id);
    const std::string label = service_counter_label(name, cluster_id, service_id);
    return client.add_counter(type_id, {key.data(), key.size()}, label);
}

axc::core::AsyncAdd<axc::core::CounterPtr> allocate_service_error_counter(axc::core::DeepmsgClient& client,
                                                                          std::int32_t cluster_id,
                                                                          std::int32_t service_id,
                                                                          std::string_view version,
                                                                          std::string_view commit_hash)
{
    const auto key = service_counter_key(cluster_id, service_id);
    std::string label = service_counter_label("Cluster Container Errors", cluster_id, service_id);
    label.append(version_info(version, commit_hash));
    return client.add_counter(CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID, {key.data(), key.size()}, label);
}

std::int32_t find_cluster_counter(aeron_counters_reader_t* reader, std::int32_t type_id,
                                  std::int32_t cluster_id) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }

    FindContext context{type_id, cluster_id};
    aeron_counters_reader_foreach_counter(reader, &on_counter, &context);
    return context.found;
}

}  // namespace deepmsg::cluster::common
