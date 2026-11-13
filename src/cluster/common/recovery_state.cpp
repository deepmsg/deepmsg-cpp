#include "cluster/common/recovery_state.h"

#include <cstring>
#include <string>
#include <vector>

namespace deepmsg::cluster::common
{
namespace
{

constexpr std::int64_t NULL_VALUE = -1;

void put_le32(std::uint8_t* buffer, std::int32_t value) noexcept
{
    const auto bits = static_cast<std::uint32_t>(value);
    buffer[0] = static_cast<std::uint8_t>(bits);
    buffer[1] = static_cast<std::uint8_t>(bits >> 8);
    buffer[2] = static_cast<std::uint8_t>(bits >> 16);
    buffer[3] = static_cast<std::uint8_t>(bits >> 24);
}

void put_le64(std::uint8_t* buffer, std::int64_t value) noexcept
{
    const auto bits = static_cast<std::uint64_t>(value);
    for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
    {
        buffer[i] = static_cast<std::uint8_t>(bits >> (8 * i));
    }
}

std::int32_t get_le32(const std::uint8_t* buffer) noexcept
{
    return static_cast<std::int32_t>(
        static_cast<std::uint32_t>(buffer[0]) | (static_cast<std::uint32_t>(buffer[1]) << 8) |
        (static_cast<std::uint32_t>(buffer[2]) << 16) | (static_cast<std::uint32_t>(buffer[3]) << 24));
}

std::int64_t get_le64(const std::uint8_t* buffer) noexcept
{
    std::uint64_t bits = 0;
    for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
    {
        bits |= static_cast<std::uint64_t>(buffer[i]) << (8 * i);
    }
    return static_cast<std::int64_t>(bits);
}

}  // namespace

std::expected<axc::core::AsyncAdd<axc::core::CounterPtr>, Error> RecoveryState::allocate(
    axc::core::DeepmsgClient& client, std::int64_t leadership_term_id, std::int64_t log_position,
    std::int64_t timestamp, std::int32_t cluster_id, std::span<const std::int64_t> snapshot_recording_ids)
{
    const std::size_t service_count = snapshot_recording_ids.size();
    const std::size_t key_length = SNAPSHOT_RECORDING_IDS_OFFSET + (service_count * sizeof(std::int64_t));
    if (key_length > AERON_COUNTER_MAX_KEY_LENGTH)
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error,
            std::to_string(key_length) + " exceeds max key length " + std::to_string(AERON_COUNTER_MAX_KEY_LENGTH)));
    }

    std::vector<std::uint8_t> key(key_length, 0);
    put_le64(key.data() + LEADERSHIP_TERM_ID_OFFSET, leadership_term_id);
    put_le64(key.data() + LOG_POSITION_OFFSET, log_position);
    put_le64(key.data() + TIMESTAMP_OFFSET, timestamp);
    put_le32(key.data() + CLUSTER_ID_OFFSET, cluster_id);
    put_le32(key.data() + SERVICE_COUNT_OFFSET, static_cast<std::int32_t>(service_count));
    for (std::size_t i = 0; i < service_count; i++)
    {
        put_le64(key.data() + SNAPSHOT_RECORDING_IDS_OFFSET + (i * sizeof(std::int64_t)), snapshot_recording_ids[i]);
    }

    std::string label{NAME};
    label.append(std::to_string(leadership_term_id));
    label.append(" logPosition=");
    label.append(std::to_string(log_position));
    label.append(" clusterId=");
    label.append(std::to_string(cluster_id));

    return client.add_counter(TYPE_ID, {key.data(), key.size()}, label);
}

std::expected<axc::core::AsyncAdd<axc::core::CounterPtr>, Error> RecoveryState::allocate(
    axc::core::DeepmsgClient& client, const RecordingLog::RecoveryPlan& plan, std::int32_t cluster_id,
    std::int64_t leadership_term_id)
{
    if (plan.snapshots.empty())
    {
        // `RecoveryState.allocate(aeron, leadershipTermId, 0, 0, clusterId)`.
        return allocate(client, leadership_term_id, 0, 0, cluster_id, {});
    }

    const RecordingLog::Snapshot& consensus_module_snapshot = plan.snapshots.front();
    std::vector<std::int64_t> service_snapshot_recording_ids(plan.snapshots.size() - 1, NULL_VALUE);
    for (std::size_t i = 1; i < plan.snapshots.size(); i++)
    {
        const RecordingLog::Snapshot& snapshot = plan.snapshots[i];
        if (snapshot.service_id < 0 ||
            static_cast<std::size_t>(snapshot.service_id) >= service_snapshot_recording_ids.size())
        {
            return std::unexpected(axc::core::make_error(
                axc::core::ErrorCode::protocol_error,
                "snapshot serviceId=" + std::to_string(snapshot.service_id) +
                    " invalid for serviceCount=" + std::to_string(service_snapshot_recording_ids.size())));
        }
        service_snapshot_recording_ids[static_cast<std::size_t>(snapshot.service_id)] = snapshot.recording_id;
    }

    return allocate(client, consensus_module_snapshot.leadership_term_id, consensus_module_snapshot.log_position,
                    consensus_module_snapshot.timestamp, cluster_id, service_snapshot_recording_ids);
}

bool RecoveryState::is_allocated(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    std::int32_t state = 0;
    std::int32_t type_id = 0;
    if (0 != aeron_counters_reader_counter_state(reader, counter_id, &state) ||
        0 != aeron_counters_reader_counter_type_id(reader, counter_id, &type_id))
    {
        return false;
    }
    return AERON_COUNTER_RECORD_ALLOCATED == state && TYPE_ID == type_id;
}

const std::uint8_t* RecoveryState::key_of(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    std::uint8_t* key = nullptr;
    if (0 != aeron_counters_reader_metadata_key(reader, counter_id, &key))
    {
        return nullptr;
    }
    return key;
}

std::int32_t RecoveryState::find_counter_id(aeron_counters_reader_t* reader, std::int32_t cluster_id) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }

    for (std::int32_t i = 0, size = aeron_counters_reader_max_counter_id(reader); i < size; i++)
    {
        std::int32_t state = 0;
        if (0 != aeron_counters_reader_counter_state(reader, i, &state))
        {
            continue;
        }

        if (AERON_COUNTER_RECORD_ALLOCATED == state)
        {
            if (!is_allocated(reader, i))
            {
                continue;
            }
            const std::uint8_t* key = key_of(reader, i);
            if (nullptr != key && get_le32(key + CLUSTER_ID_OFFSET) == cluster_id)
            {
                return i;
            }
        }
        else if (AERON_COUNTER_RECORD_UNUSED == state)
        {
            break;
        }
    }

    return AERON_NULL_COUNTER_ID;
}

std::int64_t RecoveryState::get_leadership_term_id(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    const std::uint8_t* key = is_allocated(reader, counter_id) ? key_of(reader, counter_id) : nullptr;
    return nullptr == key ? NULL_VALUE : get_le64(key + LEADERSHIP_TERM_ID_OFFSET);
}

std::int64_t RecoveryState::get_log_position(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    const std::uint8_t* key = is_allocated(reader, counter_id) ? key_of(reader, counter_id) : nullptr;
    return nullptr == key ? NULL_VALUE : get_le64(key + LOG_POSITION_OFFSET);
}

std::int64_t RecoveryState::get_timestamp(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    const std::uint8_t* key = is_allocated(reader, counter_id) ? key_of(reader, counter_id) : nullptr;
    return nullptr == key ? NULL_VALUE : get_le64(key + TIMESTAMP_OFFSET);
}

std::optional<std::int64_t> RecoveryState::get_snapshot_recording_id(aeron_counters_reader_t* reader,
                                                                     std::int32_t counter_id,
                                                                     std::int32_t service_id) noexcept
{
    if (!is_allocated(reader, counter_id))
    {
        return std::nullopt;
    }
    const std::uint8_t* key = key_of(reader, counter_id);
    if (nullptr == key)
    {
        return std::nullopt;
    }

    const std::int32_t service_count = get_le32(key + SERVICE_COUNT_OFFSET);
    if (service_id < 0 || service_id >= service_count)
    {
        return std::nullopt;
    }

    return get_le64(key + SNAPSHOT_RECORDING_IDS_OFFSET +
                    (static_cast<std::size_t>(service_id) * sizeof(std::int64_t)));
}

}  // namespace deepmsg::cluster::common
