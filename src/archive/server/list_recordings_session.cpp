#include "archive/server/list_recordings_session.h"

#include "archive/server/catalog_index.h"
#include "archive/server/codecs.h"

namespace deepmsg::archive::server
{

ListRecordingsSession::ListRecordingsSession(const Catalog& catalog, ListRecordingsResponses& responses,
                                             const ListRecordingsConfig& config)
    : catalog_(catalog), responses_(responses), config_(config), recording_id_(config.from_recording_id)
{
}

int ListRecordingsSession::do_work()
{
    // `AbstractListRecordingsSession.doWork` (`AbstractListRecordingsSession.java:79-142`).
    if (is_done_)
    {
        return 0;
    }

    const CatalogIndex& catalog_index = catalog_.index();
    const int last_position = catalog_index.last_position();
    const std::span<const std::int64_t> index = catalog_index.index();
    int position = CatalogIndex::find(index, recording_id_, last_position);

    if (position < 0)
    {
        // `:88-102`: no exact hit (e.g. from `INT64_MIN`) ⇒ linear scan for the
        // first id at or above the cursor.
        for (int slot = 0; slot <= last_position; slot += 2)
        {
            if (index[static_cast<std::size_t>(slot)] >= recording_id_)
            {
                position = slot;
                break;
            }
        }
    }

    const int batch_start_position = position;
    for (int records_scanned = 0; sent_ < config_.count && records_scanned < MAX_SCANS_PER_WORK_CYCLE;
         records_scanned++)
    {
        const bool no_more_recordings = position < 0 || position > last_position;
        std::span<std::uint8_t> frame;
        // The `||` must short-circuit: `index[position + 1]` is out of bounds
        // when the scan has run past the last slot.
        const bool unavailable = no_more_recordings || catalog_.wrap_descriptor_at_offset(
                                                           frame, static_cast<std::int32_t>(index[position + 1])) < 0;

        if (unavailable)
        {
            responses_.send_recording_unknown(config_.control_session_id, config_.correlation_id,
                                              no_more_recordings ? recording_id_ : index[position]);
            is_done_ = true;
            break;
        }

        if (accept_descriptor(frame))
        {
            if (!responses_.send_descriptor(config_.control_session_id, config_.correlation_id, frame))
            {
                // Back-pressure: keep the cursor and retry on the next pass
                // (`:123-127`).
                is_done_ = responses_.control_session_is_done(config_.control_session_id);
                break;
            }
            sent_ += 1;
        }

        if (position < last_position)
        {
            recording_id_ = index[position + 2];
        }
        else
        {
            recording_id_ += 1;
        }
        position += 2;
    }

    if (sent_ == config_.count)
    {
        is_done_ = true;
    }

    return (position - batch_start_position) / 2;
}

void ListRecordingsSession::close()
{
    // `:141-145`.
    responses_.clear_active_listing(config_.control_session_id);
}

bool ListRecordingsSession::accept_descriptor(std::span<const std::uint8_t> frame) const
{
    if (!config_.filter_by_uri)
    {
        // `ListRecordingsSession.acceptDescriptor` (`:39-42`).
        return true;
    }

    // `ListRecordingsForUriSession.acceptDescriptor` (`:52-61`).
    archive_codecs::RecordingDescriptor descriptor;
    descriptor.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(frame.data())),
                             Catalog::DESCRIPTOR_HEADER_LENGTH, archive_codecs::RecordingDescriptor::SBE_BLOCK_LENGTH,
                             archive_codecs::RecordingDescriptor::SBE_SCHEMA_VERSION, frame.size());

    const auto fragment = std::span<const std::uint8_t>{
        reinterpret_cast<const std::uint8_t*>(config_.channel_fragment.data()), config_.channel_fragment.size()};
    return config_.stream_id == descriptor.streamId() && Catalog::original_channel_contains(frame, fragment);
}

}  // namespace deepmsg::archive::server
