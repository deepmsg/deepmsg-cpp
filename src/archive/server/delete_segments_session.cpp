#include "archive/server/delete_segments_session.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <system_error>

#include "archive/server/segment_files.h"

namespace deepmsg::archive::server
{

DeleteSegmentsSession::DeleteSegmentsSession(std::int64_t recording_id, std::int64_t control_session_id,
                                             std::int64_t correlation_id, std::string archive_dir,
                                             std::vector<std::string> files, DeleteSegmentsResponder& responder,
                                             DeleteSegmentsControl& control, bool await_replays_stop)
    : recording_id_(recording_id),
      control_session_id_(control_session_id),
      correlation_id_(correlation_id),
      archive_dir_(std::move(archive_dir)),
      files_(files.begin(), files.end()),
      responder_(&responder),
      control_(&control),
      max_delete_position_(std::numeric_limits<std::int64_t>::min()),
      state_(await_replays_stop ? State::await_replays_stop : State::delete_files)
{
    // `DeleteSegmentsSession.java:70-99`: the largest position in the file names.
    for (const std::string& name : files_)
    {
        std::int64_t file_position = 0;
        if (parse_segment_file_position_for_recording(name, recording_id, file_position))
        {
            max_delete_position_ = std::max(max_delete_position_, file_position);
        }
    }
}

int DeleteSegmentsSession::do_work()
{
    // `DeleteSegmentsSession.doWork` (`:122-135`).
    switch (state_)
    {
        case State::await_replays_stop:
            return do_await_replays_stop();
        case State::delete_files:
            return do_delete_files();
        case State::done:
            return 0;
    }

    return 0;
}

void DeleteSegmentsSession::close()
{
    // `:101-108`: the DELETE signal belongs to the close, not to the last delete.
    if (nullptr != control_)
    {
        control_->remove_delete_segments_session(recording_id_);
    }
    if (nullptr != responder_)
    {
        responder_->send_delete_signal(control_session_id_, correlation_id_, recording_id_);
    }
}

int DeleteSegmentsSession::do_await_replays_stop()
{
    // `:137-147`.
    if (nullptr != control_ && control_->has_in_progress_replays(recording_id_))
    {
        return 0;
    }

    state_ = State::delete_files;
    return 1;
}

int DeleteSegmentsSession::do_delete_files()
{
    // `:149-175`: one file per pass.
    if (files_.empty())
    {
        state_ = State::done;
        return 0;
    }

    const std::string name = files_.front();
    files_.pop_front();

    const std::filesystem::path path = std::filesystem::path(archive_dir_) / name;
    std::error_code error;
    if (!std::filesystem::remove(path, error))
    {
        if (std::filesystem::exists(path))
        {
            on_delete_error(name);
        }
        else if (name.size() < DELETE_SUFFIX.size() ||
                 name.compare(name.size() - DELETE_SUFFIX.size(), DELETE_SUFFIX.size(), DELETE_SUFFIX) != 0)
        {
            // The reference deletes the already-renamed `<name>.del`, which is
            // what an interrupted earlier delete leaves behind. It does not
            // rename the file here.
            const std::string renamed_name = name + std::string(DELETE_SUFFIX);
            const std::filesystem::path renamed = std::filesystem::path(archive_dir_) / renamed_name;
            std::error_code renamed_error;
            if (!std::filesystem::remove(renamed, renamed_error) && std::filesystem::exists(renamed))
            {
                on_delete_error(renamed_name);
            }
        }
    }

    return 1;
}

void DeleteSegmentsSession::on_delete_error(std::string_view file_name)
{
    // `:177-182`.
    const std::string message = "unable to delete segment file: " + std::string(file_name);
    if (nullptr != responder_)
    {
        responder_->send_error(control_session_id_, correlation_id_, message);
    }
    if (nullptr != control_)
    {
        control_->log_error(message);
    }
}

}  // namespace deepmsg::archive::server
