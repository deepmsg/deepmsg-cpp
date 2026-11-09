#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <string_view>
#include <vector>

namespace deepmsg::archive::server
{

/// The control responses a delete session sends (`ControlSession.sendSignal` /
/// `ControlSession.sendErrorResponse`). Kept as an interface so a unit test can
/// drive the session without a publication; the conductor looks the session up
/// by id, exactly like the replay path does.
class DeleteSegmentsResponder
{
public:
    DeleteSegmentsResponder() = default;
    DeleteSegmentsResponder(const DeleteSegmentsResponder&) = delete;
    DeleteSegmentsResponder& operator=(const DeleteSegmentsResponder&) = delete;
    virtual ~DeleteSegmentsResponder() = default;

    virtual void send_delete_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                                    std::int64_t recording_id) = 0;
    virtual void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::string_view message) = 0;
};

/// The conductor side of a delete session (`ControlSession.archiveConductor()`).
/// An interface so a unit test can drive `DeleteSegmentsSession` without a
/// conductor.
class DeleteSegmentsControl
{
public:
    DeleteSegmentsControl() = default;
    DeleteSegmentsControl(const DeleteSegmentsControl&) = delete;
    DeleteSegmentsControl& operator=(const DeleteSegmentsControl&) = delete;
    virtual ~DeleteSegmentsControl() = default;

    /// `ArchiveConductor.hasInProgressReplays` (`ArchiveConductor.java:2226-2239`).
    [[nodiscard]] virtual bool has_in_progress_replays(std::int64_t recording_id) = 0;

    /// `ArchiveConductor.removeDeleteSegmentsSession` (`:1711-1714`).
    virtual void remove_delete_segments_session(std::int64_t recording_id) = 0;

    /// The reference's `errorHandler.onError(new ArchiveEvent(...))`.
    virtual void log_error(std::string_view message) = 0;
};

/// `DeleteSegmentsSession` (`DeleteSegmentsSession.java:29-177`): deletes the
/// segment files one at a time, waiting first for every replay of the recording
/// to stop, and sends the DELETE recording signal from `close()`.
class DeleteSegmentsSession
{
public:
    enum class State
    {
        await_replays_stop,
        delete_files,
        done
    };

    DeleteSegmentsSession(std::int64_t recording_id, std::int64_t control_session_id, std::int64_t correlation_id,
                          std::string archive_dir, std::vector<std::string> files, DeleteSegmentsResponder& responder,
                          DeleteSegmentsControl& control, bool await_replays_stop);

    DeleteSegmentsSession(const DeleteSegmentsSession&) = delete;
    DeleteSegmentsSession& operator=(const DeleteSegmentsSession&) = delete;

    /// `:113-120`: `files.isEmpty()`, not a flag.
    [[nodiscard]] bool is_done() const noexcept
    {
        return files_.empty();
    }

    /// `DeleteSegmentsSession.doWork` (`:122-135`).
    int do_work();

    /// `:101-108`: removes the session from the conductor and sends DELETE.
    void close();

    [[nodiscard]] std::int64_t recording_id() const noexcept
    {
        return recording_id_;
    }

    /// `maxDeletePosition` (`:70-99`), used by `startReplay`'s guard
    /// (`ArchiveConductor.java:880-886`).
    [[nodiscard]] std::int64_t max_delete_position() const noexcept
    {
        return max_delete_position_;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    /// The remaining file names, front first (test observable).
    [[nodiscard]] const std::deque<std::string>& files() const noexcept
    {
        return files_;
    }

private:
    int do_await_replays_stop();
    int do_delete_files();
    void on_delete_error(std::string_view file_name);

    std::int64_t recording_id_{0};
    std::int64_t control_session_id_{0};
    std::int64_t correlation_id_{0};
    std::string archive_dir_;
    std::deque<std::string> files_;
    DeleteSegmentsResponder* responder_{nullptr};
    DeleteSegmentsControl* control_{nullptr};
    std::int64_t max_delete_position_{0};
    State state_{State::await_replays_stop};
};

}  // namespace deepmsg::archive::server
