#include "archive/server/archive_conductor.h"

#include <sched.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <span>

#include "core/counters/position_view.h"
#include "core/time/clock.h"

#include "archive/server/archive_counters.h"
#include "archive/server/channel_uri.h"
#include "archive/server/frame_layout.h"
#include "archive/server/recording_pos.h"
#include "archive/server/replication_client.h"
#include "archive/server/replication_validation.h"
#include "archive/server/segment_files.h"
#include "archive/server/segment_validation.h"
#include "core/deepmsg/image_handlers.h"
#include "core/io/fs.h"
#include "core/time/clock.h"

// The subscription's registration id has no public accessor in 1.54, and the
// internal header's `volatile` parameters trip a warning under -Werror (the
// group is spelled differently per compiler).
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-volatile"
#else
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wvolatile"
#endif
extern "C"
{
#include "aeron_subscription.h"
}
#if defined(__clang__)
#pragma clang diagnostic pop
#else
#pragma GCC diagnostic pop
#endif

namespace deepmsg::archive::server
{
namespace
{

/// `SemanticVersion.major`.
[[nodiscard]] std::int32_t major_version(std::int32_t version) noexcept
{
    return (version >> 16) & 0xFFFF;
}

[[nodiscard]] std::string version_to_string(std::int32_t version)
{
    return std::to_string((version >> 16) & 0xFFFF) + "." + std::to_string((version >> 8) & 0xFF) + "." +
           std::to_string(version & 0xFF);
}

/// `ArchiveException` error codes carried as the relevant id of an ERROR
/// response (`ArchiveException.java:29-84`); `GENERIC` (0) is the two-argument
/// `send_error_response` overload.
constexpr std::int64_t GENERIC = 0;
constexpr std::int64_t ACTIVE_LISTING = 1;
constexpr std::int64_t ACTIVE_RECORDING = 2;
constexpr std::int64_t ACTIVE_SUBSCRIPTION = 3;
constexpr std::int64_t UNKNOWN_SUBSCRIPTION = 4;
constexpr std::int64_t UNKNOWN_RECORDING = 5;
constexpr std::int64_t MAX_RECORDINGS = 8;
constexpr std::int64_t INVALID_EXTENSION = 9;
constexpr std::int64_t STORAGE_SPACE = 11;
constexpr std::int64_t EMPTY_RECORDING = 15;
constexpr std::int64_t INVALID_POSITION = 16;
/// `ArchiveException.UNKNOWN_REPLICATION` (`ArchiveException.java:89`).
constexpr std::int64_t UNKNOWN_REPLICATION = 12;
constexpr std::int64_t REPLICATION_CONNECTION_FAILURE = 14;
constexpr std::int64_t MAX_REPLAYS = 7;

/// `Aeron.NULL_VALUE` / `Aeron.NULL_POSITION` / `AERON_NULL_COUNTER_ID`.
constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int64_t NULL_VALUE = -1;
constexpr std::int32_t NULL_COUNTER_ID = -1;

/// `AeronArchive.REPLAY_ALL_AND_FOLLOW` / `REPLAY_ALL_AND_STOP`.
constexpr std::int64_t REPLAY_ALL_AND_FOLLOW = -1;
constexpr std::int64_t REPLAY_ALL_AND_STOP = -2;

/// `SourceLocation.LOCAL` (`SourceLocation.java`).
constexpr std::int32_t SOURCE_LOCATION_LOCAL = 0;

/// `ReplaySession`'s live view of a limit counter (`ReplaySession.java:544-578`):
/// the value is read by address, `is_closed` is the conductor's `CounterState`.
class CounterLimitSource final : public ReplayLimitSource
{
public:
    CounterLimitSource(aeron_counters_reader_t* reader, std::int32_t counter_id, bool& closed,
                       std::int64_t& registration_id) noexcept
        : reader_(reader), counter_id_(counter_id), closed_(closed), registration_id_(registration_id)
    {
    }

    [[nodiscard]] std::int64_t value() const noexcept override
    {
        return axc::core::PositionView::at(reader_, counter_id_).get_acquire();
    }

    [[nodiscard]] bool is_closed() const noexcept override
    {
        return closed_;
    }

    [[nodiscard]] bool registration_matches() const noexcept override
    {
        std::int64_t actual = 0;
        return nullptr != reader_ &&
               0 == aeron_counters_reader_counter_registration_id(reader_, counter_id_, &actual) &&
               actual == registration_id_;
    }

private:
    aeron_counters_reader_t* reader_{nullptr};
    std::int32_t counter_id_{-1};
    bool& closed_;
    std::int64_t& registration_id_;
};

/// The Java enum name `isValidDetach`'s error message prints
/// (`ArchiveConductor.java:2366`).
[[nodiscard]] std::string_view replay_state_name(ReplaySession::State state) noexcept
{
    switch (state)
    {
        case ReplaySession::State::init:
            return "INIT";
        case ReplaySession::State::replay:
            return "REPLAY";
        case ReplaySession::State::inactive:
            return "INACTIVE";
        case ReplaySession::State::done:
            return "DONE";
    }
    return "UNKNOWN";
}

}  // namespace

ArchiveConductor::ArchiveConductor(const ArchiveConductorContext& context)
    : context_(context),
      authenticator_(nullptr != context.authenticator ? context.authenticator : &default_authenticator_),
      adapter_(*this)
{
}

ArchiveConductor::~ArchiveConductor()
{
    close();
}

std::expected<std::unique_ptr<ArchiveConductor>, axc::core::Error> ArchiveConductor::create(
    const ArchiveConductorContext& context)
{
    auto conductor = std::unique_ptr<ArchiveConductor>(new ArchiveConductor(context));
    if (auto initialized = conductor->init(); !initialized)
    {
        return std::unexpected(initialized.error());
    }
    return conductor;
}

std::expected<void, axc::core::Error> ArchiveConductor::init()
{
    axc::core::ClientConfig config;
    config.dir = context_.aeron_dir;
    config.use_conductor_agent_invoker = true;
    config.on_unavailable_counter = &ArchiveConductor::unavailable_counter_trampoline;
    config.on_unavailable_counter_clientd = this;

    auto client = axc::core::DeepmsgClient::connect(config);
    if (!client)
    {
        return std::unexpected(client.error());
    }
    client_ = std::move(*client);

    if (0 != aeron_image_controlled_fragment_assembler_create(&control_assembler_,
                                                              &ArchiveConductor::assembled_trampoline, this))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "control fragment assembler create failed"));
    }

    axc::core::ImageHandlers handlers;
    handlers.on_unavailable = &ArchiveConductor::unavailable_image_trampoline;
    handlers.on_unavailable_clientd = this;

    // `Archive.java:692`: a disabled control channel leaves the local
    // (privileged) one only.
    if (context_.control_channel_enabled)
    {
        auto subscription = client_.add_subscription(context_.control_channel, context_.control_stream_id, handlers);
        for (int attempt = 0; attempt < 5000; attempt++)
        {
            auto polled = subscription.poll();
            if (!polled)
            {
                return std::unexpected(polled.error());
            }
            if (polled->has_value())
            {
                control_subscription_ = std::move(**polled);
                break;
            }

            // The driver has to answer the async add, so the conductor has to keep
            // turning between polls (`铁律 2`: never block inside an add).
            const int work = client_.do_work();
            (void)work;
            ::usleep(1000);
        }
    }

    if (context_.control_channel_enabled && !control_subscription_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "control subscription did not complete"));
    }

    // `ArchiveConductor.java:238-239`: the local (privileged) control channel.
    auto local_subscription =
        client_.add_subscription(context_.local_control_channel, context_.local_control_stream_id, handlers);
    for (int attempt = 0; attempt < 5000; attempt++)
    {
        auto polled = local_subscription.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            local_control_subscription_ = std::move(**polled);
            break;
        }
        (void)client_.do_work();
        ::usleep(1000);
    }

    if (!local_control_subscription_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "local control subscription did not complete"));
    }

    cached_clock_.update(epoch_clock_.time_ms());

    // `DedicatedModeArchiveConductor.newRecorder`/`newReplayer`: the worker
    // halves hand sessions over through the two inboxes and take them back
    // through the close queues.
    if (is_dedicated())
    {
        recorder_.set_queues(&recording_inbox_, &recording_close_queue_);
        replayer_.set_queues(&replay_inbox_, &replay_close_queue_);
    }

    // `Archive.Context.conclude` (`Archive.java:1570-1700`): the catalog and the
    // four archive-level counters, allocated before the conductor serves.
    CatalogOptions catalog_options;
    catalog_options.archive_dir = context_.archive_dir;
    catalog_options.file_sync_level = context_.catalog_file_sync_level;
    catalog_options.catalog_capacity = context_.catalog_capacity;
    catalog_options.clock = &epoch_clock_;

    auto catalog = Catalog::create(catalog_options);
    if (!catalog)
    {
        return std::unexpected(catalog.error());
    }
    catalog_ = std::move(*catalog);

    // `Archive.java:1656-1667`: one recorder max-write-time counter per archive.
    if (AERON_NULL_COUNTER_ID !=
        ArchiveCounters::find(client_.counters_reader(), ArchiveCounters::MAX_WRITE_TIME_TYPE_ID, context_.archive_id))
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error,
            "existing max write time counter detected for archiveId=" + std::to_string(context_.archive_id)));
    }

    const auto archive_id_key = ArchiveCounters::archive_id_key(context_.archive_id);

    // `Archive.java:1626-1631`: the archive control sessions count.
    auto control_sessions =
        allocate_counter(ArchiveCounters::CONTROL_SESSIONS_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::CONTROL_SESSIONS_NAME, context_.archive_id));
    if (!control_sessions)
    {
        return std::unexpected(control_sessions.error());
    }
    control_sessions_counter_.emplace(std::move(*control_sessions));

    auto max_write_time =
        allocate_counter(ArchiveCounters::MAX_WRITE_TIME_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::MAX_WRITE_TIME_NAME, context_.archive_id));
    if (!max_write_time)
    {
        return std::unexpected(max_write_time.error());
    }
    max_write_time_counter_.emplace(std::move(*max_write_time));

    auto total_write_bytes =
        allocate_counter(ArchiveCounters::TOTAL_WRITE_BYTES_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::TOTAL_WRITE_BYTES_NAME, context_.archive_id));
    if (!total_write_bytes)
    {
        return std::unexpected(total_write_bytes.error());
    }
    total_write_bytes_counter_.emplace(std::move(*total_write_bytes));

    auto total_write_time =
        allocate_counter(ArchiveCounters::TOTAL_WRITE_TIME_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::TOTAL_WRITE_TIME_NAME, context_.archive_id));
    if (!total_write_time)
    {
        return std::unexpected(total_write_time.error());
    }
    total_write_time_counter_.emplace(std::move(*total_write_time));

    auto recording_sessions = allocate_counter(
        ArchiveCounters::RECORDING_SESSION_COUNT_TYPE_ID,
        std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
        ArchiveCounters::make_label(ArchiveCounters::RECORDING_SESSION_COUNT_NAME, context_.archive_id));
    if (!recording_sessions)
    {
        return std::unexpected(recording_sessions.error());
    }
    recording_sessions_counter_.emplace(std::move(*recording_sessions));

    recorder_.set_counters(&*total_write_bytes_counter_, &*total_write_time_counter_, &*max_write_time_counter_);

    // `Archive.java:1644-1730`: the replay session count and the three
    // replayer read metrics.
    auto replay_sessions =
        allocate_counter(ArchiveCounters::REPLAY_SESSION_COUNT_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::REPLAY_SESSION_COUNT_NAME, context_.archive_id));
    if (!replay_sessions)
    {
        return std::unexpected(replay_sessions.error());
    }
    replay_session_counter_.emplace(std::move(*replay_sessions));

    auto max_read_time = allocate_counter(
        ArchiveCounters::REPLAYER_MAX_READ_TIME_TYPE_ID,
        std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
        ArchiveCounters::make_label(ArchiveCounters::REPLAYER_MAX_READ_TIME_NAME, context_.archive_id));
    if (!max_read_time)
    {
        return std::unexpected(max_read_time.error());
    }
    max_read_time_counter_.emplace(std::move(*max_read_time));

    auto total_read_bytes = allocate_counter(
        ArchiveCounters::REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
        std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
        ArchiveCounters::make_label(ArchiveCounters::REPLAYER_TOTAL_READ_BYTES_NAME, context_.archive_id));
    if (!total_read_bytes)
    {
        return std::unexpected(total_read_bytes.error());
    }
    total_read_bytes_counter_.emplace(std::move(*total_read_bytes));

    auto total_read_time = allocate_counter(
        ArchiveCounters::REPLAYER_TOTAL_READ_TIME_TYPE_ID,
        std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
        ArchiveCounters::make_label(ArchiveCounters::REPLAYER_TOTAL_READ_TIME_NAME, context_.archive_id));
    if (!total_read_time)
    {
        return std::unexpected(total_read_time.error());
    }
    total_read_time_counter_.emplace(std::move(*total_read_time));

    replayer_.set_counters(&*total_read_bytes_counter_, &*total_read_time_counter_, &*max_read_time_counter_);

    // `Archive.java:1470-1545`: the conductor tracker exists in every mode; the
    // recorder/replayer ones only in DEDICATED.
    if (auto allocated = allocate_duty_cycle(conductor_duty_cycle_, "archive-conductor",
                                             context_.threading.conductor_cycle_threshold_ns);
        !allocated)
    {
        return std::unexpected(allocated.error());
    }
    // `ArchiveConductor.onStart` (`ArchiveConductor.java:252`): the conductor's
    // tracker is anchored before the first cycle, otherwise its first
    // `measureAndUpdate` reports `nanoTime() - 0` — the machine's uptime — as
    // the max cycle (measured: 6.1e13 ns on a 17-hour box, against the
    // reference's ~2.5e7). The recorder and replayer anchor themselves in
    // `on_start_threaded`.
    conductor_duty_cycle_.tracker.update(axc::core::NanoClock::now());
    if (is_dedicated())
    {
        if (auto allocated = allocate_duty_cycle(recorder_duty_cycle_, "archive-recorder",
                                                 context_.threading.recorder_cycle_threshold_ns);
            !allocated)
        {
            return std::unexpected(allocated.error());
        }
        if (auto allocated = allocate_duty_cycle(replayer_duty_cycle_, "archive-replayer",
                                                 context_.threading.replayer_cycle_threshold_ns);
            !allocated)
        {
            return std::unexpected(allocated.error());
        }
        recorder_.set_duty_cycle_tracker(&recorder_duty_cycle_.tracker);
        replayer_.set_duty_cycle_tracker(&replayer_duty_cycle_.tracker);
    }

    if (context_.recording.recording_events_enabled)
    {
        // `ArchiveConductor.java:224-225` and `Archive.Context` validation in
        // `parse_recording_settings` (channel required when enabled).
        auto async = client_.add_exclusive_publication(context_.recording.recording_events_channel,
                                                       context_.recording.recording_events_stream_id);
        for (int attempt = 0; attempt < 5000; attempt++)
        {
            auto polled = async.poll();
            if (!polled)
            {
                return std::unexpected(polled.error());
            }
            if (polled->has_value())
            {
                auto handle = std::move(**polled);
                events_publication_ = std::make_unique<DeepmsgExclusivePublication>(handle.release());
                events_.emplace(*events_publication_);
                break;
            }
            (void)client_.do_work();
            ::usleep(1000);
        }

        if (!events_)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "recording events publication did not complete"));
        }
    }

    return {};
}

std::expected<axc::core::PositionWriter, axc::core::Error> ArchiveConductor::allocate_counter(
    std::int32_t type_id, std::span<const std::uint8_t> key, std::string_view label)
{
    // Two-phase (`铁律 2`), driven by the client's own conductor while waiting.
    auto async = client_.add_counter(type_id, key, label);
    for (int attempt = 0; attempt < 5000; attempt++)
    {
        auto polled = async.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            return axc::core::PositionWriter{std::move(**polled)};
        }

        (void)client_.do_work();
        ::usleep(1000);
    }

    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "counter add did not complete"));
}

void ArchiveConductor::close()
{
    // The subscriptions close asynchronously and may deliver an
    // unavailable-image callback, so the adapter must forget the sessions
    // before they are destroyed.
    adapter_.clear_sessions();
    local_control_subscription_.reset();
    control_subscription_.reset();

    // Sessions next: their writers own segment files and their counters.
    if (is_dedicated())
    {
        // The runtime has already stopped the recorder/replayer runners
        // (`DedicatedModeArchiveConductor.closeSessionWorkers`, `:96-116`): hand
        // their remaining sessions back and close them on this thread.
        recorder_.close_threaded();
        replayer_.close_threaded();
        while (!recording_close_queue_.empty() || !replay_close_queue_.empty())
        {
            (void)process_close_queue();
        }
    }
    else
    {
        recorder_.close();
        replayer_.close();
    }
    replay_publication_sessions_ = SessionWorker<CreateReplayPublicationSession>{};
    replication_sessions_ = SessionWorker<ReplicationSession>{};
    replication_session_by_id_.clear();
    list_recordings_sessions_ = SessionWorker<ListRecordingsSession>{};
    delete_sessions_ = SessionWorker<DeleteSegmentsSession>{};
    delete_session_by_recording_id_.clear();
    sessions_ = SessionWorker<ControlSession>{};
    publications_.clear();
    if (nullptr != control_assembler_)
    {
        (void)aeron_image_controlled_fragment_assembler_delete(control_assembler_);
        control_assembler_ = nullptr;
    }
    if (!catalog_.is_closed())
    {
        (void)catalog_.close();
    }
    events_.reset();
    events_publication_.reset();

    // Everything that owns a client resource must be released before the client
    // itself: `aeron_close` deletes every registration, and a later close of the
    // same handle is a use-after-free.
    recording_subscriptions_.clear();
    recording_subscription_keys_.clear();
    recording_subscription_order_.clear();
    pending_recording_subscriptions_.clear();
    extend_outcomes_.clear();
    pending_control_counters_.clear();
    pending_publications_.clear();
    control_session_counters_.clear();
    replay_session_by_id_.clear();
    replay_tokens_.clear();
    counter_by_id_.clear();

    control_sessions_counter_.reset();
    recording_sessions_counter_.reset();
    replay_session_counter_.reset();
    max_read_time_counter_.reset();
    total_read_bytes_counter_.reset();
    total_read_time_counter_.reset();
    total_write_time_counter_.reset();
    total_write_bytes_counter_.reset();
    max_write_time_counter_.reset();
    // The three duty-cycle tracker pairs (103/104) own counters too.
    for (DutyCycleTrackers* trackers : {&conductor_duty_cycle_, &recorder_duty_cycle_, &replayer_duty_cycle_})
    {
        trackers->max_cycle_time.reset();
        trackers->threshold_exceeded.reset();
        trackers->tracker = axc::core::DutyCycleTracker{};
    }
    client_.close();
}

int ArchiveConductor::do_work()
{
    // `ArchiveConductor.doWork` (`ArchiveConductor.java:363-395`) for the control
    // plane: drive the client conductor, read the control subscription, run the
    // sessions.
    cached_clock_.update(epoch_clock_.time_ms());
    conductor_duty_cycle_.tracker.measure_and_update(axc::core::NanoClock::now());

    int work_count = is_dedicated() ? process_close_queue() : 0;
    work_count += client_.do_work();
    const int control_work = poll_control_subscription();
    work_count += control_work;
    work_count += poll_pending_publications();
    work_count += poll_pending_recording_subscriptions();
    work_count += poll_pending_control_counters();
    work_count += poll_recording_images();
    work_count += poll_listings();
    work_count += list_recordings_sessions_.do_work([](ListRecordingsSession& session) { session.close(); });
    check_replay_tokens();
    work_count += replay_publication_sessions_.do_work([](CreateReplayPublicationSession&) {});
    if (!is_dedicated())
    {
        work_count += replayer_.do_work([this](ReplaySession& session) { close_replay_session(session); });
    }
    work_count += delete_sessions_.do_work([](DeleteSegmentsSession& session) { session.close(); });
    work_count += replication_sessions_.do_work(
        [this](ReplicationSession& session)
        {
            replication_session_by_id_.erase(session.session_id());
            session.close();
        });
    if (!is_dedicated())
    {
        work_count += recorder_.do_work([this](RecordingSession& session) { close_recording_session(session); });
    }
    work_count += sessions_.do_work(
        [this](ControlSession& session)
        {
            adapter_.unregister_session(session.session_id());
            sessions_by_id_.erase(session.session_id());
            remove_replay_tokens_for_session(session.session_id());

            // `ControlSessionAdapter.removeControlSession` (`:1167-1171`).
            if (control_sessions_counter_)
            {
                control_sessions_counter_->set_value(control_sessions_counter_->value() - 1);
            }
            control_session_counters_.erase(session.session_id());
            for (std::size_t index = 0; index < pending_control_counters_.size();)
            {
                if (pending_control_counters_[index].control_session_id == session.session_id())
                {
                    pending_control_counters_.erase(pending_control_counters_.begin() +
                                                    static_cast<std::ptrdiff_t>(index));
                }
                else
                {
                    index++;
                }
            }
            for (Listing& listing : listings_)
            {
                if (listing.control_session_id == session.session_id())
                {
                    listing.done = true;
                }
            }
            publications_.erase(session.session_id());

            // A session that ended before its publication add completed must not
            // leave the async registration behind: the client conductor walks
            // it on close.
            for (std::size_t index = 0; index < pending_publications_.size();)
            {
                if (pending_publications_[index].control_session_id == session.session_id())
                {
                    if (0 != aeron_async_add_exclusive_publication_cancel(client_.handle(),
                                                                          pending_publications_[index].async))
                    {
                        log_warning("failed to cancel the control publication add of an ended session");
                    }
                    pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
                }
                else
                {
                    index++;
                }
            }
        });

    return work_count;
}

int ArchiveConductor::poll_control_subscription()
{
    int work_count = 0;

    // `ArchiveConductor.doWork` polls both subscriptions through the adapter
    // (`:363-395`); the local one is privileged and shares the same sessions map.
    aeron_subscription_t* subscriptions[] = {control_subscription_.get(), local_control_subscription_.get()};
    for (aeron_subscription_t* subscription : subscriptions)
    {
        if (nullptr == subscription)
        {
            continue;
        }

        const int image_count = aeron_subscription_image_count(subscription);
        for (int index = 0; index < image_count; index++)
        {
            aeron_image_t* image = aeron_subscription_image_at_index(subscription, static_cast<std::size_t>(index));
            if (nullptr == image)
            {
                continue;
            }

            current_control_image_ = image;
            work_count +=
                aeron_image_controlled_poll(image, &aeron_image_controlled_fragment_assembler_handler,
                                            control_assembler_, static_cast<std::size_t>(context_.fragment_limit));
            current_control_image_ = nullptr;
            aeron_image_release(image);
        }
    }

    return work_count;
}

aeron_controlled_fragment_handler_action_t ArchiveConductor::assembled_trampoline(void* clientd,
                                                                                  const std::uint8_t* buffer,
                                                                                  size_t length, aeron_header_t* header)
{
    (void)header;
    auto* conductor = static_cast<ArchiveConductor*>(clientd);
    conductor->adapter_.on_fragment(buffer, static_cast<std::int32_t>(length), conductor->current_control_image_);
    return AERON_ACTION_CONTINUE;
}

void ArchiveConductor::unavailable_counter_trampoline(void* clientd, aeron_counters_reader_t* reader,
                                                      std::int64_t registration_id, std::int32_t counter_id)
{
    // `ArchiveConductor.onUnavailableCounter` (`:260-270`): the cached `Counter`
    // object is closed, which a bounded replay's `isClosed()` observes.
    (void)reader;
    (void)registration_id;
    static_cast<ArchiveConductor*>(clientd)->mark_counter_closed(counter_id);
}

void ArchiveConductor::unavailable_image_trampoline(void* clientd, aeron_subscription_t* subscription,
                                                    aeron_image_t* image)
{
    (void)subscription;
    auto* conductor = static_cast<ArchiveConductor*>(clientd);
    conductor->log_warning("control image unavailable");
    conductor->adapter_.abort_sessions_for_image(image, "control image unavailable");
}

ControlSession* ArchiveConductor::on_connect_request(const ConnectRequest& request)
{
    // `ArchiveConductor.newControlSession` (`ArchiveConductor.java:447-520`).
    std::string invalid_version_message;
    if (major_version(request.protocol_version) != PROTOCOL_MAJOR_VERSION)
    {
        invalid_version_message = "invalid client version " + version_to_string(request.protocol_version) +
                                  ", archive is " + version_to_string(PROTOCOL_SEMANTIC_VERSION);
    }

    ControlSession::Config config;
    config.control_session_id = next_session_id_++;
    config.correlation_id = request.correlation_id;
    config.connect_timeout_ms = context_.connect_timeout_ms;
    config.session_liveness_check_interval_ms = context_.session_liveness_check_interval_ms;
    config.invalid_version_message = std::move(invalid_version_message);

    // `ArchiveConductor.newControlSession` (`ArchiveConductor.java:457-482`):
    // strip the client's channel, then apply the archive's term length, sparse
    // flag and MTU (the channel's own values win), plus the correlation id for
    // an MDS response channel.
    ChannelUriOptions options;
    options.term_length = context_.control_term_buffer_length;
    options.mtu_length = context_.control_mtu_length;
    options.sparse = context_.control_term_buffer_sparse;

    aeron_image_constants_t image_constants{};
    if (nullptr != request.image &&
        0 == aeron_image_constants(const_cast<aeron_image_t*>(static_cast<const aeron_image_t*>(request.image)),
                                   &image_constants) &&
        is_mds_response_channel(request.response_channel))
    {
        options.has_response_correlation_id = true;
        options.response_correlation_id = image_constants.correlation_id;
    }

    auto response_channel = control_response_channel(request.response_channel, options);
    if (!response_channel)
    {
        log_warning("failed to rewrite the control response channel " + request.response_channel + ": " +
                    std::string{response_channel.error().message.view()});
        return nullptr;
    }

    // `ControlSession.init` adds the publication asynchronously and waits for it
    // in INIT (`ControlSession.java:859-908`); the add is started here and
    // completed in `poll_pending_publications`.
    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (0 != aeron_async_add_exclusive_publication(&async, client_.handle(), response_channel->c_str(),
                                                   request.response_stream_id))
    {
        log_warning("failed to add the control publication on " + *response_channel);
        return nullptr;
    }

    auto session = std::make_unique<ControlSession>(config, proxy_, *authenticator_, cached_clock_);
    ControlSession* raw_session = session.get();

    sessions_by_id_[config.control_session_id] = raw_session;
    pending_publications_.push_back(PendingPublication{config.control_session_id, async});
    sessions_.add(std::move(session));

    // `ArchiveConductor.java:519`: the control sessions counter.
    if (control_sessions_counter_)
    {
        control_sessions_counter_->set_value(control_sessions_counter_->value() + 1);
    }

    authenticator_->on_connect_request(config.control_session_id, request.encoded_credentials, cached_clock_.time_ms());

    // `ArchiveConductor.newControlSession` (`ArchiveConductor.java:502-508`):
    // the type-113 control-session counter, labelled with the client info.
    std::string client_info = request.client_info;
    aeron_image_constants_t counter_image_constants{};
    if (nullptr != request.image &&
        0 == aeron_image_constants(const_cast<aeron_image_t*>(static_cast<const aeron_image_t*>(request.image)),
                                   &counter_image_constants))
    {
        std::string image_info = "sourceIdentity=";
        image_info += nullptr == counter_image_constants.source_identity ? "" : counter_image_constants.source_identity;
        image_info += " sessionId=" + std::to_string(counter_image_constants.session_id);
        if (!client_info.empty())
        {
            client_info += " ";
        }
        client_info += image_info;
    }

    const auto counter_key = ArchiveCounters::control_session_key(context_.archive_id, config.control_session_id);
    PendingControlCounter pending_counter;
    pending_counter.control_session_id = config.control_session_id;
    pending_counter.add =
        client_.add_counter(ArchiveCounters::CONTROL_SESSION_TYPE_ID, {counter_key.data(), counter_key.size()},
                            ArchiveCounters::control_session_label(client_info, context_.archive_id));
    pending_control_counters_.push_back(std::move(pending_counter));

    return raw_session;
}

int ArchiveConductor::poll_pending_publications()
{
    int work_count = 0;

    for (std::size_t index = 0; index < pending_publications_.size();)
    {
        PendingPublication& pending = pending_publications_[index];
        aeron_exclusive_publication_t* publication = nullptr;

        // `aeron_client_conductor_async_resource_poll`: 0 = still awaiting the
        // driver, 1 = registered (complete), -1 = error or timeout.
        const int add_result = aeron_async_add_exclusive_publication_poll(&publication, pending.async);
        if (add_result < 0)
        {
            log_warning(std::string{"control publication add failed: "} + aeron_errmsg());
            if (0 != aeron_async_add_exclusive_publication_cancel(client_.handle(), pending.async))
            {
                log_warning("failed to cancel the control publication add");
            }
            pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
            continue;
        }

        if (1 != add_result || nullptr == publication)
        {
            index++;
            continue;
        }

        auto wrapper = std::make_unique<DeepmsgExclusivePublication>(publication);
        const auto found = sessions_by_id_.find(pending.control_session_id);
        if (found != sessions_by_id_.end())
        {
            found->second->set_publication(wrapper.get());
            publications_[pending.control_session_id] = std::move(wrapper);
        }

        pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
        work_count++;
    }

    return work_count;
}

ControlSession* ArchiveConductor::control_session_by_id(std::int64_t id) noexcept
{
    const auto found = sessions_by_id_.find(id);
    return found == sessions_by_id_.end() ? nullptr : found->second;
}

void ArchiveConductor::increment_recording_sessions() noexcept
{
    if (recording_sessions_counter_)
    {
        recording_sessions_counter_->set_value(recording_sessions_counter_->value() + 1);
    }
}

void ArchiveConductor::decrement_recording_sessions() noexcept
{
    if (recording_sessions_counter_)
    {
        recording_sessions_counter_->set_value(recording_sessions_counter_->value() - 1);
    }
}

std::expected<void, axc::core::Error> ArchiveConductor::allocate_duty_cycle(DutyCycleTrackers& trackers,
                                                                            std::string_view role,
                                                                            std::int64_t threshold_ns)
{
    // `Archive.java:1477-1487`: two counters per agent, the labels carry the
    // role, the threshold and the configured threading mode.
    const std::string mode{threading_mode_name(context_.threading.mode)};
    const std::string max_label =
        ArchiveCounters::make_label(std::string{role} + " max cycle time in ns: " + mode, context_.archive_id);
    const std::string exceeded_label = ArchiveCounters::make_label(
        std::string{role} + " work cycle time exceeded count: threshold=" + format_duration_ns(threshold_ns) + " " +
            mode,
        context_.archive_id);
    const auto key = ArchiveCounters::archive_id_key(context_.archive_id);

    auto max_counter = allocate_counter(ArchiveCounters::MAX_CYCLE_TIME_TYPE_ID,
                                        std::span<const std::uint8_t>{key.data(), key.size()}, max_label);
    if (!max_counter)
    {
        return std::unexpected(max_counter.error());
    }
    trackers.max_cycle_time.emplace(std::move(*max_counter));

    auto exceeded = allocate_counter(ArchiveCounters::CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                                     std::span<const std::uint8_t>{key.data(), key.size()}, exceeded_label);
    if (!exceeded)
    {
        return std::unexpected(exceeded.error());
    }
    trackers.threshold_exceeded.emplace(std::move(*exceeded));

    trackers.tracker =
        axc::core::DutyCycleTracker(&*trackers.max_cycle_time, &*trackers.threshold_exceeded, threshold_ns);
    return {};
}

std::int64_t ArchiveConductor::recording_sessions_counter_value() const noexcept
{
    return recording_sessions_counter_ ? recording_sessions_counter_->value() : -1;
}

void ArchiveConductor::on_start_recording(const StartRecordingRequest& request)
{
    // `ArchiveConductor.startRecording` (`ArchiveConductor.java:530-583`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    const auto max_recordings = static_cast<std::size_t>(context_.recording.max_concurrent_recordings);
    if (recording_subscriptions_.size() >= max_recordings || sessions_by_recording_id_.size() >= max_recordings)
    {
        session->send_error_response(
            request.correlation_id, MAX_RECORDINGS,
            "max concurrent recordings reached at " + std::to_string(context_.recording.max_concurrent_recordings));
        return;
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        session->send_error_response(
            request.correlation_id, STORAGE_SPACE,
            "low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
                " <= usableSpace=" + std::to_string(usable));
        return;
    }

    auto key = make_key(request.stream_id, request.channel);
    if (!key)
    {
        session->send_error_response(request.correlation_id, std::string{key.error().message.view()});
        return;
    }

    if (recording_subscriptions_.find(*key) != recording_subscriptions_.end())
    {
        session->send_error_response(
            request.correlation_id, ACTIVE_SUBSCRIPTION,
            "recording exists for streamId=" + std::to_string(request.stream_id) + " channel=" + request.channel);
        return;
    }

    auto stripped = stripped_channel(request.channel);
    if (!stripped)
    {
        session->send_error_response(request.correlation_id, std::string{stripped.error().message.view()});
        return;
    }

    const std::string subscription_channel =
        SOURCE_LOCATION_LOCAL == request.source_location && is_udp_channel(request.channel) ? "aeron-spy:" + *stripped
                                                                                            : *stripped;

    PendingRecordingSubscription pending;
    pending.key = *key;
    pending.stripped_channel = *stripped;
    pending.original_channel = request.channel;
    pending.subscription_channel = subscription_channel;
    pending.stream_id = request.stream_id;
    pending.control_session_id = session->session_id();
    pending.correlation_id = request.correlation_id;
    pending.auto_stop = request.auto_stop;
    pending.subscription_add = client_.add_subscription(subscription_channel, request.stream_id, {});
    pending_recording_subscriptions_.push_back(std::move(pending));
}

int ArchiveConductor::poll_pending_recording_subscriptions()
{
    int work_count = 0;

    for (auto it = pending_recording_subscriptions_.begin(); it != pending_recording_subscriptions_.end();)
    {
        auto polled = it->subscription_add.poll();
        if (!polled)
        {
            const std::string message{polled.error().message.view()};
            log_warning("recording subscription add failed: " + message);
            if (auto* session = control_session_by_id(it->control_session_id))
            {
                session->send_error_response(it->correlation_id, message);
            }
            if (0 != it->extend_token)
            {
                extend_outcomes_[it->extend_token] = ExtendRecordingOutcome{0, message};
            }
            it = pending_recording_subscriptions_.erase(it);
            continue;
        }

        if (!polled->has_value())
        {
            ++it;
            continue;
        }

        auto handle = std::move(**polled);
        const std::int64_t registration_id = handle->registration_id;

        RecordingSubscription subscription;
        subscription.key = it->key;
        subscription.stripped_channel = it->stripped_channel;
        subscription.original_channel = it->original_channel;
        subscription.subscription_channel = it->subscription_channel;
        subscription.stream_id = it->stream_id;
        subscription.control_session_id = it->control_session_id;
        subscription.correlation_id = it->correlation_id;
        subscription.auto_stop = it->auto_stop;
        subscription.extend = it->extend;
        subscription.extend_recording_id = it->extend_recording_id;
        subscription.registration_id = handle->registration_id;
        subscription.ref_count = 1;
        subscription.subscription = std::move(handle);

        recording_subscription_keys_[subscription.registration_id] = subscription.key;
        recording_subscription_order_.push_back(subscription.key);
        recording_subscriptions_[subscription.key] = std::move(subscription);

        // The OK carries the registration id and is sent before any image
        // arrives (`ArchiveConductor.java:570`, `:1139`).
        if (auto* session = control_session_by_id(it->control_session_id))
        {
            session->send_ok_response(it->correlation_id, registration_id);
        }
        if (0 != it->extend_token)
        {
            extend_outcomes_[it->extend_token] = ExtendRecordingOutcome{registration_id, {}};
        }

        it = pending_recording_subscriptions_.erase(it);
        work_count++;
    }

    return work_count;
}

void ArchiveConductor::on_extend_recording(const ExtendRecordingRequest& request)
{
    // `ArchiveConductor.extendRecording` (`ArchiveConductor.java:1067-1157`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    // The wire path has no waiter for the registration id (the OK carries it);
    // the replication path keeps the returned token.
    (void)extend_recording(request.correlation_id, request.recording_id, request.stream_id, request.source_location,
                           request.auto_stop, request.channel, *session);
}

std::expected<std::int64_t, std::string> ArchiveConductor::extend_recording(
    std::int64_t correlation_id, std::int64_t recording_id, std::int32_t stream_id, std::int32_t source_location,
    bool auto_stop, std::string_view original_channel, ControlSession& session)
{
    // `ArchiveConductor.extendRecording` (`ArchiveConductor.java:1067-1151`).
    // The reference sends the ERROR (and returns the same message) on every
    // refusal below; the OK with the registration id goes out from
    // `poll_pending_recording_subscriptions` once the driver answers
    // (`ArchiveConductor.java:1139`).
    const auto max_recordings = static_cast<std::size_t>(context_.recording.max_concurrent_recordings);
    if (recording_subscriptions_.size() >= max_recordings || sessions_by_recording_id_.size() >= max_recordings)
    {
        const std::string msg =
            "max concurrent recordings reached at " + std::to_string(context_.recording.max_concurrent_recordings);
        session.send_error_response(correlation_id, MAX_RECORDINGS, msg);
        return std::unexpected(msg);
    }

    if (!catalog_.has_recording(recording_id))
    {
        const std::string msg = "unknown recording id: " + std::to_string(recording_id);
        session.send_error_response(correlation_id, UNKNOWN_RECORDING, msg);
        return std::unexpected(msg);
    }

    const auto summary = catalog_.recording_summary(recording_id);
    if (stream_id != summary.stream_id)
    {
        const std::string msg = "cannot extend recording " + std::to_string(summary.recording_id) +
                                " with streamId=" + std::to_string(stream_id) +
                                " for existing streamId=" + std::to_string(summary.stream_id);
        session.send_error_response(correlation_id, UNKNOWN_RECORDING, msg);
        return std::unexpected(msg);
    }

    if (sessions_by_recording_id_.find(recording_id) != sessions_by_recording_id_.end())
    {
        const std::string msg = "cannot extend active recording " + std::to_string(recording_id);
        session.send_error_response(correlation_id, ACTIVE_RECORDING, msg);
        return std::unexpected(msg);
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        const std::string msg =
            "low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
            " <= usableSpace=" + std::to_string(usable);
        session.send_error_response(correlation_id, STORAGE_SPACE, msg);
        return std::unexpected(msg);
    }

    auto key = make_key(stream_id, original_channel);
    if (!key)
    {
        const std::string msg{key.error().message.view()};
        session.send_error_response(correlation_id, msg);
        return std::unexpected(msg);
    }

    if (recording_subscriptions_.find(*key) != recording_subscriptions_.end())
    {
        const std::string msg =
            "recording exists for streamId=" + std::to_string(stream_id) + " channel=" + std::string{original_channel};
        session.send_error_response(correlation_id, ACTIVE_SUBSCRIPTION, msg);
        return std::unexpected(msg);
    }

    auto stripped = stripped_channel(original_channel);
    if (!stripped)
    {
        const std::string msg{stripped.error().message.view()};
        session.send_error_response(correlation_id, msg);
        return std::unexpected(msg);
    }

    const std::string subscription_channel =
        SOURCE_LOCATION_LOCAL == source_location && is_udp_channel(original_channel) ? "aeron-spy:" + *stripped
                                                                                     : *stripped;

    const std::int64_t token = next_extend_token_++;

    PendingRecordingSubscription pending;
    pending.key = *key;
    pending.stripped_channel = *stripped;
    pending.original_channel = std::string{original_channel};
    pending.subscription_channel = subscription_channel;
    pending.stream_id = stream_id;
    pending.control_session_id = session.session_id();
    pending.correlation_id = correlation_id;
    pending.auto_stop = auto_stop;
    pending.extend = true;
    pending.extend_recording_id = recording_id;
    pending.extend_token = token;
    pending.subscription_add = client_.add_subscription(subscription_channel, stream_id, {});
    pending_recording_subscriptions_.push_back(std::move(pending));

    return token;
}

std::optional<ExtendRecordingOutcome> ArchiveConductor::take_extend_recording_outcome(std::int64_t token)
{
    const auto found = extend_outcomes_.find(token);
    if (found == extend_outcomes_.end())
    {
        return std::nullopt;
    }

    ExtendRecordingOutcome outcome = std::move(found->second);
    extend_outcomes_.erase(found);
    return outcome;
}

void ArchiveConductor::cancel_extend_recording(std::int64_t token)
{
    extend_outcomes_.erase(token);

    const auto found =
        std::find_if(pending_recording_subscriptions_.begin(), pending_recording_subscriptions_.end(),
                     [token](const PendingRecordingSubscription& pending) { return pending.extend_token == token; });
    if (found != pending_recording_subscriptions_.end())
    {
        // The AsyncAdd destructor cancels the driver request, so no OK goes out.
        pending_recording_subscriptions_.erase(found);
    }
}

int ArchiveConductor::extend_recording_session(RecordingSubscription& subscription, aeron_image_t* image)
{
    // `ArchiveConductor.extendRecordingSession` (`ArchiveConductor.java:2059-2170`).
    const std::int64_t recording_id = subscription.extend_recording_id;
    auto* control = control_session_by_id(subscription.control_session_id);

    if (sessions_by_recording_id_.find(recording_id) != sessions_by_recording_id_.end())
    {
        if (nullptr != control)
        {
            control->send_error_response(subscription.correlation_id, ACTIVE_RECORDING,
                                         "cannot extend active recording " + std::to_string(recording_id));
        }
        return 0;
    }

    if (sessions_by_recording_id_.size() >= static_cast<std::size_t>(context_.recording.max_concurrent_recordings))
    {
        if (nullptr != control)
        {
            control->send_error_response(
                subscription.correlation_id, MAX_RECORDINGS,
                "max concurrent recordings reached at " + std::to_string(context_.recording.max_concurrent_recordings));
        }
        return 0;
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        if (nullptr != control)
        {
            control->send_error_response(
                subscription.correlation_id, STORAGE_SPACE,
                "low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
                    " <= usableSpace=" + std::to_string(usable));
        }
        return 0;
    }

    aeron_image_constants_t constants{};
    if (0 != aeron_image_constants(image, &constants))
    {
        log_warning(std::string{"image constants unavailable: "} + aeron_errmsg());
        return 0;
    }

    const auto summary = catalog_.recording_summary(recording_id);

    // `validateImageForExtendRecording` (`ArchiveConductor.java:2190-2242`).
    struct Field
    {
        std::int64_t image_value;
        std::int64_t summary_value;
        const char* name;
    };
    const Field fields[] = {
        {constants.join_position, summary.stop_position, "joinPosition"},
        {constants.initial_term_id, summary.initial_term_id, "initialTermId"},
        {static_cast<std::int64_t>(constants.term_buffer_length), summary.term_buffer_length, "termBufferLength"},
        {static_cast<std::int64_t>(constants.mtu_length), summary.mtu_length, "mtuLength"},
    };
    for (const Field& field : fields)
    {
        if (field.image_value != field.summary_value)
        {
            if (nullptr != control)
            {
                control->send_error_response(subscription.correlation_id, INVALID_EXTENSION,
                                             "cannot extend recording " + std::to_string(recording_id) + " image." +
                                                 field.name + "=" + std::to_string(field.image_value) + " != rec." +
                                                 field.name + "=" + std::to_string(field.summary_value));
            }
            return 0;
        }
    }

    const std::string source_identity = nullptr == constants.source_identity ? "" : constants.source_identity;
    const auto position_key =
        RecordingPos::make_key(context_.archive_id, recording_id, constants.session_id, source_identity);
    const auto position_label = RecordingPos::make_label(context_.archive_id, recording_id, constants.session_id,
                                                         subscription.stream_id, subscription.stripped_channel);

    auto counter = allocate_counter(RecordingPos::TYPE_ID, position_key.view(), position_label);
    if (!counter)
    {
        log_warning(std::string{"recording position counter failed: "} + std::string{counter.error().message.view()});
        return 0;
    }

    axc::core::PositionWriter position = std::move(*counter);
    position.set_release(constants.join_position);
    reset_counter_state(position.id(), position.registration_id());

    RecordingWriterContext writer_context;
    writer_context.archive_dir = context_.archive_dir;
    writer_context.file_sync_level = context_.recording.file_sync_level;
    writer_context.archive_dir_fd = -1;
    writer_context.io_fallocate = context_.recording.file_io_fallocate;

    const auto term_buffer_length = static_cast<std::int32_t>(constants.term_buffer_length);

    RecordingSession::Config config;
    config.correlation_id = subscription.correlation_id;
    config.recording_id = recording_id;
    config.control_session_id = subscription.control_session_id;
    config.subscription_registration_id = subscription.registration_id;
    config.start_position = summary.start_position;
    config.join_position = constants.join_position;
    config.segment_length = summary.segment_file_length;
    config.term_length = term_buffer_length;
    config.block_length_limit = std::min(term_buffer_length, context_.recording.file_io_max_length);
    config.session_id = constants.session_id;
    config.stream_id = subscription.stream_id;
    config.image = image;
    config.auto_stop = subscription.auto_stop;
    config.original_channel = subscription.original_channel;
    config.stripped_channel = subscription.stripped_channel;
    config.source_identity = source_identity;
    config.events = events_ ? &*events_ : nullptr;

    RecordingWriter writer(recording_id, summary.start_position, summary.segment_file_length, term_buffer_length,
                           constants.join_position, std::move(writer_context), &recorder_);

    auto session = std::make_unique<RecordingSession>(
        std::move(config), std::move(writer), std::make_unique<axc::core::PositionWriterTarget>(std::move(position)),
        std::make_unique<AeronRecordingImage>(image));

    (void)catalog_.extend_recording(recording_id, subscription.control_session_id, subscription.correlation_id,
                                    constants.session_id);

    sessions_by_image_[image] = session.get();
    sessions_by_recording_id_[recording_id] = session.get();
    add_recording_session(std::move(session));
    subscription.ref_count++;
    increment_recording_sessions();

    if (nullptr != control)
    {
        control->send_signal(subscription.correlation_id, recording_id, subscription.registration_id,
                             constants.join_position, archive_codecs::RecordingSignal::EXTEND);
    }

    return 1;
}

int ArchiveConductor::poll_pending_control_counters()
{
    int work_count = 0;

    for (auto it = pending_control_counters_.begin(); it != pending_control_counters_.end();)
    {
        auto polled = it->add.poll();
        if (!polled)
        {
            log_warning(std::string{"control session counter failed: "} + std::string{polled.error().message.view()});
            it = pending_control_counters_.erase(it);
            continue;
        }

        if (!polled->has_value())
        {
            ++it;
            continue;
        }

        control_session_counters_.emplace(it->control_session_id, axc::core::PositionWriter{std::move(**polled)});
        it = pending_control_counters_.erase(it);
        work_count++;
    }

    return work_count;
}

int ArchiveConductor::poll_listings()
{
    int work_count = 0;

    for (auto it = listings_.begin(); it != listings_.end();)
    {
        Listing& listing = *it;
        ControlSession* control = control_session_by_id(listing.control_session_id);
        if (nullptr == control || control->is_done())
        {
            listing.done = true;
        }

        if (!listing.done && Listing::Kind::recording_by_id == listing.kind)
        {
            // `ListRecordingByIdSession.doWork` (`ListRecordingByIdSession.java:72-96`).
            const auto frame = catalog_.wrap_descriptor(listing.recording_id);
            if (frame.empty())
            {
                control->send_recording_unknown(listing.correlation_id, listing.recording_id);
                listing.done = true;
            }
            else if (control->send_descriptor(listing.correlation_id, frame))
            {
                listing.done = true;
            }
            work_count++;
        }
        else if (!listing.done && Listing::Kind::update_channel == listing.kind)
        {
            // `UpdateChannelSession.doWork` (`UpdateChannelSession.java:73-133`).
            auto replaced = catalog_.replace_recording_channels(listing.recording_id, listing.stripped_channel,
                                                                listing.original_channel);
            if (!replaced)
            {
                if (axc::core::ErrorCode::not_found == replaced.error().code)
                {
                    control->send_recording_unknown(listing.correlation_id, listing.recording_id);
                }
                else
                {
                    control->send_error_response(listing.correlation_id, std::string{replaced.error().message.view()});
                }
            }
            else
            {
                control->send_ok_response(listing.correlation_id, 0);
            }

            listing.done = true;
            work_count++;
        }
        else if (!listing.done)
        {
            // `ListRecordingSubscriptionsSession.doWork` (`ListRecordingSubscriptionsSession.java:96-131`).
            std::int32_t cursor = listing.pseudo_index;
            while (cursor < static_cast<std::int32_t>(recording_subscription_order_.size()) && !listing.done)
            {
                const std::string& key = recording_subscription_order_[static_cast<std::size_t>(cursor)];
                const auto subscription = recording_subscriptions_.find(key);
                if (subscription == recording_subscriptions_.end())
                {
                    cursor++;
                    continue;
                }

                const RecordingSubscription& candidate = subscription->second;
                if ((listing.apply_stream_id && candidate.stream_id != listing.stream_id) ||
                    candidate.subscription_channel.find(listing.channel_fragment) == std::string::npos)
                {
                    cursor++;
                    continue;
                }

                if (!control->send_subscription_descriptor(listing.correlation_id, candidate.registration_id,
                                                           candidate.stream_id, candidate.subscription_channel))
                {
                    // Back-pressure: retry the same subscription on a later pass.
                    work_count++;
                    break;
                }

                cursor++;
                listing.sent++;
                work_count++;
                if (listing.sent >= listing.subscription_count)
                {
                    listing.done = true;
                }
            }
            listing.pseudo_index = cursor;

            if (!listing.done &&
                listing.pseudo_index >= static_cast<std::int32_t>(recording_subscription_order_.size()))
            {
                control->send_subscription_unknown(listing.correlation_id);
                listing.done = true;
                work_count++;
            }
        }

        if (listing.done)
        {
            if (nullptr != control)
            {
                control->set_active_listing(false);
            }
            it = listings_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    return work_count;
}

void ArchiveConductor::on_find_last_matching_recording(const FindLastMatchingRecordingRequest& request)
{
    // `ArchiveConductor.findLastMatchingRecording` (`ArchiveConductor.java:742-761`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (request.min_recording_id < 0)
    {
        session->send_error_response(request.correlation_id, UNKNOWN_RECORDING,
                                     "minRecordingId=" + std::to_string(request.min_recording_id) + " < 0");
        return;
    }

    const auto fragment = std::span<const std::uint8_t>{
        reinterpret_cast<const std::uint8_t*>(request.channel_fragment.data()), request.channel_fragment.size()};
    const std::int64_t recording_id =
        catalog_.find_last(request.min_recording_id, request.session_id, request.stream_id, fragment);

    // Not found is -1, which the client expects.
    session->send_ok_response(request.correlation_id, recording_id);
}

void ArchiveConductor::on_list_recording(const RecordingIdRequest& request)
{
    // `ArchiveConductor.listRecording` (`ArchiveConductor.java:688-706`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (session->has_active_listing())
    {
        session->send_error_response(request.correlation_id, ACTIVE_LISTING, "active listing already in progress");
        return;
    }

    if (!catalog_.has_recording(request.recording_id))
    {
        session->send_recording_unknown(request.correlation_id, request.recording_id);
        return;
    }

    Listing listing;
    listing.kind = Listing::Kind::recording_by_id;
    listing.correlation_id = request.correlation_id;
    listing.control_session_id = session->session_id();
    listing.recording_id = request.recording_id;
    listings_.push_back(std::move(listing));
    session->set_active_listing(true);
}

void ArchiveConductor::on_list_recording_subscriptions(const ListRecordingSubscriptionsRequest& request)
{
    // `ArchiveConductor.listRecordingSubscriptions` (`ArchiveConductor.java:1266-1299`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (session->has_active_listing())
    {
        session->send_error_response(request.correlation_id, ACTIVE_LISTING, "active listing already in progress");
        return;
    }

    if (request.pseudo_index < 0 ||
        request.pseudo_index >= static_cast<std::int32_t>(recording_subscriptions_.size()) ||
        request.subscription_count <= 0)
    {
        session->send_subscription_unknown(request.correlation_id);
        return;
    }

    Listing listing;
    listing.kind = Listing::Kind::subscriptions;
    listing.correlation_id = request.correlation_id;
    listing.control_session_id = session->session_id();
    listing.pseudo_index = request.pseudo_index;
    listing.subscription_count = request.subscription_count;
    listing.apply_stream_id = request.apply_stream_id;
    listing.stream_id = request.stream_id;
    listing.channel_fragment = request.channel_fragment;
    listings_.push_back(std::move(listing));
    session->set_active_listing(true);
}

int ArchiveConductor::poll_recording_images()
{
    int work_count = 0;
    std::vector<aeron_image_t*> seen;

    for (auto& [key, subscription] : recording_subscriptions_)
    {
        (void)key;
        if (!subscription.subscription)
        {
            continue;
        }

        const int image_count = aeron_subscription_image_count(subscription.subscription.get());
        for (int index = 0; index < image_count; index++)
        {
            aeron_image_t* image =
                aeron_subscription_image_at_index(subscription.subscription.get(), static_cast<std::size_t>(index));
            if (nullptr == image)
            {
                continue;
            }

            seen.push_back(image);
            if (sessions_by_image_.find(image) == sessions_by_image_.end())
            {
                work_count += subscription.extend ? extend_recording_session(subscription, image)
                                                  : start_recording_session(subscription, image);
            }

            aeron_image_release(image);
        }
    }

    // A session whose image is no longer in the subscription has no stream left
    // to record; it is aborted before anything can touch the freed image.
    for (auto it = sessions_by_image_.begin(); it != sessions_by_image_.end();)
    {
        if (std::find(seen.begin(), seen.end(), it->first) == seen.end())
        {
            it->second->abort("image unavailable");
            it = sessions_by_image_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    return work_count;
}

int ArchiveConductor::start_recording_session(RecordingSubscription& subscription, aeron_image_t* image)
{
    // `ArchiveConductor.startRecordingSession` (`ArchiveConductor.java:1998-2075`).
    aeron_image_constants_t constants{};
    if (0 != aeron_image_constants(image, &constants))
    {
        log_warning(std::string{"image constants unavailable: "} + aeron_errmsg());
        return 0;
    }

    if (sessions_by_recording_id_.size() >= static_cast<std::size_t>(context_.recording.max_concurrent_recordings))
    {
        log_warning("max concurrent recordings reached at " +
                    std::to_string(context_.recording.max_concurrent_recordings));
        return 0;
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        log_warning("low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
                    " <= usableSpace=" + std::to_string(usable));
        return 0;
    }

    const auto term_buffer_length = static_cast<std::int32_t>(constants.term_buffer_length);
    const auto segment_file_length = std::max(context_.recording.segment_file_length, term_buffer_length);
    const std::string source_identity = nullptr == constants.source_identity ? "" : constants.source_identity;

    auto recording_id = catalog_.add_new_recording(
        constants.join_position, epoch_clock_.time_ms(), constants.initial_term_id, segment_file_length,
        term_buffer_length, static_cast<std::int32_t>(constants.mtu_length), constants.session_id,
        subscription.stream_id, subscription.stripped_channel, subscription.original_channel, source_identity);
    if (!recording_id)
    {
        log_warning(std::string{"catalog addNewRecording failed: "} + std::string{recording_id.error().message.view()});
        return 0;
    }

    // `RecordingPos.allocate` (`RecordingPos.java:93-132`), two-phase.
    const auto position_key =
        RecordingPos::make_key(context_.archive_id, *recording_id, constants.session_id, source_identity);
    const auto position_label = RecordingPos::make_label(context_.archive_id, *recording_id, constants.session_id,
                                                         subscription.stream_id, subscription.stripped_channel);

    auto counter = allocate_counter(RecordingPos::TYPE_ID, position_key.view(), position_label);
    if (!counter)
    {
        log_warning(std::string{"recording position counter failed: "} + std::string{counter.error().message.view()});
        (void)catalog_.recording_stopped(*recording_id, constants.join_position, epoch_clock_.time_ms());
        return 0;
    }

    axc::core::PositionWriter position = std::move(*counter);
    position.set_release(constants.join_position);
    reset_counter_state(position.id(), position.registration_id());

    RecordingWriterContext writer_context;
    writer_context.archive_dir = context_.archive_dir;
    writer_context.file_sync_level = context_.recording.file_sync_level;
    writer_context.archive_dir_fd = -1;
    writer_context.io_fallocate = context_.recording.file_io_fallocate;

    RecordingSession::Config config;
    config.correlation_id = subscription.correlation_id;
    config.recording_id = *recording_id;
    config.control_session_id = subscription.control_session_id;
    config.subscription_registration_id = subscription.registration_id;
    config.start_position = constants.join_position;
    config.join_position = constants.join_position;
    config.segment_length = segment_file_length;
    config.term_length = term_buffer_length;
    config.block_length_limit = std::min(term_buffer_length, context_.recording.file_io_max_length);
    config.session_id = constants.session_id;
    config.stream_id = subscription.stream_id;
    config.image = image;
    config.auto_stop = subscription.auto_stop;
    config.original_channel = subscription.original_channel;
    config.stripped_channel = subscription.stripped_channel;
    config.source_identity = source_identity;
    config.events = events_ ? &*events_ : nullptr;

    RecordingWriter writer(*recording_id, constants.join_position, segment_file_length, term_buffer_length,
                           constants.join_position, std::move(writer_context), &recorder_);

    auto session = std::make_unique<RecordingSession>(
        std::move(config), std::move(writer), std::make_unique<axc::core::PositionWriterTarget>(std::move(position)),
        std::make_unique<AeronRecordingImage>(image));

    sessions_by_image_[image] = session.get();
    sessions_by_recording_id_[*recording_id] = session.get();
    add_recording_session(std::move(session));
    subscription.ref_count++;
    increment_recording_sessions();

    // `ArchiveConductor.java:2046-2051`: the START signal at the join position.
    if (auto* control = control_session_by_id(subscription.control_session_id))
    {
        control->send_signal(subscription.correlation_id, *recording_id, subscription.registration_id,
                             constants.join_position, archive_codecs::RecordingSignal::START);
    }

    return 1;
}

std::optional<ArchiveConductor::RecordingSubscription> ArchiveConductor::remove_recording_subscription(
    std::int64_t registration_id)
{
    // `ArchiveConductor.removeRecordingSubscription` (`ArchiveConductor.java:2174-2188`).
    const auto key = recording_subscription_keys_.find(registration_id);
    if (key == recording_subscription_keys_.end())
    {
        return std::nullopt;
    }

    auto subscription = recording_subscriptions_.find(key->second);
    if (subscription == recording_subscriptions_.end())
    {
        recording_subscription_keys_.erase(key);
        return std::nullopt;
    }

    RecordingSubscription removed = std::move(subscription->second);
    recording_subscriptions_.erase(subscription);
    recording_subscription_keys_.erase(key);
    recording_subscription_order_.erase(
        std::remove(recording_subscription_order_.begin(), recording_subscription_order_.end(), removed.key),
        recording_subscription_order_.end());
    return removed;
}

std::optional<ArchiveConductor::RecordingSubscription> ArchiveConductor::remove_recording_subscription_by_key(
    std::string_view key)
{
    auto subscription = recording_subscriptions_.find(std::string{key});
    if (subscription == recording_subscriptions_.end())
    {
        return std::nullopt;
    }

    RecordingSubscription removed = std::move(subscription->second);
    recording_subscriptions_.erase(subscription);
    recording_subscription_keys_.erase(removed.registration_id);
    recording_subscription_order_.erase(
        std::remove(recording_subscription_order_.begin(), recording_subscription_order_.end(), removed.key),
        recording_subscription_order_.end());
    return removed;
}

void ArchiveConductor::add_recording_session(std::unique_ptr<RecordingSession> session)
{
    // `DedicatedModeRecorder.addSession` (`DedicatedModeArchiveConductor.java:213-218`).
    if (!is_dedicated())
    {
        recorder_.add_session(std::move(session));
        return;
    }

    RecordingSession* raw = session.release();
    while (!recording_inbox_.push(raw))
    {
        // Only an allocation failure can refuse; the reference retries too.
        ::sched_yield();
    }
}

void ArchiveConductor::add_replay_session(std::unique_ptr<ReplaySession> session)
{
    // `DedicatedModeReplayer.addSession` (`DedicatedModeArchiveConductor.java:330-335`).
    if (!is_dedicated())
    {
        replayer_.add_session(std::move(session));
        return;
    }

    ReplaySession* raw = session.release();
    while (!replay_inbox_.push(raw))
    {
        ::sched_yield();
    }
}

int ArchiveConductor::process_close_queue()
{
    // `DedicatedModeArchiveConductor.processCloseQueue` (`:118-137`): at most
    // ten reaps per pass, each closed on the conductor thread.
    constexpr int COMMAND_LIMIT = 10;

    int work_count = 0;
    for (; work_count < COMMAND_LIMIT; ++work_count)
    {
        if (RecordingSession* recording = recording_close_queue_.pop())
        {
            std::unique_ptr<RecordingSession> owned(recording);
            close_recording_session(*owned);
            continue;
        }
        if (ReplaySession* replay = replay_close_queue_.pop())
        {
            std::unique_ptr<ReplaySession> owned(replay);
            close_replay_session(*owned);
            continue;
        }
        break;
    }

    return work_count;
}

void ArchiveConductor::abort_sessions_for_subscription(std::int64_t registration_id, std::string_view reason)
{
    // `abortRecordingSessionAndCloseSubscription` (`ArchiveConductor.java:1766-1787`):
    // the reference walks its own `recordingSessionByIdMap`, not the worker's
    // array (the worker may be on another thread in DEDICATED mode).
    for (auto& entry : sessions_by_recording_id_)
    {
        RecordingSession& session = *entry.second;
        if (session.subscription_registration_id() == registration_id)
        {
            session.abort(reason);
        }
    }
}

void ArchiveConductor::on_stop_recording(const StopRecordingRequest& request)
{
    // `ArchiveConductor.stopRecording` (`ArchiveConductor.java:585-610`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    auto key = make_key(request.stream_id, request.channel);
    if (!key)
    {
        session->send_error_response(request.correlation_id, std::string{key.error().message.view()});
        return;
    }

    auto subscription = remove_recording_subscription_by_key(*key);
    if (!subscription)
    {
        session->send_error_response(
            request.correlation_id, UNKNOWN_SUBSCRIPTION,
            "no recording found for streamId=" + std::to_string(request.stream_id) + " channel=" + request.channel);
        return;
    }

    abort_sessions_for_subscription(subscription->registration_id, "stop recording");
    if (0 >= --subscription->ref_count)
    {
        subscription->subscription.reset();
    }

    session->send_ok_response(request.correlation_id, 0);
}

void ArchiveConductor::on_stop_recording_subscription(const StopRecordingSubscriptionRequest& request)
{
    // `ArchiveConductor.stopRecordingSubscription` (`ArchiveConductor.java:612-636`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (!stop_recording_subscription(request.subscription_id))
    {
        session->send_error_response(
            request.correlation_id, UNKNOWN_SUBSCRIPTION,
            "no recording subscription found for subscriptionId=" + std::to_string(request.subscription_id));
        return;
    }

    session->send_ok_response(request.correlation_id, 0);
}

bool ArchiveConductor::stop_recording_subscription(std::int64_t registration_id)
{
    // `ArchiveConductor.stopRecordingSubscription` (`ArchiveConductor.java:625-635`).
    auto subscription = remove_recording_subscription(registration_id);
    if (!subscription)
    {
        return false;
    }

    abort_sessions_for_subscription(subscription->registration_id, "stop recording");
    if (0 >= --subscription->ref_count)
    {
        subscription->subscription.reset();
    }
    return true;
}

namespace
{
/// `ArchiveConductor.hasRecording` (`ArchiveConductor.java:1950-1960`): a live
/// recording or a catalog entry; an unknown id answers with
/// `UNKNOWN_RECORDING` and `buildUnknownRecordingErrorMsg`.
bool require_recording(const Catalog& catalog, std::int64_t recording_id, std::int64_t correlation_id,
                       ControlSession& session)
{
    if (catalog.has_recording(recording_id))
    {
        return true;
    }
    session.send_error_response(correlation_id, UNKNOWN_RECORDING,
                                "unknown recording id: " + std::to_string(recording_id));
    return false;
}
}  // namespace

void ArchiveConductor::on_get_recording_position(const RecordingIdRequest& request)
{
    // `ArchiveConductor.getRecordingPosition` (`ArchiveConductor.java:1167-1176`).
    if (nullptr == request.session ||
        !require_recording(catalog_, request.recording_id, request.correlation_id, *request.session))
    {
        return;
    }
    const auto active = sessions_by_recording_id_.find(request.recording_id);
    const std::int64_t position =
        active == sessions_by_recording_id_.end() ? Catalog::NULL_POSITION : active->second->recorded_position();
    request.session->send_ok_response(request.correlation_id, position);
}

void ArchiveConductor::on_get_stop_position(const RecordingIdRequest& request)
{
    // `ArchiveConductor.getStopPosition` (`ArchiveConductor.java:1178-1184`).
    if (nullptr == request.session ||
        !require_recording(catalog_, request.recording_id, request.correlation_id, *request.session))
    {
        return;
    }
    request.session->send_ok_response(request.correlation_id, catalog_.stop_position(request.recording_id));
}

void ArchiveConductor::on_get_start_position(const RecordingIdRequest& request)
{
    // `ArchiveConductor.getStartPosition` (`ArchiveConductor.java:1159-1165`).
    if (nullptr == request.session ||
        !require_recording(catalog_, request.recording_id, request.correlation_id, *request.session))
    {
        return;
    }
    request.session->send_ok_response(request.correlation_id, catalog_.start_position(request.recording_id));
}

void ArchiveConductor::on_get_max_recorded_position(const RecordingIdRequest& request)
{
    // `ArchiveConductor.getMaxRecordedPosition` (`ArchiveConductor.java:1186-1195`).
    if (nullptr == request.session ||
        !require_recording(catalog_, request.recording_id, request.correlation_id, *request.session))
    {
        return;
    }
    const auto active = sessions_by_recording_id_.find(request.recording_id);
    const std::int64_t position = active != sessions_by_recording_id_.end()
                                      ? active->second->recorded_position()
                                      : catalog_.stop_position(request.recording_id);
    request.session->send_ok_response(request.correlation_id, position);
}

void ArchiveConductor::on_stop_recording_by_identity(const StopRecordingByIdentityRequest& request)
{
    // `ArchiveConductor.stopRecordingByIdentity` (`ArchiveConductor.java:1301-1327`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (!catalog_.has_recording(request.recording_id))
    {
        session->send_error_response(request.correlation_id, UNKNOWN_RECORDING,
                                     "unknown recording id: " + std::to_string(request.recording_id));
        return;
    }

    std::int64_t found = 0;
    const auto active = sessions_by_recording_id_.find(request.recording_id);
    if (active != sessions_by_recording_id_.end())
    {
        RecordingSession* recording_session = active->second;
        recording_session->abort("stop recording by identity");

        const auto registration_id = recording_session->subscription_registration_id();
        auto subscription = remove_recording_subscription(registration_id);
        if (subscription)
        {
            found = 1;
            if (0 >= --subscription->ref_count)
            {
                subscription->subscription.reset();
            }
        }
    }

    session->send_ok_response(request.correlation_id, found);
}

void ArchiveConductor::close_recording_session(RecordingSession& session)
{
    // `ArchiveConductor.closeRecordingSession` (`ArchiveConductor.java:1329-1363`).
    sessions_by_image_.erase(session.image_handle());
    sessions_by_recording_id_.erase(session.recording_id());
    // `closeSession` closes the type-100 counter (`RecordingSession.java:118-122`),
    // which is what a live replay's `notExtended` observes.
    mark_counter_closed(session.position_counter_id());

    const std::int64_t position = session.recorded_position();

    (void)catalog_.recording_stopped(session.recording_id(), position, epoch_clock_.time_ms());

    if (auto* control = control_session_by_id(session.control_session_id()))
    {
        if (!session.error_message().empty())
        {
            control->send_error_response(session.correlation_id(), session.error_message());
        }
        control->send_signal(session.correlation_id(), session.recording_id(), session.subscription_registration_id(),
                             position, archive_codecs::RecordingSignal::STOP);
    }

    const auto found = recording_subscription_keys_.find(session.subscription_registration_id());
    if (found != recording_subscription_keys_.end())
    {
        auto subscription = recording_subscriptions_.find(found->second);
        if (subscription != recording_subscriptions_.end())
        {
            subscription->second.ref_count--;
            if (subscription->second.ref_count <= 0 || session.auto_stop())
            {
                recording_subscription_keys_.erase(found);
                recording_subscriptions_.erase(subscription);
            }
        }
    }

    decrement_recording_sessions();
}

void ArchiveConductor::on_start_replay(const ReplayRequest& request)
{
    start_replay_request(request, NULL_COUNTER_ID, false);
}

void ArchiveConductor::on_start_bounded_replay(const BoundedReplayRequest& request)
{
    ReplayRequest replay;
    replay.correlation_id = request.correlation_id;
    replay.recording_id = request.recording_id;
    replay.position = request.position;
    replay.length = request.length;
    replay.file_io_max_length = request.file_io_max_length;
    replay.replay_stream_id = request.replay_stream_id;
    replay.replay_token = request.replay_token;
    replay.replay_channel = request.replay_channel;
    replay.session = request.session;
    start_replay_request(replay, request.limit_counter_id, true);
}

void ArchiveConductor::start_replay_request(const ReplayRequest& request, std::int32_t limit_counter_id, bool bounded)
{
    // `ArchiveConductor.startReplay` (`ArchiveConductor.java:763-929`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (num_active_replays_ == context_.replay.max_concurrent_replays)
    {
        session->send_error_response(
            request.correlation_id, MAX_REPLAYS,
            "max concurrent replays reached " + std::to_string(context_.replay.max_concurrent_replays));
        return;
    }

    if (!catalog_.has_recording(request.recording_id))
    {
        session->send_error_response(request.correlation_id, UNKNOWN_RECORDING,
                                     "unknown recording id: " + std::to_string(request.recording_id));
        return;
    }

    // `:788-800`: the client's bounded counter wins; otherwise a live recording
    // lends its own position counter.
    std::int32_t resolved_limit_counter_id = NULL_COUNTER_ID;
    bool limit_client_supplied = false;
    std::int64_t limit_position = NULL_POSITION;
    if (bounded)
    {
        // `startBoundedReplay` (`:996-1035`): `new Counter(reader, id)` refuses
        // an id outside the directory.
        const std::int32_t max_counter_id = aeron_counters_reader_max_counter_id(client_.counters_reader());
        if (limit_counter_id < 0 || limit_counter_id > max_counter_id)
        {
            session->send_error_response(request.correlation_id, 0,
                                         "unable to create replay limit counter id= " +
                                             std::to_string(limit_counter_id) + " because of: " + aeron_errmsg());
            return;
        }

        resolved_limit_counter_id = limit_counter_id;
        limit_client_supplied = true;
        (void)counter_state(limit_counter_id, true);
        limit_position = axc::core::PositionView::at(client_.counters_reader(), limit_counter_id).get_acquire();
    }
    else
    {
        const auto live = sessions_by_recording_id_.find(request.recording_id);
        if (live != sessions_by_recording_id_.end())
        {
            const std::int32_t position_counter_id = live->second->position_counter_id();
            if (position_counter_id >= 0)
            {
                resolved_limit_counter_id = position_counter_id;
                (void)counter_state(position_counter_id, false);
                limit_position =
                    axc::core::PositionView::at(client_.counters_reader(), position_counter_id).get_acquire();
            }
        }
    }

    const RecordingSummary summary = catalog_.recording_summary(request.recording_id);
    const std::int64_t start_position = summary.start_position;
    std::int64_t replay_position = start_position;

    if (NULL_POSITION != request.position)
    {
        // `isInvalidReplayPosition` (`:2291-2329`).
        if (0 != (request.position & (FRAME_ALIGNMENT - 1)))
        {
            session->send_error_response(request.correlation_id,
                                         "requested replay start position=" + std::to_string(request.position) +
                                             " is not a multiple of FRAME_ALIGNMENT (" +
                                             std::to_string(FRAME_ALIGNMENT) + ") for recording " +
                                             std::to_string(request.recording_id));
            return;
        }

        if (request.position - start_position < 0)
        {
            session->send_error_response(
                request.correlation_id, "requested replay start position=" + std::to_string(request.position) +
                                            " is less than recording start position=" + std::to_string(start_position) +
                                            " for recording " + std::to_string(request.recording_id));
            return;
        }

        if (NULL_POSITION != summary.stop_position && request.position >= summary.stop_position)
        {
            session->send_error_response(
                request.correlation_id, INVALID_POSITION,
                "requested replay start position=" + std::to_string(request.position) +
                    " must be less than the limit position=" + std::to_string(summary.stop_position) +
                    " for recording " + std::to_string(request.recording_id));
            return;
        }

        replay_position = request.position;
    }

    const std::int32_t file_io_max_length = request.file_io_max_length;
    if (file_io_max_length > 0 && file_io_max_length < summary.mtu_length)
    {
        session->send_error_response(request.correlation_id, "fileIoMaxLength=" + std::to_string(file_io_max_length) +
                                                                 " < mtuLength=" + std::to_string(summary.mtu_length));
        return;
    }
    if (context_.recording.file_io_max_length < summary.mtu_length)
    {
        session->send_error_response(request.correlation_id,
                                     "replayBufferCapacity=" + std::to_string(context_.recording.file_io_max_length) +
                                         " < mtuLength=" + std::to_string(summary.mtu_length));
        return;
    }

    std::int64_t stop_position = 0;
    std::int64_t max_length = 0;
    if (NULL_POSITION != limit_position)
    {
        if (replay_position > limit_position)
        {
            session->send_error_response(request.correlation_id, INVALID_POSITION,
                                         "requested replay start position=" + std::to_string(replay_position) +
                                             " must be less than the limit position=" + std::to_string(limit_position) +
                                             " for recording " + std::to_string(request.recording_id));
            return;
        }
        stop_position = limit_position;
        max_length = std::numeric_limits<std::int64_t>::max() - replay_position;
    }
    else
    {
        stop_position = summary.stop_position;
        max_length = stop_position - replay_position;
    }

    std::int64_t replay_length = 0;
    if (REPLAY_ALL_AND_FOLLOW == request.length)
    {
        replay_length = max_length;
    }
    else if (REPLAY_ALL_AND_STOP == request.length)
    {
        replay_length = stop_position - replay_position;
        if (0 == replay_length)
        {
            session->send_error_response(
                request.correlation_id, EMPTY_RECORDING,
                "when replaying and stopping the replay length must be non-zero, recordingId=" +
                    std::to_string(request.recording_id));
            return;
        }
    }
    else
    {
        replay_length = std::min(request.length, max_length);
    }

    if (replay_length < 0)
    {
        session->send_error_response(request.correlation_id,
                                     "replay length must be positive: replayLength=" + std::to_string(replay_length) +
                                         ", length=" + std::to_string(request.length) +
                                         ", stopPosition=" + std::to_string(stop_position) +
                                         ", replayPosition=" + std::to_string(replay_position) + " for recording " +
                                         std::to_string(request.recording_id));
        return;
    }

    // `deleteSegmentsSessionByIdMap` is A4a's, so that check has no input yet.

    // `:892-901`: the replay publication's channel.
    ReplayChannelOptions channel_options;
    channel_options.position = replay_position;
    channel_options.initial_term_id = summary.initial_term_id;
    channel_options.term_length = summary.term_buffer_length;
    channel_options.mtu_length = summary.mtu_length;
    channel_options.linger_timeout_ns = context_.replay.replay_linger_timeout_ns;

    auto channel = replay_channel(request.replay_channel, channel_options);
    if (!channel)
    {
        const std::string message = "failed to process replayChannel - " + std::string{channel.error().message.view()};
        log_warning(message);
        session->send_error_response(request.correlation_id, message);
        return;
    }

    CreateReplayPublicationSession::Config config;
    config.correlation_id = request.correlation_id;
    config.recording_id = request.recording_id;
    config.replay_position = replay_position;
    config.replay_length = replay_length;
    config.start_position = start_position;
    config.stop_position = stop_position;
    config.segment_file_length = summary.segment_file_length;
    config.term_buffer_length = summary.term_buffer_length;
    config.stream_id = summary.stream_id;
    config.replay_stream_id = request.replay_stream_id;
    config.replay_channel = std::move(*channel);
    config.file_io_max_length = file_io_max_length;
    config.control_session_id = session->session_id();
    config.limit_counter_id = resolved_limit_counter_id;
    config.limit_client_supplied = limit_client_supplied;

    replay_publication_sessions_.add(std::make_unique<CreateReplayPublicationSession>(std::move(config), *this));
    on_replay_start();
}

void ArchiveConductor::on_stop_replay(const StopReplayRequest& request)
{
    // `ArchiveConductor.stopReplay` (`:1037-1054`): OK unconditionally.
    const auto found = replay_session_by_id_.find(request.replay_session_id);
    if (found != replay_session_by_id_.end())
    {
        found->second->abort("stop replay");
    }

    if (nullptr != request.session)
    {
        request.session->send_ok_response(request.correlation_id, 0);
    }
}

void ArchiveConductor::on_stop_all_replays(const StopAllReplaysRequest& request)
{
    // `ArchiveConductor.stopAllReplays` (`:1048-1062`).
    for (auto& [replay_session_id, session] : replay_session_by_id_)
    {
        (void)replay_session_id;
        if (NULL_VALUE == request.recording_id || session->recording_id() == request.recording_id)
        {
            session->abort("stop all replays");
        }
    }

    if (nullptr != request.session)
    {
        request.session->send_ok_response(request.correlation_id, 0);
    }
}

void ArchiveConductor::on_replay_token(const ReplayTokenRequest& request)
{
    // `ControlSessionAdapter.java:1084-1106`.
    if (nullptr == request.session)
    {
        return;
    }

    const std::int64_t replay_token = generate_replay_token(*request.session, request.recording_id);
    request.session->send_ok_response(request.correlation_id, replay_token);
}

std::int64_t ArchiveConductor::generate_replay_token(ControlSession& session, std::int64_t recording_id)
{
    // `ArchiveConductor.generateReplayToken` (`:2674-2687`).
    std::int64_t replay_token = NULL_VALUE;
    do
    {
        replay_token = next_replay_token();
    } while (NULL_VALUE == replay_token || replay_tokens_.find(replay_token) != replay_tokens_.end());

    SessionForReplay session_for_replay;
    session_for_replay.recording_id = recording_id;
    session_for_replay.control_session_id = session.session_id();
    session_for_replay.deadline_ns = axc::core::NanoClock::now() + context_.connect_timeout_ms * 1'000'000;
    replay_tokens_[replay_token] = session_for_replay;
    return replay_token;
}

ControlSession* ArchiveConductor::replay_session_for_token(std::int64_t token, std::int64_t recording_id)
{
    // `ArchiveConductor.getReplaySession` (`:2689-2702`).
    const auto found = replay_tokens_.find(token);
    if (found == replay_tokens_.end() || found->second.recording_id != recording_id ||
        axc::core::NanoClock::now() >= found->second.deadline_ns)
    {
        return nullptr;
    }

    return control_session_by_id(found->second.control_session_id);
}

void ArchiveConductor::remove_replay_tokens_for_session(std::int64_t control_session_id)
{
    // `ArchiveConductor.removeReplayTokensForSession` (`:2704-2716`).
    for (auto it = replay_tokens_.begin(); it != replay_tokens_.end();)
    {
        if (it->second.control_session_id == control_session_id)
        {
            it = replay_tokens_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void ArchiveConductor::check_replay_tokens()
{
    // `ArchiveConductor.checkReplayTokens` (`:2718-2735`).
    const std::int64_t now_ns = axc::core::NanoClock::now();
    for (auto it = replay_tokens_.begin(); it != replay_tokens_.end();)
    {
        if (it->second.deadline_ns <= now_ns)
        {
            it = replay_tokens_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

std::int64_t ArchiveConductor::next_replay_token()
{
    // Any unique non-null value works; the reference uses `java.util.Random`.
    replay_token_state_ ^= replay_token_state_ << 13;
    replay_token_state_ ^= replay_token_state_ >> 7;
    replay_token_state_ ^= replay_token_state_ << 17;
    return static_cast<std::int64_t>(replay_token_state_ & 0x7FFF'FFFF'FFFF'FFFFULL);
}

void ArchiveConductor::async_send_ok(std::int64_t control_session_id, std::int64_t correlation_id,
                                     std::int64_t replay_session_id)
{
    // `ControlSession.asyncSendOkResponse` (`:733-745`); a session that is gone
    // simply drops it (the reference's closed publication does the same).
    if (ControlSession* session = control_session_by_id(control_session_id); nullptr != session)
    {
        session->async_send_ok_response(correlation_id, replay_session_id);
    }
}

void ArchiveConductor::send_error(std::int64_t control_session_id, std::int64_t correlation_id,
                                  std::int64_t relevant_id, std::string_view message)
{
    if (ControlSession* session = control_session_by_id(control_session_id); nullptr != session)
    {
        session->send_error_response(correlation_id, relevant_id, message);
    }
}

void ArchiveConductor::unexpected_frame(std::int32_t frame_type, std::int64_t position)
{
    log_warning("unexpected frame type " + std::to_string(frame_type) + " at position=" + std::to_string(position));
}

axc::core::AsyncAdd<axc::core::ExPubPtr> ArchiveConductor::add_replay_publication(std::string_view channel,
                                                                                  std::int32_t stream_id)
{
    return client_.add_exclusive_publication(channel, stream_id);
}

void ArchiveConductor::replay_publication_failed(std::int64_t control_session_id, std::int64_t correlation_id,
                                                 std::string_view message)
{
    // `CreateReplayPublicationSession.java:152-156`.
    on_replay_end();
    log_warning(message);
    if (ControlSession* session = control_session_by_id(control_session_id); nullptr != session)
    {
        session->send_error_response(correlation_id, message);
    }
}

void ArchiveConductor::new_replay_session(const CreateReplayPublicationSession::Config& config,
                                          std::unique_ptr<ReplayPublication> publication)
{
    // `ArchiveConductor.newReplaySession` (`:941-994`).
    const std::int64_t replay_session_id = (static_cast<std::int64_t>(next_replay_id_++) << 32) |
                                           (static_cast<std::int64_t>(publication->session_id()) & 0xFFFF'FFFFLL);

    // `Archive.Context.replayBuffer()` (`Archive.java:3894-3906`) is the read
    // window's capacity; the buffer itself lives in the session's
    // `SegmentReader` (the pread copy buffer or the mmap view).
    std::size_t buffer_length = static_cast<std::size_t>(context_.recording.file_io_max_length);
    if (config.file_io_max_length > 0 && static_cast<std::size_t>(config.file_io_max_length) < buffer_length)
    {
        buffer_length = static_cast<std::size_t>(config.file_io_max_length);
    }

    std::unique_ptr<ReplayLimitSource> limit;
    if (NULL_COUNTER_ID != config.limit_counter_id)
    {
        CounterState& state = counter_state(config.limit_counter_id, config.limit_client_supplied);
        limit = std::make_unique<CounterLimitSource>(client_.counters_reader(), config.limit_counter_id, state.closed,
                                                     state.registration_id);
    }

    ReplaySession::Config replay_config;
    replay_config.correlation_id = config.correlation_id;
    replay_config.recording_id = config.recording_id;
    replay_config.replay_position = config.replay_position;
    replay_config.replay_length = config.replay_length;
    replay_config.start_position = config.start_position;
    replay_config.stop_position = config.stop_position;
    replay_config.segment_file_length = config.segment_file_length;
    replay_config.term_buffer_length = config.term_buffer_length;
    replay_config.stream_id = config.stream_id;
    replay_config.replay_session_id = replay_session_id;
    replay_config.control_session_id = config.control_session_id;
    replay_config.connect_timeout_ms = context_.connect_timeout_ms;
    replay_config.archive_dir = context_.archive_dir;

    auto session = std::make_unique<ReplaySession>(std::move(replay_config), std::move(publication), *this, replayer_,
                                                   replay_clock_, context_.replay_checksum, std::move(limit),
                                                   static_cast<std::int32_t>(buffer_length), this);

    replay_session_by_id_[replay_session_id] = session.get();
    add_replay_session(std::move(session));

    // `:993`: the type-112 replay session counter.
    if (replay_session_counter_)
    {
        replay_session_counter_->set_value(replay_session_counter_->value() + 1);
    }
}

void ArchiveConductor::close_replay_session(ReplaySession& session)
{
    // `ArchiveConductor.closeReplaySession` (`:1368-1390`).
    session.send_pending_error();
    replay_session_by_id_.erase(session.session_id());
    on_replay_end();
    session.close();

    if (replay_session_counter_)
    {
        replay_session_counter_->set_value(replay_session_counter_->value() - 1);
    }
}

void ArchiveConductor::on_replay_start() noexcept
{
    // `ArchiveConductor.onReplayStart` (`:931-934`).
    num_active_replays_++;
}

void ArchiveConductor::on_replay_end() noexcept
{
    // `ArchiveConductor.onReplayEnd` (`:936-939`).
    num_active_replays_--;
}

ArchiveConductor::CounterState& ArchiveConductor::counter_state(std::int32_t counter_id, bool client_supplied)
{
    auto found = counter_by_id_.find(counter_id);
    if (found == counter_by_id_.end())
    {
        CounterState state;
        state.client_supplied = client_supplied;

        std::int64_t registration_id = 0;
        if (0 == aeron_counters_reader_counter_registration_id(client_.counters_reader(), counter_id, &registration_id))
        {
            state.registration_id = registration_id;
        }

        found = counter_by_id_.emplace(counter_id, state).first;
    }
    return found->second;
}

void ArchiveConductor::reset_counter_state(std::int32_t counter_id, std::int64_t registration_id)
{
    if (counter_id < 0)
    {
        return;
    }

    CounterState& state = counter_state(counter_id, false);
    state.registration_id = registration_id;
    state.closed = false;
    state.client_supplied = false;
}

void ArchiveConductor::mark_counter_closed(std::int32_t counter_id)
{
    const auto found = counter_by_id_.find(counter_id);
    if (found != counter_by_id_.end())
    {
        found->second.closed = true;
    }
}

void ArchiveConductor::on_truncate_recording(const TruncateRecordingRequest& request)
{
    // `ControlSession.onTruncateRecording` (`ControlSession.java:505-512`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.truncateRecording` (`:1199-1246`).
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    if (const auto invalid = validate_truncate(summary, request.position))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }
    if (!is_delete_allowed(*session, request.correlation_id, recording_id))
    {
        return;
    }

    const std::int64_t stop_position = summary.stop_position;
    const std::int64_t start_position = summary.start_position;
    const std::int32_t segment_length = summary.segment_file_length;
    const std::int32_t term_length = summary.term_buffer_length;
    const std::int64_t segment_base_position =
        segment_file_base_position(start_position, request.position, term_length, segment_length);
    const auto segment_offset = static_cast<std::int32_t>(request.position - segment_base_position);

    if (auto stopped = catalog_.stop_position(recording_id, request.position); !stopped)
    {
        session->send_error_response(request.correlation_id, std::string{stopped.error().message.view()});
        return;
    }

    std::vector<std::string> files;
    if (start_position == request.position)
    {
        files = list_segment_files_with_delete_suffix(context_.archive_dir, recording_id);
    }
    else
    {
        if (segment_offset > 0)
        {
            if (stop_position != request.position)
            {
                const std::string file_name = segment_file_name(recording_id, segment_base_position);
                const auto term_count =
                    static_cast<std::int32_t>(request.position >> position_bits_to_shift(term_length));
                const std::int32_t term_id = summary.initial_term_id + term_count;
                auto erased = erase_remaining_segment(context_.archive_dir, file_name, request.position, segment_length,
                                                      segment_offset, term_length, term_id, summary.stream_id);
                if (!erased)
                {
                    session->send_error_response(request.correlation_id, erased.error());
                    return;
                }
            }
        }
        else
        {
            files.push_back(segment_file_name(recording_id, segment_base_position));
        }

        for (std::int64_t position = segment_base_position + segment_length; position <= stop_position;
             position += segment_length)
        {
            files.push_back(segment_file_name(recording_id, position));
        }
    }

    stop_all_replays(recording_id);
    delete_segments(*session, request.correlation_id, recording_id, std::move(files), true);
}

void ArchiveConductor::on_purge_recording(const PurgeRecordingRequest& request)
{
    // `ControlSession.onPurgeRecording` (`ControlSession.java:514-522`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.purgeRecording` (`:1251-1263`).
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    if (const auto invalid = validate_purge(summary, has_in_progress_replays(recording_id)))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }
    if (!is_delete_allowed(*session, request.correlation_id, recording_id))
    {
        return;
    }

    if (auto changed = catalog_.change_state(recording_id, archive_codecs::RecordingState::DELETED); !changed)
    {
        session->send_error_response(request.correlation_id, std::string{changed.error().message.view()});
        return;
    }

    std::vector<std::string> files = list_segment_files_with_delete_suffix(context_.archive_dir, recording_id);
    delete_segments(*session, request.correlation_id, recording_id, std::move(files), false);
}

void ArchiveConductor::on_detach_segments(const SegmentStartPositionRequest& request)
{
    // `ControlSession.onDetachSegments` (`ControlSession.java:637-644`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.detachSegments` (`:1502-1514`): the catalog is the only
    // thing that moves, no file is touched.
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    if (const auto invalid = validate_detach(summary, request.new_start_position, recorded_position_for(recording_id),
                                             replays_for(recording_id)))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }

    if (auto started = catalog_.start_position(recording_id, request.new_start_position); !started)
    {
        session->send_error_response(request.correlation_id, std::string{started.error().message.view()});
        return;
    }

    session->send_ok_response(request.correlation_id, 0);
}

void ArchiveConductor::on_delete_detached_segments(const RecordingIdRequest& request)
{
    // `ControlSession.onDeleteDetachedSegments` (`ControlSession.java:646-653`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.deleteDetachedSegments` (`:1516-1540`).
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }
    if (!is_delete_allowed(*session, request.correlation_id, recording_id))
    {
        return;
    }

    std::int64_t min_position = std::numeric_limits<std::int64_t>::max();
    for (const std::string& name : list_segment_files_with_delete_suffix(context_.archive_dir, recording_id))
    {
        std::int64_t position = 0;
        if (parse_segment_file_position_for_recording(name, recording_id, position))
        {
            min_position = std::min(min_position, position);
        }
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    std::vector<std::string> files;
    if (std::numeric_limits<std::int64_t>::max() != min_position)
    {
        files = find_detached_segments(context_.archive_dir, recording_id, summary.start_position, min_position,
                                       summary.term_buffer_length, summary.segment_file_length);
    }

    delete_segments(*session, request.correlation_id, recording_id, std::move(files), false);
}

void ArchiveConductor::on_purge_segments(const SegmentStartPositionRequest& request)
{
    // `ControlSession.onPurgeSegments` (`ControlSession.java:655-662`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.purgeSegments` (`:1542-1561`).
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    if (const auto invalid = validate_detach(summary, request.new_start_position, recorded_position_for(recording_id),
                                             replays_for(recording_id)))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }
    if (!is_delete_allowed(*session, request.correlation_id, recording_id))
    {
        return;
    }

    const std::int64_t old_start_position = summary.start_position;
    if (auto started = catalog_.start_position(recording_id, request.new_start_position); !started)
    {
        session->send_error_response(request.correlation_id, std::string{started.error().message.view()});
        return;
    }

    std::vector<std::string> files =
        find_detached_segments(context_.archive_dir, recording_id, request.new_start_position, old_start_position,
                               summary.term_buffer_length, summary.segment_file_length);
    delete_segments(*session, request.correlation_id, recording_id, std::move(files), false);
}

void ArchiveConductor::on_attach_segments(const RecordingIdRequest& request)
{
    // `ControlSession.onAttachSegments` (`ControlSession.java:664-671`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.attachSegments` (`:1563-1624`).
    const std::int64_t recording_id = request.recording_id;
    if (!require_recording(catalog_, recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary summary = catalog_.recording_summary(recording_id);
    const std::int32_t segment_length = summary.segment_file_length;
    const std::int32_t term_length = summary.term_buffer_length;
    const std::int32_t bits_to_shift = position_bits_to_shift(term_length);
    const std::int32_t stream_id = summary.stream_id;

    std::int64_t position = summary.start_position - segment_length;
    std::int64_t count = 0;

    while (position >= 0)
    {
        const std::string file_name = segment_file_name(recording_id, position);
        const std::filesystem::path path = std::filesystem::path(context_.archive_dir) / file_name;
        std::error_code error;
        if (!std::filesystem::exists(path, error))
        {
            break;
        }

        const auto file_length = std::filesystem::file_size(path, error);
        if (error)
        {
            session->send_error_response(request.correlation_id, error.message());
            return;
        }
        if (static_cast<std::int64_t>(file_length) != segment_length)
        {
            session->send_error_response(request.correlation_id,
                                         "fileLength=" + std::to_string(file_length) +
                                             " not equal to segmentLength=" + std::to_string(segment_length));
            return;
        }

        const auto term_count = static_cast<std::int32_t>(position >> bits_to_shift);
        const std::int32_t term_id = summary.initial_term_id + term_count;
        auto term_offset = find_term_offset_for_start(context_.archive_dir, file_name, stream_id, term_id, term_length);
        if (!term_offset)
        {
            session->send_error_response(request.correlation_id, term_offset.error());
            return;
        }

        if (0 == *term_offset)
        {
            if (auto started = catalog_.start_position(recording_id, position); !started)
            {
                session->send_error_response(request.correlation_id, std::string{started.error().message.view()});
                return;
            }
            count += 1;
            position -= segment_length;
        }
        else
        {
            if (auto started = catalog_.start_position(recording_id, position + *term_offset); !started)
            {
                session->send_error_response(request.correlation_id, std::string{started.error().message.view()});
                return;
            }
            count += 1;
            break;
        }
    }

    session->send_ok_response(request.correlation_id, count);
}

void ArchiveConductor::on_replicate(const ReplicateRequest& request)
{
    // `ControlSession.onReplicate` (`ControlSession.java:490-500`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.replicate` (`:1385-1478`).
    const std::string replication_channel =
        request.replication_channel.empty() ? context_.replication_channel : request.replication_channel;

    if (const auto refusal = validate_replication_channel_configured(replication_channel))
    {
        session->send_error_response(request.correlation_id, refusal->code, refusal->message);
        return;
    }

    const bool has_tags = NULL_VALUE != request.channel_tag_id || NULL_VALUE != request.subscription_tag_id;
    if (const auto refusal = validate_replication_channel(is_mds_response_channel(replication_channel),
                                                          !request.live_destination.empty(), has_tags))
    {
        session->send_error_response(request.correlation_id, refusal->code, refusal->message);
        return;
    }

    const bool has_recording = catalog_.has_recording(request.dst_recording_id);
    bool is_active_recording = false;
    RecordingSummary dst_summary{};
    if (has_recording)
    {
        dst_summary = catalog_.recording_summary(request.dst_recording_id);
        is_active_recording =
            NULL_POSITION == dst_summary.stop_position ||
            sessions_by_recording_id_.find(request.dst_recording_id) != sessions_by_recording_id_.end();
    }

    if (const auto refusal =
            validate_replication_destination(request.dst_recording_id, has_recording, is_active_recording))
    {
        session->send_error_response(request.correlation_id, refusal->code, refusal->message);
        return;
    }

    ReplicationConfig config;
    config.control_session_id = session->session_id();
    config.replication_id = next_session_id_++;
    config.src_recording_id = request.src_recording_id;
    config.dst_recording_id = request.dst_recording_id;
    config.stop_position = request.stop_position;
    config.channel_tag_id = request.channel_tag_id;
    config.subscription_tag_id = request.subscription_tag_id;
    config.src_control_stream_id = request.src_control_stream_id;
    config.file_io_max_length = request.file_io_max_length;
    config.replication_session_id = request.replication_session_id;
    config.src_control_channel = request.src_control_channel;
    config.live_destination = request.live_destination;
    config.replication_channel = replication_channel;
    config.src_response_channel = request.src_response_channel;
    config.encoded_credentials = request.encoded_credentials;
    config.has_dst_recording = has_recording;
    config.dst_start_position = dst_summary.start_position;
    config.dst_stop_position = dst_summary.stop_position;
    config.dst_stream_id = dst_summary.stream_id;
    config.action_timeout_ms = context_.message_timeout_ms;
    config.retry_attempts = context_.message_retry_attempts;
    config.clock = &cached_clock_;

    // `ArchiveConductor.java:1433-1457`: clone the archive's client context —
    // request channel/stream from the replication request, the response
    // channel from the request or from our own control channel
    // (`Archive.java:1587-1610`), the client name suffixed with the
    // replication id.
    std::string response_channel = request.src_response_channel;
    if (response_channel.empty())
    {
        response_channel = context_.control_response_channel;
    }
    if (response_channel.empty())
    {
        auto derived = replication_response_channel(context_.control_channel);
        if (!derived)
        {
            session->send_error_response(request.correlation_id, GENERIC, derived.error().message.view());
            return;
        }
        response_channel = *derived;
    }
    config.src_response_channel = response_channel;

    SourceArchiveClient::Config client_config;
    client_config.control_request_channel = request.src_control_channel;
    client_config.control_request_stream_id = request.src_control_stream_id;
    client_config.control_response_channel = response_channel;
    client_config.control_response_stream_id = context_.control_response_stream_id;
    client_config.client_name = "archive archiveId=" + std::to_string(context_.archive_id) +
                                " replicationSessionId=" + std::to_string(config.replication_id);
    client_config.encoded_credentials = request.encoded_credentials;

    auto source = SourceArchiveClient::create(client_.handle(), client_config);
    if (!source)
    {
        // `ReplicationSession.connect` reports this under the replication id in
        // the reference; we have no session yet, so the request's correlation
        // carries the same code and message.
        session->send_error_response(request.correlation_id, REPLICATION_CONNECTION_FAILURE,
                                     "Replication connection failed=" + std::string{source.error().message.view()});
        return;
    }

    auto replication = std::make_unique<ReplicationSession>(config, std::move(*source), *this);
    replication_session_by_id_[config.replication_id] = replication.get();
    replication_sessions_.add(std::move(replication));

    // `:1474-1477`: the OK carries the replication id, not the outcome.
    session->send_ok_response(request.correlation_id, config.replication_id);
}

void ArchiveConductor::send_replication_signal(std::int64_t control_session_id, std::int64_t replication_id,
                                               std::int64_t recording_id, std::int64_t subscription_id,
                                               std::int64_t position, archive_codecs::RecordingSignal::Value signal)
{
    // `ControlSession.sendSignal` (`ControlSession.java:779-800`).
    if (auto* control = control_session_by_id(control_session_id))
    {
        control->send_signal(replication_id, recording_id, subscription_id, position, signal);
    }
}

void ArchiveConductor::send_replication_error(std::int64_t control_session_id, std::int64_t replication_id,
                                              std::int64_t error_code, std::string_view message)
{
    // `ControlSession.sendErrorResponse(replicationId, errorCode, msg)`.
    if (auto* control = control_session_by_id(control_session_id))
    {
        control->send_error_response(replication_id, error_code, message);
    }
}

std::int64_t ArchiveConductor::catalog_stop_position(std::int64_t recording_id)
{
    return catalog_.stop_position(recording_id);
}

std::expected<std::int64_t, std::string> ArchiveConductor::add_new_recording(
    std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp, std::int64_t stop_timestamp,
    std::int32_t image_initial_term_id, std::int32_t segment_file_length, std::int32_t term_buffer_length,
    std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
    std::string_view original_channel, std::string_view source_identity)
{
    auto added = catalog_.add_new_recording(start_position, stop_position, start_timestamp, stop_timestamp,
                                            image_initial_term_id, segment_file_length, term_buffer_length, mtu_length,
                                            session_id, stream_id, stripped_channel, original_channel, source_identity);
    if (!added)
    {
        return std::unexpected(std::string{added.error().message.view()});
    }
    return *added;
}

std::expected<void, std::string> ArchiveConductor::replace_recording(
    std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp,
    std::int64_t stop_timestamp, std::int32_t image_initial_term_id, std::int32_t segment_file_length,
    std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id,
    std::string_view stripped_channel, std::string_view original_channel, std::string_view source_identity)
{
    auto replaced =
        catalog_.replace_recording(recording_id, start_position, stop_position, start_timestamp, stop_timestamp,
                                   image_initial_term_id, segment_file_length, term_buffer_length, mtu_length,
                                   session_id, stream_id, stripped_channel, original_channel, source_identity);
    if (!replaced)
    {
        return std::unexpected(std::string{replaced.error().message.view()});
    }
    return {};
}

std::expected<std::int64_t, std::string> ArchiveConductor::start_extend_recording(
    std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t recording_id, std::int32_t stream_id,
    std::int32_t source_location, bool auto_stop, std::string_view original_channel)
{
    ControlSession* session = control_session_by_id(control_session_id);
    if (nullptr == session)
    {
        // The requesting control session is gone; there is nobody to answer.
        return std::unexpected("no control session " + std::to_string(control_session_id));
    }

    return extend_recording(correlation_id, recording_id, stream_id, source_location, auto_stop, original_channel,
                            *session);
}

aeron_image_t* ArchiveConductor::image_by_session_id(std::int64_t registration_id, std::int32_t session_id)
{
    const auto key = recording_subscription_keys_.find(registration_id);
    if (key == recording_subscription_keys_.end())
    {
        return nullptr;
    }

    const auto subscription = recording_subscriptions_.find(key->second);
    if (subscription == recording_subscriptions_.end() || !subscription->second.subscription)
    {
        return nullptr;
    }

    return aeron_subscription_image_by_session_id(subscription->second.subscription.get(), session_id);
}

std::int64_t ArchiveConductor::image_position(aeron_image_t* image)
{
    return aeron_image_position(image);
}

bool ArchiveConductor::image_is_closed(aeron_image_t* image)
{
    return aeron_image_is_closed(image);
}

bool ArchiveConductor::image_is_end_of_stream(aeron_image_t* image)
{
    return aeron_image_is_end_of_stream(image);
}

std::optional<std::string> ArchiveConductor::subscription_resolved_endpoint(std::int64_t registration_id)
{
    const auto key = recording_subscription_keys_.find(registration_id);
    if (key == recording_subscription_keys_.end())
    {
        return std::nullopt;
    }

    const auto subscription = recording_subscriptions_.find(key->second);
    if (subscription == recording_subscriptions_.end() || !subscription->second.subscription)
    {
        return std::nullopt;
    }

    // `Subscription.resolvedEndpoint()` (`Subscription.java:579-587`): null
    // until the channel status counter carries an address.
    char endpoint[AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = {};
    if (aeron_subscription_resolved_endpoint(subscription->second.subscription.get(), endpoint, sizeof(endpoint)) <= 0)
    {
        return std::nullopt;
    }
    return std::string{endpoint};
}

void ArchiveConductor::on_stop_replication(const StopReplicationRequest& request)
{
    // `ControlSession.onStopReplication` (`ControlSession.java:502-510`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.stopReplication` (`:1480-1493`).
    const auto found = replication_session_by_id_.find(request.replication_id);
    if (found == replication_session_by_id_.end())
    {
        session->send_error_response(request.correlation_id, UNKNOWN_REPLICATION,
                                     "unknown replication id " + std::to_string(request.replication_id));
        return;
    }

    found->second->abort("stop replication");
    replication_session_by_id_.erase(found);
    session->send_ok_response(request.correlation_id, 0);
}

void ArchiveConductor::on_list_recordings(const ListRecordingsRequest& request)
{
    // `ControlSession.onListRecordings` (`ControlSession.java:374-381`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.newListRecordingsSession` (`:637-656`).
    if (const auto error = listing_start_error(session->has_active_listing()))
    {
        session->send_error_response(request.correlation_id, ACTIVE_LISTING, *error);
        return;
    }

    ListRecordingsConfig config;
    config.control_session_id = session->session_id();
    config.correlation_id = request.correlation_id;
    config.from_recording_id = request.from_recording_id;
    config.count = request.record_count;
    list_recordings_sessions_.add(std::make_unique<ListRecordingsSession>(catalog_, *this, config));
    session->set_active_listing(true);
}

void ArchiveConductor::on_list_recordings_for_uri(const ListRecordingsForUriRequest& request)
{
    // `ControlSession.onListRecordingsForUri` (`ControlSession.java:354-372`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.newListRecordingsForUriSession` (`:658-685`).
    if (const auto error = listing_start_error(session->has_active_listing()))
    {
        session->send_error_response(request.correlation_id, ACTIVE_LISTING, *error);
        return;
    }

    ListRecordingsConfig config;
    config.control_session_id = session->session_id();
    config.correlation_id = request.correlation_id;
    config.from_recording_id = request.from_recording_id;
    config.count = request.record_count;
    config.stream_id = request.stream_id;
    config.channel_fragment = request.channel_fragment;
    config.filter_by_uri = true;
    list_recordings_sessions_.add(std::make_unique<ListRecordingsSession>(catalog_, *this, config));
    session->set_active_listing(true);
}

bool ArchiveConductor::send_descriptor(std::int64_t control_session_id, std::int64_t correlation_id,
                                       std::span<const std::uint8_t> frame)
{
    ControlSession* session = control_session_by_id(control_session_id);
    return nullptr != session && session->send_descriptor(correlation_id, frame);
}

void ArchiveConductor::send_recording_unknown(std::int64_t control_session_id, std::int64_t correlation_id,
                                              std::int64_t recording_id)
{
    if (ControlSession* session = control_session_by_id(control_session_id); nullptr != session)
    {
        session->send_recording_unknown(correlation_id, recording_id);
    }
}

bool ArchiveConductor::control_session_is_done(std::int64_t control_session_id) const
{
    const auto found = sessions_by_id_.find(control_session_id);
    return found == sessions_by_id_.end() || found->second->is_done();
}

void ArchiveConductor::clear_active_listing(std::int64_t control_session_id)
{
    if (ControlSession* session = control_session_by_id(control_session_id); nullptr != session)
    {
        session->set_active_listing(false);
    }
}

void ArchiveConductor::on_migrate_segments(const MigrateSegmentsRequest& request)
{
    // `ControlSession.onMigrateSegments` (`ControlSession.java:673-680`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }
    session->attempt_to_activate();
    if (ControlSession::State::active != session->state())
    {
        return;
    }

    // `ArchiveConductor.migrateSegments` (`:1626-1701`).
    if (!require_recording(catalog_, request.src_recording_id, request.correlation_id, *session) ||
        !require_recording(catalog_, request.dst_recording_id, request.correlation_id, *session))
    {
        return;
    }

    const RecordingSummary src = catalog_.recording_summary(request.src_recording_id);
    const RecordingSummary dst = catalog_.recording_summary(request.dst_recording_id);

    if (const auto invalid = validate_migrate_source(src))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }
    if (const auto invalid = validate_migrate_stream_parameters(src, dst))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }

    std::int64_t join_position = 0;
    if (src.stop_position == dst.start_position)
    {
        join_position = src.stop_position;
    }
    else if (src.start_position == dst.stop_position)
    {
        join_position = src.start_position;
    }
    else
    {
        session->send_error_response(request.correlation_id,
                                     "invalid migrate: src and dst are not contiguous" +
                                         std::string{" srcStartPosition="} + std::to_string(src.start_position) +
                                         " srcStopPosition=" + std::to_string(src.stop_position) +
                                         " dstStartPosition=" + std::to_string(dst.start_position) +
                                         " dstStopPosition=" + std::to_string(dst.stop_position));
        return;
    }

    if (const auto invalid = validate_migrate_seam("src", src, join_position))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }
    if (const auto invalid = validate_migrate_seam("dst", dst, join_position))
    {
        session->send_error_response(request.correlation_id, invalid->code, invalid->message);
        return;
    }

    std::vector<std::string> empty_following_src_segments;
    const MoveSegmentsResult moved = move_all_segments(
        context_.archive_dir, request.src_recording_id, request.dst_recording_id, src.start_position, src.stop_position,
        src.term_buffer_length, src.segment_file_length, empty_following_src_segments);
    if (!moved.ok)
    {
        session->send_error_response(request.correlation_id, moved.error);
        return;
    }

    const auto delete_count = static_cast<std::int64_t>(empty_following_src_segments.size());
    if (delete_count > 0)
    {
        auto delete_session = std::make_unique<DeleteSegmentsSession>(
            request.src_recording_id, session->session_id(), request.correlation_id, context_.archive_dir,
            std::move(empty_following_src_segments), *this, *this, false);
        delete_session_by_recording_id_[request.src_recording_id] = delete_session.get();
        delete_sessions_.add(std::move(delete_session));
    }

    if (src.stop_position == dst.start_position)
    {
        if (auto started = catalog_.start_position(request.dst_recording_id, src.stop_position); !started)
        {
            session->send_error_response(request.correlation_id, std::string{started.error().message.view()});
            return;
        }
    }
    else
    {
        if (auto stopped = catalog_.stop_position(request.dst_recording_id, src.stop_position); !stopped)
        {
            session->send_error_response(request.correlation_id, std::string{stopped.error().message.view()});
            return;
        }
    }

    if (auto stopped = catalog_.stop_position(request.src_recording_id, src.start_position); !stopped)
    {
        session->send_error_response(request.correlation_id, std::string{stopped.error().message.view()});
        return;
    }

    session->send_ok_response(request.correlation_id, moved.moved);

    if (moved.moved > 0 && 0 == delete_count)
    {
        session->send_signal(request.correlation_id, request.src_recording_id, NULL_VALUE, NULL_VALUE,
                             archive_codecs::RecordingSignal::DELETE);
    }
}

void ArchiveConductor::on_update_channel(const UpdateChannelRequest& request)
{
    // `ArchiveConductor.updateChannel` (`:707-735`); the adapter calls this
    // directly, without `ControlSession.attemptToActivate`.
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    if (session->has_active_listing())
    {
        session->send_error_response(request.correlation_id, ACTIVE_LISTING, "active listing already in progress");
        return;
    }

    if (!catalog_.has_recording(request.recording_id))
    {
        session->send_recording_unknown(request.correlation_id, request.recording_id);
        return;
    }

    auto stripped = stripped_channel(request.channel);
    if (!stripped)
    {
        // The reference's `ChannelUri.parse` throws and the agent logs it; no
        // response is sent. Same outward behaviour.
        log_warning(std::string{stripped.error().message.view()});
        return;
    }

    Listing listing;
    listing.kind = Listing::Kind::update_channel;
    listing.correlation_id = request.correlation_id;
    listing.control_session_id = session->session_id();
    listing.recording_id = request.recording_id;
    listing.original_channel = request.channel;
    listing.stripped_channel = std::move(*stripped);
    listings_.push_back(std::move(listing));
    session->set_active_listing(true);
}

bool ArchiveConductor::is_delete_allowed(ControlSession& session, std::int64_t correlation_id,
                                         std::int64_t recording_id)
{
    // `ArchiveConductor.isDeleteAllowed` (`:1969-1979`).
    if (delete_session_by_recording_id_.find(recording_id) != delete_session_by_recording_id_.end())
    {
        session.send_error_response(
            correlation_id, "another delete operation in progress for recording id: " + std::to_string(recording_id));
        return false;
    }
    return true;
}

void ArchiveConductor::stop_all_replays(std::int64_t recording_id)
{
    // `ArchiveConductor.stopAllReplays(recordingId)` (`:1048-1062`) without the
    // OK response: the request that triggered it owns that.
    for (auto& entry : replay_session_by_id_)
    {
        if (entry.second->recording_id() == recording_id)
        {
            entry.second->abort("stop all replays");
        }
    }
}

std::int64_t ArchiveConductor::recorded_position_for(std::int64_t recording_id) const
{
    const auto found = sessions_by_recording_id_.find(recording_id);
    return found == sessions_by_recording_id_.end() ? NULL_POSITION : found->second->recorded_position();
}

std::vector<ReplayInProgressInfo> ArchiveConductor::replays_for(std::int64_t recording_id) const
{
    std::vector<ReplayInProgressInfo> replays;
    for (const auto& entry : replay_session_by_id_)
    {
        const ReplaySession& session = *entry.second;
        if (session.recording_id() != recording_id)
        {
            continue;
        }

        ReplayInProgressInfo info;
        info.recording_id = session.recording_id();
        info.segment_file_base_position = session.segment_file_base_position();
        info.session_id = session.session_id();
        info.stream_id = session.replay_stream_id();
        info.channel = std::string{session.replay_channel()};
        info.state = std::string{replay_state_name(session.state())};
        replays.push_back(std::move(info));
    }
    return replays;
}

bool ArchiveConductor::has_in_progress_replays(std::int64_t recording_id)
{
    // `ArchiveConductor.hasInProgressReplays` (`:2226-2239`).
    for (const auto& entry : replay_session_by_id_)
    {
        if (entry.second->recording_id() == recording_id)
        {
            return true;
        }
    }
    return false;
}

void ArchiveConductor::remove_delete_segments_session(std::int64_t recording_id)
{
    // `ArchiveConductor.removeDeleteSegmentsSession` (`:1711-1714`).
    delete_session_by_recording_id_.erase(recording_id);
}

void ArchiveConductor::log_error(std::string_view message)
{
    // The reference's `errorHandler.onError(new ArchiveEvent(...))` writes to the
    // error log; the archive's error log is the warnings sink until C1a.
    log_warning(message);
}

void ArchiveConductor::send_delete_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                                          std::int64_t recording_id)
{
    // `ControlSession.sendSignal(correlationId, recordingId, NULL, NULL, DELETE)`
    // (`DeleteSegmentsSession.java:105-107`). A session that has already gone is
    // simply dropped, as in the replay path.
    const auto found = sessions_by_id_.find(control_session_id);
    if (found != sessions_by_id_.end())
    {
        found->second->send_signal(correlation_id, recording_id, NULL_VALUE, NULL_VALUE,
                                   archive_codecs::RecordingSignal::DELETE);
    }
}

void ArchiveConductor::send_error(std::int64_t control_session_id, std::int64_t correlation_id,
                                  std::string_view message)
{
    const auto found = sessions_by_id_.find(control_session_id);
    if (found != sessions_by_id_.end())
    {
        found->second->send_error_response(correlation_id, message);
    }
}

void ArchiveConductor::delete_segments(ControlSession& session, std::int64_t correlation_id, std::int64_t recording_id,
                                       std::vector<std::string> files, bool await_replays_stop)
{
    // `ArchiveConductor.deleteSegments` (`:2654-2670`).
    const auto count = static_cast<std::int64_t>(files.size());
    if (count > 0)
    {
        auto delete_session = std::make_unique<DeleteSegmentsSession>(
            recording_id, session.session_id(), correlation_id, context_.archive_dir, std::move(files), *this, *this,
            await_replays_stop);
        delete_session_by_recording_id_[recording_id] = delete_session.get();
        delete_sessions_.add(std::move(delete_session));
    }

    session.send_ok_response(correlation_id, count);

    if (0 == count)
    {
        session.send_signal(correlation_id, recording_id, NULL_VALUE, NULL_VALUE,
                            archive_codecs::RecordingSignal::DELETE);
    }
}

void ArchiveConductor::log_warning(std::string_view message)
{
    // `ArchiveConductor.logWarning` (`ArchiveConductor.java:442-445`) feeds the
    // error log; the distinct error log lands with A2b, so the message is kept
    // for the caller (and the tests) in the meantime.
    warnings_.emplace_back(message);
}

}  // namespace deepmsg::archive::server
