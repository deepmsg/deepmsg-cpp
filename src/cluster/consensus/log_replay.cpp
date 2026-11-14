#include "cluster/consensus/log_replay.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>

#include "cluster/service/role.h"

namespace deepmsg::cluster::consensus
{

LogReplay::LogReplay(axc::core::DeepmsgClient& client, aeron_archive_t* archive, std::int64_t recording_id,
                     std::int64_t start_position, std::int64_t stop_position, LogAdapter& log_adapter,
                     std::string replay_channel, std::int32_t replay_stream_id, LogReplayHost& host)
    : client_(&client),
      archive_(archive),
      recording_id_(recording_id),
      start_position_(start_position),
      stop_position_(stop_position),
      log_adapter_(&log_adapter),
      replay_channel_(std::move(replay_channel)),
      replay_stream_id_(replay_stream_id),
      host_(&host)
{
    // The replay must be anchored to a subscription that already exists; the
    // C client's replay otherwise creates it too late and loses the first
    // fragments (C2b's finding).
    auto add = client_->add_subscription(replay_channel_, replay_stream_id_);
    axc::core::SubPtr subscription{};
    while (true)
    {
        (void)client_->do_work();
        auto added = add.poll();
        if (!added)
        {
            host_->on_log_replay_error(added.error());
            closed_ = true;
            return;
        }
        if (added->has_value())
        {
            subscription = std::move(**added);
            break;
        }
    }
    subscription_ = std::move(subscription);

    aeron_subscription_constants_t constants{};
    if (0 != aeron_subscription_constants(subscription_.get(), &constants))
    {
        host_->on_log_replay_error(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "replay subscription constants failed"));
        closed_ = true;
        return;
    }

    aeron_archive_replay_params_t params{};
    if (0 != aeron_archive_replay_params_init(&params))
    {
        host_->on_log_replay_error(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "replay params init failed"));
        closed_ = true;
        return;
    }
    params.position = start_position_;
    params.length = stop_position_ - start_position_;
    params.subscription_registration_id = constants.registration_id;

    const int replay_result = aeron_archive_start_replay(&replay_session_id_, archive_, recording_id_,
                                                         replay_channel_.c_str(), replay_stream_id_, &params);
    if (0 != replay_result)
    {
        host_->on_log_replay_error(axc::core::make_error(
            axc::core::ErrorCode::internal_error, std::string{"start replay failed: "} + std::string{aeron_errmsg()}));
        closed_ = true;
        return;
    }
    log_session_id_ = static_cast<std::int32_t>(replay_session_id_);
    log_adapter_->subscription(subscription_.get());
    log_adapter_->client(client_);
}

LogReplay::~LogReplay()
{
    close();
}

int LogReplay::do_work()
{
    if (closed_)
    {
        return 0;
    }

    if (nullptr == log_adapter_->image())
    {
        aeron_image_t* image = aeron_subscription_image_by_session_id(subscription_.get(), log_session_id_);
        if (nullptr == image)
        {
            return 0;
        }

        aeron_image_constants_t image_constants{};
        (void)aeron_image_constants(image, &image_constants);
        if (image_constants.join_position != start_position_)
        {
            host_->on_log_replay_error(
                axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                      "joinPosition=" + std::to_string(image_constants.join_position) +
                                          " expected startPosition=" + std::to_string(start_position_)));
            closed_ = true;
            return 0;
        }

        log_adapter_->image(image);
        auto ready = host_->await_services_ready(replay_channel_, replay_stream_id_, log_session_id_, start_position_,
                                                 stop_position_, true, service::Role::follower);
        if (!ready)
        {
            host_->on_log_replay_error(ready.error());
            closed_ = true;
            return 0;
        }
        return 1;
    }

    auto polled = host_->replay_log_poll(*log_adapter_, stop_position_);
    if (!polled)
    {
        host_->on_log_replay_error(polled.error());
        closed_ = true;
        return 0;
    }
    return *polled;
}

bool LogReplay::is_done() const
{
    const bool done = !closed_ && log_adapter_->position() >= stop_position_ && !host_->is_snapshot_state();
    return done;
}

std::int64_t LogReplay::position() const
{
    return log_adapter_->position();
}

void LogReplay::close()
{
    if (closed_ && nullptr == subscription_)
    {
        return;
    }

    if (NULL_VALUE != replay_session_id_ && nullptr != archive_)
    {
        (void)aeron_archive_stop_replay(archive_, replay_session_id_);
        replay_session_id_ = NULL_VALUE;
    }
    (void)log_adapter_->disconnect();
    subscription_.reset();
    closed_ = true;
}

}  // namespace deepmsg::cluster::consensus
