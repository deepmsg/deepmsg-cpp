#include "archive/server/create_replay_publication_session.h"

#include <utility>

#include <aeronc.h>

#include "aeron_client_error.h"

namespace deepmsg::archive::server
{

CreateReplayPublicationSession::CreateReplayPublicationSession(Config config,
                                                               CreateReplayPublicationSessionHandler& handler) noexcept
    : config_(std::move(config)), handler_(handler)
{
}

CreateReplayPublicationSession::~CreateReplayPublicationSession()
{
    close();
}

void CreateReplayPublicationSession::close() noexcept
{
    // The AsyncAdd destructor cancels the pending registration; `:86-93` removes
    // a completed publication, which cannot happen here because a successful
    // poll clears the add before the hand-off.
    add_.reset();
}

int CreateReplayPublicationSession::do_work()
{
    // `CreateReplayPublicationSession.doWork` (`:126-183`).
    if (done_)
    {
        return 0;
    }

    if (!add_.has_value())
    {
        // `:132-136`: the first pass starts the asynchronous add.
        add_.emplace(handler_.add_replay_publication(config_.replay_channel, config_.replay_stream_id));
    }

    auto polled = add_->poll();
    if (!polled)
    {
        const int error_code = aeron_errcode();
        add_.reset();

        // `:146-151`: a temporarily unavailable resource is retried after idle.
        if (AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE == error_code)
        {
            return 0;
        }

        // `:152-156`: done, onReplayEnd, ERROR, rethrow.
        done_ = true;
        handler_.replay_publication_failed(config_.control_session_id, config_.correlation_id,
                                           std::string{"failed to create replay publication: "} + aeron_errmsg());
        return 1;
    }

    if (!polled->has_value())
    {
        return 0;
    }

    // `:159-179`: hand the publication to the conductor, which creates the
    // ReplaySession.
    std::optional<axc::core::ExPubPtr> publication = std::move(*polled);
    add_.reset();
    done_ = true;
    handler_.new_replay_session(
        config_, std::make_unique<ExclusiveReplayPublication>(publication->release(), config_.replay_channel));
    return 1;
}

}  // namespace deepmsg::archive::server
