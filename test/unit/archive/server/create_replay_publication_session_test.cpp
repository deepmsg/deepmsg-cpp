// A3b-4 criteria: `CreateReplayPublicationSession` (`CreateReplayPublicationSession.java:26-184`):
// the successful hand-off, the RESOURCE_TEMPORARILY_UNAVAILABLE retry and the
// fatal error path, with a poll function instead of a driver.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <aeronc.h>

// The internal error header does not wrap its declaration in `extern "C"`.
extern "C"
{
#include "util/aeron_error.h"
}

#include "archive/server/create_replay_publication_session.h"

namespace
{

using axc::core::AsyncAdd;
using axc::core::ExPubPtr;
using deepmsg::archive::server::CreateReplayPublicationSession;
using deepmsg::archive::server::CreateReplayPublicationSessionHandler;
using deepmsg::archive::server::ReplayPublication;

using Raw = aeron_exclusive_publication_t*;

struct PollState
{
    int calls{0};
    int success_on_call{1};
    int failure_code{0};
};

int poll_stateful(Raw* handle, void* async)
{
    auto* state = static_cast<PollState*>(async);
    state->calls++;

    if (0 != state->failure_code)
    {
        // `aeron_errcode()` reads the Aerogon/Aeron per-thread state, which only
        // `aeron_err_set` (the AERON_SET_ERR macro) writes.
        AERON_SET_ERR(state->failure_code, "%s", "injected publish failure");
        return -1;
    }

    if (state->calls < state->success_on_call)
    {
        return 0;
    }

    *handle = nullptr;  // a null handle is enough for the hand-off criterion
    return 1;
}

class FakeHandler final : public CreateReplayPublicationSessionHandler
{
public:
    enum class Mode
    {
        success,
        pending_then_success,
        unavailable_then_success,
        fatal
    };

    axc::core::AsyncAdd<axc::core::ExPubPtr> add_replay_publication(std::string_view channel,
                                                                    std::int32_t stream_id) override
    {
        add_count++;
        last_channel.assign(channel);
        last_stream_id = stream_id;

        auto state = std::make_unique<PollState>();
        switch (mode)
        {
            case Mode::pending_then_success:
                state->success_on_call = 2;
                break;
            case Mode::unavailable_then_success:
                if (1 == add_count)
                {
                    state->failure_code = 10;  // AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE
                }
                break;
            case Mode::fatal:
                state->failure_code = 22;
                break;
            case Mode::success:
            default:
                break;
        }

        PollState* raw_state = state.get();
        poll_states.push_back(std::move(state));
        return AsyncAdd<ExPubPtr>(nullptr, raw_state, poll_stateful, nullptr);
    }

    void replay_publication_failed(std::int64_t control_session_id, std::int64_t correlation_id,
                                   std::string_view message) override
    {
        failed_control_session_id = control_session_id;
        failed_correlation_id = correlation_id;
        failure_message.assign(message);
    }

    void new_replay_session(const CreateReplayPublicationSession::Config& session_config,
                            std::unique_ptr<ReplayPublication> session_publication) override
    {
        new_session_count++;
        captured = session_config;
        publication = std::move(session_publication);
    }

    Mode mode{Mode::success};
    int add_count{0};
    int new_session_count{0};
    std::int64_t failed_control_session_id{0};
    std::int64_t failed_correlation_id{0};
    std::string failure_message;
    std::string last_channel;
    std::int32_t last_stream_id{0};
    CreateReplayPublicationSession::Config captured;
    std::unique_ptr<ReplayPublication> publication;
    std::vector<std::unique_ptr<PollState>> poll_states;
};

CreateReplayPublicationSession::Config make_config()
{
    CreateReplayPublicationSession::Config config;
    config.correlation_id = 42;
    config.recording_id = 99;
    config.replay_position = 0;
    config.replay_length = 4096;
    config.start_position = 0;
    config.stop_position = 4096;
    config.segment_file_length = 128 * 1024;
    config.term_buffer_length = 64 * 1024;
    config.stream_id = 77;
    config.replay_stream_id = 66;
    config.replay_channel = "aeron:udp?endpoint=localhost:6666";
    config.file_io_max_length = 4096;
    config.control_session_id = 11;
    config.limit_counter_id = -1;
    return config;
}

TEST(CreateReplayPublicationSessionTest, handsThePublicationToTheConductor)
{
    FakeHandler handler;
    CreateReplayPublicationSession session(make_config(), handler);

    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(1, handler.add_count);
    EXPECT_EQ("aeron:udp?endpoint=localhost:6666", handler.last_channel);
    EXPECT_EQ(66, handler.last_stream_id);
    ASSERT_EQ(1, handler.new_session_count);
    EXPECT_EQ(99, handler.captured.recording_id);
    EXPECT_EQ(11, handler.captured.control_session_id);
    ASSERT_NE(nullptr, handler.publication);
    EXPECT_TRUE(handler.failure_message.empty());
}

TEST(CreateReplayPublicationSessionTest, staysPendingUntilTheAddCompletes)
{
    FakeHandler handler;
    handler.mode = FakeHandler::Mode::pending_then_success;
    CreateReplayPublicationSession session(make_config(), handler);

    EXPECT_EQ(0, session.do_work());
    EXPECT_FALSE(session.is_done());
    EXPECT_EQ(0, handler.new_session_count);

    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(1, handler.new_session_count);
}

TEST(CreateReplayPublicationSessionTest, retriesAResourceThatIsTemporarilyUnavailable)
{
    FakeHandler handler;
    handler.mode = FakeHandler::Mode::unavailable_then_success;
    CreateReplayPublicationSession session(make_config(), handler);

    EXPECT_EQ(0, session.do_work());
    EXPECT_FALSE(session.is_done());
    EXPECT_TRUE(handler.failure_message.empty());

    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(2, handler.add_count);
    EXPECT_EQ(1, handler.new_session_count);
}

TEST(CreateReplayPublicationSessionTest, aFatalErrorEndsTheReplayWithAnError)
{
    FakeHandler handler;
    handler.mode = FakeHandler::Mode::fatal;
    CreateReplayPublicationSession session(make_config(), handler);

    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(0, handler.new_session_count);
    EXPECT_EQ(11, handler.failed_control_session_id);
    EXPECT_EQ(42, handler.failed_correlation_id);
    EXPECT_NE(std::string::npos, handler.failure_message.find("failed to create replay publication"));
}

}  // namespace
