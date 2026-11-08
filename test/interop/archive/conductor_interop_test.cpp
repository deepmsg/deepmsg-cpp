// C criterion for card A2a: the reference C archive client has to connect and
// authenticate against our conductor.
//
// Both peers run in this process: `aeronmd` is the interop fixture's C driver,
// the archive is `ArchiveConductor`, and the client is
// `aeron_archive_async_connect` from the pinned `aeron_archive_c_client_static`.
//
// The client's response channel is set explicitly to `aeron:ipc`: its default is
// an MDS response channel (`control-mode=response`), whose URI rewriting the
// conductor does not implement yet (records/a2a-part2.md §3).

#include <gtest/gtest.h>

#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "archive/server/archive_conductor.h"
#include "fixture/c_driver.h"

#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT "aeronmd"
#endif

namespace
{

using deepmsg::archive::server::ArchiveConductor;
using deepmsg::archive::server::ArchiveConductorContext;
using deepmsg::archive::server::Authenticator;
using deepmsg::archive::server::SampleAuthenticator;

class ConductorInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-conductor-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        archive_dir_ = root_ + "/archive";
        std::filesystem::create_directories(archive_dir_);

        if (!std::filesystem::exists(AXC_AERONMD_DEFAULT))
        {
            GTEST_SKIP() << "aeronmd not built at " << AXC_AERONMD_DEFAULT;
        }

        auto driver = axc::interop::CDriver::start(AXC_AERONMD_DEFAULT, aeron_dir_);
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::move(*driver);
    }

    void TearDown() override
    {
        // `aeron_archive_close` returns the duplicated context (and the Aeron
        // client it owns); the original context was handed to the async connect
        // with `owns_aeron_client=false`, so closing it only frees the struct
        // (`aeron_archive_async_connect_leak_test.cpp:143-145`).
        if (nullptr != client_archive_)
        {
            EXPECT_EQ(0, aeron_archive_close(client_archive_));
            client_archive_ = nullptr;
        }
        if (nullptr != archive_context_)
        {
            EXPECT_EQ(0, aeron_archive_context_close(archive_context_));
            archive_context_ = nullptr;
        }
        driver_.stop();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    [[nodiscard]] std::unique_ptr<ArchiveConductor> start_conductor(Authenticator* authenticator = nullptr)
    {
        ArchiveConductorContext context;
        context.archive_dir = archive_dir_;
        context.aeron_dir = aeron_dir_;
        context.control_channel = "aeron:ipc";
        context.control_stream_id = CONTROL_STREAM_ID;
        context.archive_id = 42;
        context.authenticator = authenticator;

        auto conductor = ArchiveConductor::create(context);
        EXPECT_TRUE(conductor) << (conductor ? std::string{} : std::string{conductor.error().message.view()});
        return conductor ? std::move(*conductor) : nullptr;
    }

    /// Drives our conductor and the client until the connect completes.
    [[nodiscard]] int connect_client(ArchiveConductor& conductor, const std::string& credentials)
    {
        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return -1;
        }
        aeron_archive_context_set_aeron_directory_name(context, aeron_dir_.c_str());
        aeron_archive_context_set_control_request_channel(context, "aeron:ipc");
        aeron_archive_context_set_control_request_stream_id(context, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(context, "aeron:ipc");
        aeron_archive_context_set_control_response_stream_id(context, RESPONSE_STREAM_ID);
        if (!credentials.empty())
        {
            credentials_.initial_text = credentials;
            credentials_.initial = {credentials_.initial_text.data(),
                                    static_cast<std::uint32_t>(credentials_.initial_text.size())};
            credentials_.challenge = {credentials_.challenge_text.data(),
                                      static_cast<std::uint32_t>(credentials_.challenge_text.size())};
            aeron_archive_context_set_credentials_supplier(context, &static_credentials, &challenge_credentials,
                                                           &credentials_free, &credentials_);
        }

        aeron_archive_async_connect_t* async = nullptr;
        if (0 != aeron_archive_async_connect(&async, context))
        {
            last_error_ = aeron_errmsg();
            aeron_archive_context_close(context);
            return -1;
        }

        int result = -1;
        for (int iteration = 0; iteration < 20'000; iteration++)
        {
            conductor.do_work();

            aeron_archive_t* archive = nullptr;
            const int poll = aeron_archive_async_connect_poll(&archive, async);
            if (poll < 0)
            {
                last_error_ = aeron_errmsg();
                result = poll;
                break;
            }
            // Completion is signalled by the poll producing the archive handle
            // (`aeron_archive_async_connect_step` is not part of the built
            // `aeron_archive_c_client_static`, so it is not used here).
            if (nullptr != archive)
            {
                result = 0;
                client_archive_ = archive;
                break;
            }
            ::usleep(200);
        }

        async_ = async;
        archive_context_ = context;
        return result;
    }

    static void credentials_free(aeron_archive_encoded_credentials_t* credentials, void* clientd)
    {
        (void)credentials;
        (void)clientd;
    }

    static aeron_archive_encoded_credentials_t* static_credentials(void* clientd)
    {
        return &static_cast<Credentials*>(clientd)->initial;
    }

    static aeron_archive_encoded_credentials_t* challenge_credentials(aeron_archive_encoded_credentials_t* challenge,
                                                                      void* clientd)
    {
        // `SampleAuthenticator` challenges `admin:adminC` and only accepts
        // `admin:CSadmin` as the answer.
        (void)challenge;
        return &static_cast<Credentials*>(clientd)->challenge;
    }

    static constexpr std::int32_t CONTROL_STREAM_ID = 10;
    static constexpr std::int32_t RESPONSE_STREAM_ID = 11;

    /// The three values the client's credentials supplier can return. The
    /// challenge answer is the one `SampleAuthenticator` expects.
    struct Credentials
    {
        std::string initial_text;
        std::string challenge_text{"admin:CSadmin"};
        aeron_archive_encoded_credentials_t initial{};
        aeron_archive_encoded_credentials_t challenge{};
    };

    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    axc::interop::CDriver driver_;
    aeron_archive_async_connect_t* async_{nullptr};
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_t* client_archive_{nullptr};
    Credentials credentials_;
    std::string last_error_;
};

TEST_F(ConductorInteropTest, theClientConnectsWithTheDefaultAuthenticator)
{
    auto conductor = start_conductor();
    ASSERT_TRUE(conductor);

    EXPECT_EQ(0, connect_client(*conductor, "admin:admin"))
        << "the async connect did not complete: " << last_error_
        << " warnings: " << (conductor->warnings().empty() ? std::string{"none"} : conductor->warnings().back());
    EXPECT_EQ(1u, conductor->session_count());

    // The client asked for the archive id once the connect completed.
    EXPECT_TRUE(conductor->warnings().empty());
}

TEST_F(ConductorInteropTest, theClientAuthenticatesThroughAChallenge)
{
    SampleAuthenticator authenticator;
    auto conductor = start_conductor(&authenticator);
    ASSERT_TRUE(conductor);

    // `admin:adminC` makes the archive answer with a challenge; the client's
    // supplier answers `admin:CSadmin`.
    EXPECT_EQ(0, connect_client(*conductor, "admin:adminC"))
        << "the challenge flow did not complete: " << last_error_
        << " warnings: " << (conductor->warnings().empty() ? std::string{"none"} : conductor->warnings().back());
    EXPECT_EQ(1u, conductor->session_count());
    // The authenticator only forgets a session once the challenge answer was
    // accepted (`SampleAuthenticator.on_challenged_session`).
    EXPECT_EQ(0u, authenticator.tracked_sessions());
    EXPECT_EQ(1u, authenticator.accepted_challenge_responses());
}

}  // namespace
