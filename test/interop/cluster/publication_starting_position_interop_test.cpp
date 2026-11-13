// C4d: the smallest rig for a resumed publication. A plain publication and
// one created at a starting position (`init-term-id` / `term-id` /
// `term-offset` / `term-length`) on the reference Java driver, each with a
// subscription on the same channel: does an image appear, and where does it
// join? This isolates the driver from the cluster and the archive.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>

#include <aeronc.h>

#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using axc::core::DeepmsgClient;

constexpr std::int32_t STREAM_ID = 100;
constexpr std::int32_t PORT = 24138;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

class PublicationStartingPositionInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const std::string jar = aeron_jar();
        if (jar.empty() || !std::filesystem::exists(jar))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar;
        }
        jar_ = jar;

        std::string pattern = "/tmp/axc-c4d-pub-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";

        std::vector<std::string> arguments = {"--add-opens",
                                              "java.base/jdk.internal.misc=ALL-UNNAMED",
                                              "--add-opens",
                                              "java.base/sun.nio.ch=ALL-UNNAMED",
                                              "-Daeron.dir=" + aeron_dir_,
                                              "-Daeron.print.configuration=false",
                                              "-Daeron.threading.mode=SHARED",
                                              "-cp",
                                              jar_,
                                              "io.aeron.driver.MediaDriver"};
        auto process = axc::interop::ChildProcess::spawn("java", arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        driver_ = std::make_unique<axc::interop::ChildProcess>(std::move(*process));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(driver_->running()) << "the Java media driver exited while starting";
            axc::core::ClientConfig config;
            config.dir = aeron_dir_;
            auto client = DeepmsgClient::connect(config);
            if (client)
            {
                client_ = std::move(*client);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "the Java media driver never published its CnC file";
    }

    void TearDown() override
    {
        client_.close();
        driver_.reset();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::string channel(std::string_view parameters) const
    {
        return "aeron:udp?endpoint=localhost:" + std::to_string(PORT) + "|" + std::string{parameters};
    }

    [[nodiscard]] axc::core::ExPubPtr add_publication(const std::string& channel)
    {
        auto add = client_.add_exclusive_publication(channel, STREAM_ID);
        while (true)
        {
            (void)client_.do_work();
            auto polled = add.poll();
            if (!polled)
            {
                break;
            }
            if (polled->has_value())
            {
                return std::move(**polled);
            }
        }
        return nullptr;
    }

    [[nodiscard]] aeron_image_t* await_image(aeron_subscription_t* subscription, std::int32_t session_id,
                                             std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            (void)client_.do_work();
            if (aeron_image_t* image = aeron_subscription_image_by_session_id(subscription, session_id))
            {
                return image;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return nullptr;
    }

    std::string jar_{};
    std::string directory_{};
    std::string aeron_dir_{};
    std::unique_ptr<axc::interop::ChildProcess> driver_{};
    DeepmsgClient client_{};
};

TEST_F(PublicationStartingPositionInteropTest, aPlainPublicationCreatesAnImage)
{
    const std::string plain = channel("term-length=1m");
    auto publication = add_publication(plain);
    ASSERT_NE(nullptr, publication);

    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication.get(), &constants));
    auto add = client_.add_subscription(plain, STREAM_ID);
    while (true)
    {
        (void)client_.do_work();
        auto polled = add.poll();
        if (!polled)
        {
            FAIL() << "subscription failed: " << polled.error().message.view();
        }
        if (polled->has_value())
        {
            auto subscription = std::move(**polled);
            aeron_image_t* image = await_image(subscription.get(), constants.session_id, std::chrono::seconds(10));
            ASSERT_NE(nullptr, image) << "a plain publication produced no image";
            aeron_image_constants_t image_constants{};
            ASSERT_EQ(0, aeron_image_constants(image, &image_constants));
            EXPECT_EQ(0, image_constants.join_position);
            return;
        }
    }
}

TEST_F(PublicationStartingPositionInteropTest, aPublicationWithAStartingPositionCreatesAnImageThere)
{
    // The position the cluster resumes at: 480 into a 1 MiB term, initial term
    // id 7 (both ids equal because the position is inside the first term).
    const std::string resumed =
        channel("init-term-id=7|term-id=7|term-offset=480|term-length=1048576|alias=log|tags=1234");
    auto publication = add_publication(resumed);
    if (nullptr == publication)
    {
        GTEST_SKIP() << "the driver refused a publication with a starting position";
    }

    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication.get(), &constants));
    EXPECT_EQ(7, constants.initial_term_id);

    auto add = client_.add_subscription(resumed, STREAM_ID);
    while (true)
    {
        (void)client_.do_work();
        auto polled = add.poll();
        if (!polled)
        {
            FAIL() << "subscription failed: " << polled.error().message.view();
        }
        if (polled->has_value())
        {
            auto subscription = std::move(**polled);
            aeron_image_t* image = await_image(subscription.get(), constants.session_id, std::chrono::seconds(10));
            ASSERT_NE(nullptr, image) << "a publication with a starting position produced no image";
            aeron_image_constants_t image_constants{};
            ASSERT_EQ(0, aeron_image_constants(image, &image_constants));
            EXPECT_EQ(480, image_constants.join_position);
            return;
        }
    }
}

TEST_F(PublicationStartingPositionInteropTest, aSessionFilteredSubscriptionSeesTheResumedPublication)
{
    const std::string resumed =
        channel("init-term-id=7|term-id=7|term-offset=480|term-length=1048576|alias=log|tags=1234");
    auto publication = add_publication(resumed);
    ASSERT_NE(nullptr, publication);

    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication.get(), &constants));

    // The cluster's JoinLog channel pins the publication's session id.
    const std::string filtered = resumed + "|session-id=" + std::to_string(constants.session_id);
    auto add = client_.add_subscription(filtered, STREAM_ID);
    while (true)
    {
        (void)client_.do_work();
        auto polled = add.poll();
        if (!polled)
        {
            FAIL() << "subscription failed: " << polled.error().message.view();
        }
        if (polled->has_value())
        {
            auto subscription = std::move(**polled);
            aeron_image_t* image = await_image(subscription.get(), constants.session_id, std::chrono::seconds(10));
            ASSERT_NE(nullptr, image) << "a session-filtered subscription saw no image";
            aeron_image_constants_t image_constants{};
            ASSERT_EQ(0, aeron_image_constants(image, &image_constants));
            EXPECT_EQ(480, image_constants.join_position);
            return;
        }
    }
}

TEST_F(PublicationStartingPositionInteropTest, anotherClientSeesTheResumedPublication)
{
    const std::string resumed =
        channel("init-term-id=7|term-id=7|term-offset=480|term-length=1048576|alias=log|tags=1234");
    auto publication = add_publication(resumed);
    ASSERT_NE(nullptr, publication);

    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication.get(), &constants));

    axc::core::ClientConfig config;
    config.dir = aeron_dir_;
    auto other = DeepmsgClient::connect(config);
    ASSERT_TRUE(other) << other.error().message.view();

    const std::string filtered = resumed + "|session-id=" + std::to_string(constants.session_id);
    auto add = other->add_subscription(filtered, STREAM_ID);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline)
    {
        (void)client_.do_work();
        (void)other->do_work();
        auto polled = add.poll();
        if (!polled)
        {
            FAIL() << "subscription failed: " << polled.error().message.view();
        }
        if (polled->has_value())
        {
            auto subscription = std::move(**polled);
            while (std::chrono::steady_clock::now() < deadline)
            {
                (void)other->do_work();
                if (aeron_image_t* image =
                        aeron_subscription_image_by_session_id(subscription.get(), constants.session_id))
                {
                    aeron_image_constants_t image_constants{};
                    ASSERT_EQ(0, aeron_image_constants(image, &image_constants));
                    EXPECT_EQ(480, image_constants.join_position);
                    other->close();
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }
    other->close();
    FAIL() << "another client saw no image for the resumed publication";
}

TEST_F(PublicationStartingPositionInteropTest, theClusterShapedResumedPublicationCreatesAnImage)
{
    // The exact shape the cluster uses: a 16 MiB term, the recording's (large,
    // possibly negative) initial term id, append position 480, alias=log and a
    // tag.
    constexpr std::int32_t INITIAL_TERM_ID = 2019489299;
    const std::string resumed =
        channel("alias=log|tags=221|init-term-id=2019489299|term-id=2019489299|term-offset=480|term-length=16777216");
    auto publication = add_publication(resumed);
    if (nullptr == publication)
    {
        GTEST_SKIP() << "the driver refused the cluster-shaped publication";
    }

    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication.get(), &constants));
    EXPECT_EQ(INITIAL_TERM_ID, constants.initial_term_id);

    const std::string filtered = resumed + "|session-id=" + std::to_string(constants.session_id);
    auto add = client_.add_subscription(filtered, STREAM_ID);
    while (true)
    {
        (void)client_.do_work();
        auto polled = add.poll();
        if (!polled)
        {
            FAIL() << "subscription failed: " << polled.error().message.view();
        }
        if (polled->has_value())
        {
            auto subscription = std::move(**polled);
            aeron_image_t* image = await_image(subscription.get(), constants.session_id, std::chrono::seconds(10));
            ASSERT_NE(nullptr, image) << "the cluster-shaped publication produced no image";
            aeron_image_constants_t image_constants{};
            ASSERT_EQ(0, aeron_image_constants(image, &image_constants));
            EXPECT_EQ(480, image_constants.join_position);
            return;
        }
    }
}

}  // namespace
