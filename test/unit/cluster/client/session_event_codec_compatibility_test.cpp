// C3c / XC-1: `SessionEventCodecCompatibilityTest.readingVersion12UsingVersion5Codec`
// (`aeron-cluster/src/test/java/io/aeron/cluster/SessionEventCodecCompatibilityTest.java`).
//
// The reference encodes with the v12 codecs and decodes with the v5 ones; both
// are generated from the one schema by `sbe.schema.transform.version`
// (`build.gradle`, `generateTestCodecsV12` / `generateTestCodecsV5`). Here the
// current schema is v17 and the older decoder is generated as v5 by
// `axc_sbe_cluster_codecs_v5` -- the same transform, C++-side.
//
// What is under test is SBE's forward compatibility: the 8-byte header carries
// the *writer's* block length and schema version, so an older decoder reads
// the fields it knows at their unchanged offsets and finds the later fields --
// including the var-data `detail` that follows a longer block -- by skipping
// the writer's block length.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs_v5/SessionEvent.h"

namespace
{

namespace current = io::aeron::cluster::codecs;
namespace v5 = io::aeron::cluster::codecs::v5;

constexpr std::int32_t semantic_version(std::int32_t major, std::int32_t minor, std::int32_t patch)
{
    return (major << 16) | (minor << 8) | patch;
}

TEST(SessionEventCodecCompatibilityTest, readingVersion12UsingVersion5Codec)
{
    constexpr std::int64_t cluster_session_id = -4623823;
    constexpr std::int64_t correlation_id = 3583456348756843L;
    constexpr std::int64_t leadership_term_id = 10'000'000'000L;
    constexpr std::int32_t leader_member_id = 2;
    constexpr current::EventCode::Value code = current::EventCode::REDIRECT;
    constexpr std::int32_t version = semantic_version(3, 47, 123);
    constexpr std::string_view detail = "some very detailed message";

    std::vector<std::uint8_t> buffer(1024, 0);

    current::SessionEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(correlation_id)
        .leadershipTermId(leadership_term_id)
        .leaderMemberId(leader_member_id)
        .code(code)
        .version(version);
    encoder.putDetail(detail);

    v5::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(buffer.data()), 0, v5::MessageHeader::sbeSchemaVersion(), buffer.size());

    v5::SessionEvent decoder;
    decoder.wrapForDecode(reinterpret_cast<char*>(buffer.data()), v5::MessageHeader::encodedLength(),
                          header.blockLength(), header.version(), buffer.size());

    // The two codecs are the same schema: same id, the writer is newer, and
    // its block grew (`SessionEventCodecCompatibilityTest.java:57-66`).
    EXPECT_EQ(v5::SessionEvent::sbeSchemaId(), current::SessionEvent::sbeSchemaId());
    EXPECT_GT(current::SessionEvent::sbeSchemaVersion(), v5::SessionEvent::sbeSchemaVersion());
    EXPECT_GT(current::SessionEvent::sbeBlockLength(), v5::SessionEvent::sbeBlockLength());

    // The header carries the writer's values; the decoder's constants stay
    // v5's (`SessionEventCodecCompatibilityTest.java:68-71`).
    EXPECT_EQ(v5::SessionEvent::sbeSchemaId(), header.schemaId());
    EXPECT_EQ(v5::MessageHeader::sbeSchemaVersion(), header.sbeSchemaVersion());
    EXPECT_EQ(current::SessionEvent::sbeSchemaVersion(), header.version());
    EXPECT_EQ(current::SessionEvent::sbeBlockLength(), header.blockLength());

    // Every required field reads, and `detail` is found after the *writer's*
    // longer block (`SessionEventCodecCompatibilityTest.java:73-78`).
    EXPECT_EQ(cluster_session_id, decoder.clusterSessionId());
    EXPECT_EQ(correlation_id, decoder.correlationId());
    EXPECT_EQ(leadership_term_id, decoder.leadershipTermId());
    EXPECT_EQ(leader_member_id, decoder.leaderMemberId());
    EXPECT_EQ(static_cast<int>(code), static_cast<int>(decoder.code()));
    EXPECT_EQ(detail, decoder.getDetailAsString());
}

}  // namespace
