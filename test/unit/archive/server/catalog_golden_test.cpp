// Golden criterion for card A1a: the catalog written by the reference Java
// implementation has to be read back field by field.
//
// `test/golden/archive/catalog/archive.catalog` was produced by
// `CatalogGoldenGen` (reference Aeron 1.54.0); `expected.txt` is its own dump of
// the same file. Regenerate both, never edit them.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include "archive/server/catalog.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogEntry;
using deepmsg::archive::server::CatalogOpenOptions;
using io::aeron::archive::codecs::RecordingState;

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/archive/catalog";
}

[[nodiscard]] std::map<std::string, std::string> read_expected()
{
    std::map<std::string, std::string> values;
    std::ifstream input{golden_dir() + "/expected.txt"};
    std::string line;
    while (std::getline(input, line))
    {
        const std::size_t equals = line.find('=');
        if (std::string::npos == equals)
        {
            continue;
        }
        values[line.substr(0, equals)] = line.substr(equals + 1);
    }
    return values;
}

[[nodiscard]] std::string state_name(std::int32_t state)
{
    switch (state)
    {
        case RecordingState::INVALID:
            return "INVALID";
        case RecordingState::VALID:
            return "VALID";
        case RecordingState::DELETED:
            return "DELETED";
        default:
            return "NULL_VALUE";
    }
}

TEST(CatalogGoldenTest, readsEveryFieldOfTheReferenceCatalog)
{
    const std::string dir = golden_dir();
    const auto expected = read_expected();
    ASSERT_FALSE(expected.empty()) << "missing " << dir << "/expected.txt";

    CatalogOpenOptions options;
    options.archive_dir = dir;
    options.catalog_capacity = Catalog::MIN_CAPACITY;
    options.writable = false;

    auto catalog = Catalog::open(options);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    const auto expect = [&](const std::string& key, std::int64_t actual)
    {
        const auto found = expected.find(key);
        ASSERT_NE(expected.end(), found) << "expected.txt has no " << key;
        EXPECT_EQ(found->second, std::to_string(actual)) << key;
    };

    expect("header.version", catalog->version());
    expect("header.nextRecordingId", catalog->next_recording_id());
    expect("header.alignment", catalog->alignment());
    expect("capacity", catalog->capacity());
    expect("entryCount", catalog->entry_count());

    int index = 0;
    const int count = catalog->for_each(
        [&](CatalogEntry& entry)
        {
            const std::string prefix = "entry." + std::to_string(index++);

            const auto found_state = expected.find(prefix + ".state");
            ASSERT_NE(expected.end(), found_state) << "expected.txt has no " << prefix << ".state";
            EXPECT_EQ(found_state->second, state_name(entry.header.state())) << prefix << ".state";

            expect(prefix + ".offset", entry.recording_descriptor_offset);
            expect(prefix + ".length", entry.header.length());
            expect(prefix + ".checksum", entry.header.checksum());
            expect(prefix + ".recordingId", entry.descriptor.recordingId());
            expect(prefix + ".startPosition", entry.descriptor.startPosition());
            expect(prefix + ".stopPosition", entry.descriptor.stopPosition());
            expect(prefix + ".startTimestamp", entry.descriptor.startTimestamp());
            expect(prefix + ".stopTimestamp", entry.descriptor.stopTimestamp());
            expect(prefix + ".initialTermId", entry.descriptor.initialTermId());
            expect(prefix + ".segmentFileLength", entry.descriptor.segmentFileLength());
            expect(prefix + ".termBufferLength", entry.descriptor.termBufferLength());
            expect(prefix + ".mtuLength", entry.descriptor.mtuLength());
            expect(prefix + ".sessionId", entry.descriptor.sessionId());
            expect(prefix + ".streamId", entry.descriptor.streamId());

            const auto expect_text = [&](const std::string& key, const std::string& actual)
            {
                const auto found = expected.find(key);
                ASSERT_NE(expected.end(), found) << "expected.txt has no " << key;
                EXPECT_EQ(found->second, actual) << key;
            };
            expect_text(prefix + ".strippedChannel", entry.descriptor.getStrippedChannelAsString());
            expect_text(prefix + ".originalChannel", entry.descriptor.getOriginalChannelAsString());
            expect_text(prefix + ".sourceIdentity", entry.descriptor.getSourceIdentityAsString());
        });

    EXPECT_EQ(index, count);
    EXPECT_EQ(3, count) << "the reference file holds three descriptors, one of them INVALID";
}

}  // namespace
