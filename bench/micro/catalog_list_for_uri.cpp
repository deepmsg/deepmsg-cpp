// A6e: `listRecordingsForUri` over a catalog with N recordings, linear scan
// against a prototype `(streamId, channel)` secondary index (P-A8 / A6-6).
//
// The prototype lives here, not in the archive: the card is the reading, and
// the reference has no secondary index at all. Both arms use the reference's
// own predicate (`streamId` + `Catalog::originalChannelContains`, a *substring*
// match), so the index can only narrow the candidates; whatever it returns is
// post-filtered with the same test and emitted in ascending recording id.
//
// A prefix index is only sound when the query fragment is a channel URI (no
// recorded channel starts with anything else, so a fragment that starts with
// `aeron:` cannot match mid-string). Any other fragment falls back to the
// linear scan, which is what the "mid" query below proves.

#include <benchmark/benchmark.h>

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "archive/server/catalog.h"
#include "archive/server/catalog_index.h"
#include "archive/server/codecs.h"

namespace
{

using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogIndex;
using deepmsg::archive::server::CatalogOptions;

constexpr std::int32_t CHANNELS = 1000;
constexpr std::int32_t STREAMS = 10;
constexpr std::int32_t MTU_LENGTH = 1408;
constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 128 * 1024;
constexpr std::int64_t INITIAL_TERM_ID = 5;
constexpr std::size_t COUNT = 100;
constexpr std::int64_t CATALOG_CAPACITY = 64 * 1024 * 1024;

/// The one endpoint every query in the "same prefix" shape asks for.
constexpr const char* SAME_ENDPOINT = "aeron:udp?endpoint=localhost:20121";

std::string channel_endpoint(int index, bool multi)
{
    if (!multi)
    {
        return SAME_ENDPOINT;
    }
    return "aeron:udp?endpoint=localhost:" + std::to_string(20000 + index % CHANNELS);
}

std::string original_channel(int index, bool multi)
{
    // Real recordings carry the publication's session id, so every original
    // channel string is unique; the query is a prefix without it.
    return channel_endpoint(index, multi) + "|session-id=" + std::to_string(1000 + index);
}

std::int32_t stream_of(int index, bool multi)
{
    return multi ? static_cast<std::int32_t>((index / CHANNELS) % STREAMS) : 0;
}

std::string next_temp_dir()
{
    static int counter = 0;
    const std::filesystem::path path =
        std::filesystem::temp_directory_path() /
        ("axc-catalog-bench-" + std::to_string(::getpid()) + "-" + std::to_string(counter++));
    std::filesystem::create_directories(path);
    return path.string();
}

struct Sample
{
    std::string label;
    std::int32_t stream_id;
    std::string fragment;
    std::size_t count;
};

/// The two query kinds of the reading: a channel prefix (indexed) and a
/// fragment in the middle (must fall back to the linear scan).
std::vector<Sample> samples(bool multi)
{
    return {
        {"prefix", 0, multi ? "aeron:udp?endpoint=localhost:20000" : SAME_ENDPOINT, COUNT},
        {"mid", 0, multi ? "endpoint=localhost:20000" : "endpoint=localhost:20121", COUNT},
    };
}

struct Fixture
{
    Fixture(int recordings, bool multi) : recordings_(recordings), multi_(multi)
    {
        dir_ = next_temp_dir();

        CatalogOptions options;
        options.archive_dir = dir_;
        options.file_sync_level = 0;
        options.catalog_capacity = CATALOG_CAPACITY;

        auto created = Catalog::create(options);
        if (!created)
        {
            std::fprintf(stderr, "catalog create failed: %s\n", std::string{created.error().message.view()}.c_str());
            std::exit(1);
        }
        catalog_ = std::move(*created);

        for (int index = 0; index < recordings; index++)
        {
            auto id = catalog_->add_new_recording(0, 0, INITIAL_TERM_ID, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1,
                                                  stream_of(index, multi_), channel_endpoint(index, multi_),
                                                  original_channel(index, multi_), "source");
            if (!id)
            {
                std::fprintf(stderr, "add_new_recording failed: %s\n", std::string{id.error().message.view()}.c_str());
                std::exit(1);
            }
            keys_[original_channel(index, multi_)].push_back(*id);
        }
    }

    ~Fixture()
    {
        if (catalog_)
        {
            (void)catalog_->close();
        }
        std::error_code error;
        std::filesystem::remove_all(dir_, error);
    }

    Fixture(const Fixture&) = delete;
    Fixture& operator=(const Fixture&) = delete;

    [[nodiscard]] int recordings() const noexcept
    {
        return recordings_;
    }

    [[nodiscard]] bool multi() const noexcept
    {
        return multi_;
    }

    [[nodiscard]] const Catalog& catalog() const noexcept
    {
        return *catalog_;
    }

    [[nodiscard]] const std::map<std::string, std::vector<std::int64_t>>& keys() const noexcept
    {
        return keys_;
    }

private:
    int recordings_;
    bool multi_;
    std::string dir_;
    std::optional<Catalog> catalog_;
    std::map<std::string, std::vector<std::int64_t>> keys_;
};

/// The reference's predicate (`ListRecordingsForUriSession.acceptDescriptor`).
bool accepts(std::span<const std::uint8_t> frame, std::int32_t stream_id, std::string_view fragment)
{
    deepmsg::archive::server::archive_codecs::RecordingDescriptor descriptor;
    descriptor.wrapForDecode(
        const_cast<char*>(reinterpret_cast<const char*>(frame.data())), Catalog::DESCRIPTOR_HEADER_LENGTH,
        deepmsg::archive::server::archive_codecs::RecordingDescriptor::SBE_BLOCK_LENGTH,
        deepmsg::archive::server::archive_codecs::RecordingDescriptor::SBE_SCHEMA_VERSION, frame.size());
    if (stream_id != descriptor.streamId())
    {
        return false;
    }
    const auto bytes =
        std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(fragment.data()), fragment.size()};
    return Catalog::original_channel_contains(frame, bytes);
}

/// `AbstractListRecordingsSession.doWork`'s walk: the dense id→offset index
/// from `from_recording_id`, one descriptor at a time, until `count` accepted.
std::vector<std::int64_t> linear(const Catalog& catalog, std::int64_t from_recording_id, std::int32_t stream_id,
                                 std::string_view fragment, std::size_t count)
{
    const CatalogIndex& catalog_index = catalog.index();
    const int last_position = catalog_index.last_position();
    const std::span<const std::int64_t> index = catalog_index.index();

    int position = CatalogIndex::find(index, from_recording_id, last_position);
    if (position < 0)
    {
        for (position = 0; position <= last_position; position += 2)
        {
            if (index[static_cast<std::size_t>(position)] >= from_recording_id)
            {
                break;
            }
        }
    }

    std::vector<std::int64_t> ids;
    for (; position <= last_position && ids.size() < count; position += 2)
    {
        std::span<std::uint8_t> frame;
        if (catalog.wrap_descriptor_at_offset(
                frame, static_cast<std::int32_t>(index[static_cast<std::size_t>(position) + 1])) < 0)
        {
            continue;
        }
        if (accepts(frame, stream_id, fragment))
        {
            ids.push_back(index[static_cast<std::size_t>(position)]);
        }
    }
    return ids;
}

/// The prototype: `(streamId, originalChannel) → ids`, a lexicographic key scan
/// for the fragment, then the same post-filter in ascending id order.
class UriIndex
{
public:
    explicit UriIndex(const std::map<std::string, std::vector<std::int64_t>>& keys) : keys_(keys) {}

    [[nodiscard]] std::vector<std::int64_t> collect(const Catalog& catalog, std::int64_t from_recording_id,
                                                    std::int32_t stream_id, std::string_view fragment,
                                                    std::size_t count) const
    {
        // A fragment that does not start the URI cannot be answered from a
        // prefix index: a mid-string match would be missed.
        if (fragment.empty() || 0 != fragment.rfind("aeron:", 0))
        {
            return linear(catalog, from_recording_id, stream_id, fragment, count);
        }

        std::vector<std::int64_t> candidates;
        for (auto key = keys_.lower_bound(std::string{fragment});
             key != keys_.end() && 0 == key->first.rfind(fragment, 0); ++key)
        {
            candidates.insert(candidates.end(), key->second.begin(), key->second.end());
        }
        std::sort(candidates.begin(), candidates.end());

        std::vector<std::int64_t> ids;
        for (const std::int64_t id : candidates)
        {
            if (ids.size() >= count)
            {
                break;
            }
            if (id < from_recording_id)
            {
                continue;
            }
            std::span<std::uint8_t> frame = catalog.wrap_descriptor(id);
            if (frame.empty())
            {
                continue;
            }
            if (accepts(frame, stream_id, fragment))
            {
                ids.push_back(id);
            }
        }
        return ids;
    }

private:
    const std::map<std::string, std::vector<std::int64_t>>& keys_;
};

std::vector<std::unique_ptr<Fixture>> g_fixtures;

Fixture& fixture_for(int recordings, bool multi)
{
    for (const auto& fixture : g_fixtures)
    {
        if (fixture->recordings() == recordings && fixture->multi() == multi)
        {
            return *fixture;
        }
    }
    std::fprintf(stderr, "no fixture for recordings=%d multi=%d\n", recordings, multi ? 1 : 0);
    std::exit(1);
}

std::string bench_name(const char* arm, int recordings, bool multi, const Sample& sample)
{
    return std::string{arm} + "/" + std::to_string(recordings) + (multi ? "/multi/" : "/same/") + sample.label;
}

double percentile(std::vector<double>& values, double fraction)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(fraction * static_cast<double>(values.size() - 1));
    return values[std::min(index, values.size() - 1)];
}

void bm_linear(benchmark::State& state, int recordings, bool multi, int sample_index)
{
    Fixture& fixture = fixture_for(recordings, multi);
    const Sample sample = samples(multi)[static_cast<std::size_t>(sample_index)];

    std::vector<double> micros;
    micros.reserve(state.max_iterations > 0 ? static_cast<std::size_t>(state.max_iterations) : 1024);
    for (auto _ : state)
    {
        const auto start = std::chrono::steady_clock::now();
        auto ids = linear(fixture.catalog(), 0, sample.stream_id, sample.fragment, sample.count);
        const auto end = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(ids.data());
        benchmark::ClobberMemory();
        micros.push_back(std::chrono::duration<double, std::micro>(end - start).count());
    }
    state.counters["p50_us"] = percentile(micros, 0.50);
    state.counters["p99_us"] = percentile(micros, 0.99);
    state.SetItemsProcessed(state.iterations());
}

void bm_index(benchmark::State& state, int recordings, bool multi, int sample_index)
{
    Fixture& fixture = fixture_for(recordings, multi);
    const Sample sample = samples(multi)[static_cast<std::size_t>(sample_index)];
    const UriIndex index{fixture.keys()};

    std::vector<double> micros;
    micros.reserve(state.max_iterations > 0 ? static_cast<std::size_t>(state.max_iterations) : 1024);
    for (auto _ : state)
    {
        const auto start = std::chrono::steady_clock::now();
        auto ids = index.collect(fixture.catalog(), 0, sample.stream_id, sample.fragment, sample.count);
        const auto end = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(ids.data());
        benchmark::ClobberMemory();
        micros.push_back(std::chrono::duration<double, std::micro>(end - start).count());
    }
    state.counters["p50_us"] = percentile(micros, 0.50);
    state.counters["p99_us"] = percentile(micros, 0.99);
    state.SetItemsProcessed(state.iterations());
}

}  // namespace

int main(int argc, char** argv)
{
    // Build every fixture before any timing, and prove the two arms agree
    // (A6e-1). A fixture set is 3 sizes × 2 shapes; the shapes share the size
    // scale, so the build is ~222k descriptors in total.
    for (const int recordings : {1000, 10000, 100000})
    {
        for (const bool multi : {true, false})
        {
            g_fixtures.push_back(std::make_unique<Fixture>(recordings, multi));
            const Fixture& fixture = *g_fixtures.back();
            const UriIndex index{fixture.keys()};
            for (const Sample& sample : samples(multi))
            {
                const std::vector<std::int64_t> expected =
                    linear(fixture.catalog(), 0, sample.stream_id, sample.fragment, sample.count);
                const std::vector<std::int64_t> actual =
                    index.collect(fixture.catalog(), 0, sample.stream_id, sample.fragment, sample.count);
                if (expected != actual)
                {
                    std::fprintf(stderr, "A6e-1 mismatch: recordings=%d multi=%d %s (linear=%zu index=%zu)\n",
                                 recordings, multi ? 1 : 0, sample.label.c_str(), expected.size(), actual.size());
                    return 1;
                }
            }
        }
    }

    for (const int recordings : {1000, 10000, 100000})
    {
        for (const bool multi : {true, false})
        {
            for (int sample_index = 0; sample_index < 2; sample_index++)
            {
                const std::string name =
                    bench_name("linear", recordings, multi, samples(multi)[static_cast<std::size_t>(sample_index)]);
                benchmark::RegisterBenchmark(name, bm_linear, recordings, multi, sample_index)
                    ->Unit(benchmark::kMicrosecond)
                    ->UseRealTime();
                benchmark::RegisterBenchmark(
                    bench_name("index", recordings, multi, samples(multi)[static_cast<std::size_t>(sample_index)]),
                    bm_index, recordings, multi, sample_index)
                    ->Unit(benchmark::kMicrosecond)
                    ->UseRealTime();
            }
        }
    }

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv))
    {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    g_fixtures.clear();
    return 0;
}
