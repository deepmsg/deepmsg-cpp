// Golden criterion for C1c: the snapshot order and expiry order of both timer
// implementations, produced by the reference Java code.
//
// `test/golden/cluster/timer/*.txt` was written by `TimerSnapshotGoldenGen`
// (reference Aeron 1.54.0). Regenerate, never edit by hand.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cluster/common/timer/priority_heap_timer_service.h"
#include "cluster/common/timer/wheel_timer_service.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::timer::ClusterTimeUnit;
using deepmsg::cluster::common::timer::PriorityHeapTimerService;
using deepmsg::cluster::common::timer::TimerHandler;
using deepmsg::cluster::common::timer::TimerService;
using deepmsg::cluster::common::timer::TimerSnapshotTaker;
using deepmsg::cluster::common::timer::WheelTimerService;

using Pairs = std::vector<std::pair<std::int64_t, std::int64_t>>;

class Recorder final : public TimerSnapshotTaker
{
public:
    void snapshot_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        entries.emplace_back(correlation_id, deadline);
    }

    Pairs entries;
};

class IdRecorder final : public TimerHandler
{
public:
    explicit IdRecorder(bool result) : result_(result) {}

    bool on_timer_event(std::int64_t correlation_id) override
    {
        ids.push_back(correlation_id);
        return result_;
    }

    std::vector<std::int64_t> ids;

private:
    bool result_;
};

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/cluster/timer";
}

[[nodiscard]] std::string read_all(const std::string& name)
{
    std::ifstream input{golden_dir() + "/" + name};
    std::string value;
    std::getline(input, value);
    return value;
}

[[nodiscard]] std::optional<std::string> value_after(const std::string& line, const std::string& key)
{
    const std::string prefix = key + "=";
    if (0 != line.rfind(prefix, 0))
    {
        return std::nullopt;
    }
    return line.substr(prefix.size());
}

/// `(1,10),(2,14)` -> pairs.
[[nodiscard]] Pairs parse_pairs(const std::string& text)
{
    Pairs pairs;
    std::size_t position = 0;

    while (position < text.size())
    {
        const std::size_t open = text.find('(', position);
        if (std::string::npos == open)
        {
            break;
        }
        const std::size_t comma = text.find(',', open);
        const std::size_t close = text.find(')', comma);
        if (std::string::npos == comma || std::string::npos == close)
        {
            break;
        }

        pairs.emplace_back(std::stoll(text.substr(open + 1, comma - open - 1)),
                           std::stoll(text.substr(comma + 1, close - comma - 1)));
        position = close + 1;
    }

    return pairs;
}

/// `[1, 2, 3]` -> ids.
[[nodiscard]] std::vector<std::int64_t> parse_list(const std::string& text)
{
    std::vector<std::int64_t> ids;
    std::size_t position = 0;

    while (true)
    {
        const std::size_t begin = text.find_first_of("0123456789", position);
        if (std::string::npos == begin)
        {
            break;
        }
        const std::size_t end = text.find_first_not_of("0123456789", begin);
        ids.push_back(std::stoll(text.substr(begin, std::string::npos == end ? end : end - begin)));
        if (std::string::npos == end)
        {
            break;
        }
        position = end;
    }

    return ids;
}

[[nodiscard]] std::unique_ptr<TimerService> wheel()
{
    return std::make_unique<WheelTimerService>(ClusterTimeUnit::millis, 0, 8, 128);
}

[[nodiscard]] std::unique_ptr<TimerService> heap()
{
    return std::make_unique<PriorityHeapTimerService>();
}

TEST(TimerGoldenTest, wheelAndHeapFiveTimerSnapshots)
{
    const std::string line = read_all("five.txt");
    const auto wheel_text = value_after(line, "wheel");
    ASSERT_TRUE(wheel_text.has_value()) << line;

    // The two lines share the file; read them separately.
    std::ifstream input{golden_dir() + "/five.txt"};
    std::string wheel_line;
    std::string heap_line;
    std::getline(input, wheel_line);
    std::getline(input, heap_line);

    const long timers[][2] = {{1, 10}, {2, 14}, {3, 30}, {4, 29}, {5, 15}};

    auto wheel_service = wheel();
    auto heap_service = heap();
    for (const auto& timer : timers)
    {
        wheel_service->schedule_timer_for_correlation_id(timer[0], timer[1]);
        heap_service->schedule_timer_for_correlation_id(timer[0], timer[1]);
    }

    Recorder wheel_recorder;
    wheel_service->snapshot(wheel_recorder);
    Recorder heap_recorder;
    heap_service->snapshot(heap_recorder);

    EXPECT_EQ(parse_pairs(value_after(wheel_line, "wheel").value_or("")), wheel_recorder.entries);
    EXPECT_EQ(parse_pairs(value_after(heap_line, "heap").value_or("")), heap_recorder.entries);
    EXPECT_NE(wheel_recorder.entries, heap_recorder.entries);
}

TEST(TimerGoldenTest, heapSnapshotAfterPoll)
{
    std::ifstream input{golden_dir() + "/heap-after-poll.txt"};
    std::string expired_line;
    std::string snapshot_line;
    std::getline(input, expired_line);
    std::getline(input, snapshot_line);

    auto service = heap();
    service->schedule_timer_for_correlation_id(1, 10);
    service->schedule_timer_for_correlation_id(2, 14);
    service->schedule_timer_for_correlation_id(3, 30);
    service->schedule_timer_for_correlation_id(4, 29);
    service->schedule_timer_for_correlation_id(5, 15);

    IdRecorder handler(true);
    EXPECT_EQ(std::stoi(value_after(expired_line, "expired").value_or("-1")), service->poll(14, handler));

    Recorder recorder;
    service->snapshot(recorder);
    EXPECT_EQ(parse_pairs(value_after(snapshot_line, "snapshot").value_or("")), recorder.entries);
}

TEST(TimerGoldenTest, manyTimerSnapshots)
{
    std::ifstream input{golden_dir() + "/many.txt"};
    std::string wheel_line;
    std::string heap_line;
    std::getline(input, wheel_line);
    std::getline(input, heap_line);

    auto wheel_service = wheel();
    auto heap_service = heap();

    for (std::int64_t i = 0; i < 40; ++i)
    {
        const std::int64_t correlation_id = 100 + i;
        const std::int64_t deadline = (i * 7) % 53;
        wheel_service->schedule_timer_for_correlation_id(correlation_id, deadline);
        heap_service->schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    for (const std::int64_t correlation_id : {101, 107, 113, 131})
    {
        EXPECT_TRUE(wheel_service->cancel_timer_by_correlation_id(correlation_id));
        EXPECT_TRUE(heap_service->cancel_timer_by_correlation_id(correlation_id));
    }

    wheel_service->schedule_timer_for_correlation_id(105, 3);
    wheel_service->schedule_timer_for_correlation_id(129, 61);
    heap_service->schedule_timer_for_correlation_id(105, 3);
    heap_service->schedule_timer_for_correlation_id(129, 61);

    Recorder wheel_recorder;
    wheel_service->snapshot(wheel_recorder);
    Recorder heap_recorder;
    heap_service->snapshot(heap_recorder);

    EXPECT_EQ(parse_pairs(value_after(wheel_line, "wheel").value_or("")), wheel_recorder.entries);
    EXPECT_EQ(parse_pairs(value_after(heap_line, "heap").value_or("")), heap_recorder.entries);
}

TEST(TimerGoldenTest, wheelExpiryOrder)
{
    std::ifstream input{golden_dir() + "/wheel-expiry.txt"};
    std::string count_line;
    std::string expired_line;
    std::getline(input, count_line);
    std::getline(input, expired_line);

    IdRecorder handler(true);
    auto service = std::make_unique<WheelTimerService>(ClusterTimeUnit::millis, 0, 8, 128);
    for (std::int64_t i = 1; i <= 12; ++i)
    {
        service->schedule_timer_for_correlation_id(i, i * 5);
    }

    EXPECT_EQ(std::stoi(value_after(count_line, "count").value_or("-1")), service->poll(60, handler));
    EXPECT_EQ(parse_list(value_after(expired_line, "expired").value_or("")), handler.ids);
}

TEST(TimerGoldenTest, wheelTickBoundary)
{
    std::ifstream input{golden_dir() + "/wheel-tick.txt"};
    std::string first_line;
    std::string second_line;
    std::getline(input, first_line);
    std::getline(input, second_line);

    const auto split = [](const std::string& line, const std::string& key)
    {
        const std::string prefix = key + "=";
        const std::size_t begin = line.find(prefix);
        const std::size_t colon = line.find(':', begin);
        return std::pair<std::int64_t, std::string>{
            std::stoll(line.substr(begin + prefix.size(), colon - begin - prefix.size())), line.substr(colon + 1)};
    };

    IdRecorder handler(true);
    auto service = std::make_unique<WheelTimerService>(ClusterTimeUnit::millis, 0, 8, 128);
    service->schedule_timer_for_correlation_id(1, 20);
    service->schedule_timer_for_correlation_id(2, 9);

    const auto first = split(first_line, "first");
    EXPECT_EQ(first.first, service->poll(10, handler));
    EXPECT_EQ(parse_list(first.second), handler.ids);

    handler.ids.clear();
    const auto second = split(second_line, "second");
    EXPECT_EQ(second.first, service->poll(25, handler));
    EXPECT_EQ(parse_list(second.second), handler.ids);
}

TEST(TimerGoldenTest, heapExpiryOrder)
{
    std::ifstream input{golden_dir() + "/heap-expiry.txt"};
    std::string count_line;
    std::string expired_line;
    std::getline(input, count_line);
    std::getline(input, expired_line);

    IdRecorder handler(true);
    auto service = heap();
    service->schedule_timer_for_correlation_id(1, 10);
    service->schedule_timer_for_correlation_id(4, 3);
    service->schedule_timer_for_correlation_id(2, 0);
    service->schedule_timer_for_correlation_id(3, 3);
    service->schedule_timer_for_correlation_id(5, 0);
    service->schedule_timer_for_correlation_id(6, 11);

    EXPECT_EQ(std::stoi(value_after(count_line, "count").value_or("-1")), service->poll(10, handler));
    EXPECT_EQ(parse_list(value_after(expired_line, "expired").value_or("")), handler.ids);
}

}  // namespace
