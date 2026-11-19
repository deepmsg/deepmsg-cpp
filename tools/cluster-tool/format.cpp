#include "format.h"

#include <chrono>
#include <cstdio>
#include <ctime>

#include "cluster/common/cluster_mark_file.h"
#include "io_aeron_cluster_codecs_mark/MarkFileHeader.h"

namespace deepmsg::tools
{

namespace mark = ::io::aeron::cluster::codecs::mark;
using cluster::common::ClusterMarkFile;
using cluster::common::RecordingLog;

std::int64_t now_ms() noexcept
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

namespace
{

std::tm local_civil(std::int64_t epoch_ms) noexcept
{
    // Java `Date` floors a negative millisecond stamp to the second; C++
    // integer division truncates toward zero, so floor it here.
    const std::int64_t seconds_value = epoch_ms >= 0 ? epoch_ms / 1000 : -((-epoch_ms + 999) / 1000);
    const std::time_t seconds = static_cast<std::time_t>(seconds_value);
    std::tm local{};
    (void)::localtime_r(&seconds, &local);
    return local;
}

void append(std::string& out, std::int64_t value)
{
    out += std::to_string(value);
}

void append(std::string& out, std::int32_t value)
{
    out += std::to_string(value);
}

}  // namespace

std::string time_of_day(std::int64_t epoch_ms)
{
    const std::tm local = local_civil(epoch_ms);
    char text[16];
    std::strftime(text, sizeof(text), "%H:%M:%S", &local);
    return text;
}

std::string date_time(std::int64_t epoch_ms)
{
    const std::tm local = local_civil(epoch_ms);
    char text[32];
    std::strftime(text, sizeof(text), "%Y-%m-%d %H:%M:%S", &local);
    return text;
}

std::string date_only(std::int64_t epoch_ms)
{
    const std::tm local = local_civil(epoch_ms);
    char text[16];
    std::strftime(text, sizeof(text), "%Y-%m-%d", &local);
    return text;
}

std::string mark_header_line(const ClusterMarkFile& mark_file)
{
    std::string out = "[MarkFileHeader](sbeTemplateId=";
    append(out, static_cast<std::int32_t>(mark::MarkFileHeader::SBE_TEMPLATE_ID));
    out += "|sbeSchemaId=";
    append(out, static_cast<std::int32_t>(mark::MarkFileHeader::SBE_SCHEMA_ID));
    out += "|sbeSchemaVersion=";
    append(out, static_cast<std::int32_t>(mark::MarkFileHeader::SBE_SCHEMA_VERSION));
    out += "|sbeBlockLength=";
    append(out, static_cast<std::int32_t>(mark::MarkFileHeader::SBE_BLOCK_LENGTH));
    out += "):version=";
    append(out, mark_file.semantic_version());
    out += "|componentType=";
    out += cluster::common::cluster_component_type_name(mark_file.component_type());
    out += "|activityTimestamp=";
    append(out, mark_file.activity_timestamp_volatile());
    out += "|startTimestamp=";
    append(out, mark_file.start_timestamp());
    out += "|pid=";
    append(out, mark_file.pid());
    out += "|candidateTermId=";
    append(out, mark_file.candidate_term_id());
    out += "|archiveStreamId=";
    append(out, mark_file.archive_stream_id());
    out += "|serviceStreamId=";
    append(out, mark_file.service_stream_id());
    out += "|consensusModuleStreamId=";
    append(out, mark_file.consensus_module_stream_id());
    out += "|ingressStreamId=";
    append(out, mark_file.ingress_stream_id());
    out += "|memberId=";
    append(out, mark_file.member_id());
    out += "|serviceId=";
    append(out, mark_file.service_id());
    out += "|headerLength=";
    append(out, mark_file.header_length());
    out += "|errorBufferLength=";
    append(out, mark_file.error_buffer_length());
    out += "|clusterId=";
    append(out, mark_file.cluster_id());
    out += "|aeronDirectory='";
    out += mark_file.aeron_directory();
    out += "'|controlChannel='";
    out += mark_file.control_channel();
    out += "'|ingressChannel='";
    out += mark_file.ingress_channel();
    out += "'|serviceName='";
    out += mark_file.service_name();
    out += "'|authenticator='";
    out += mark_file.authenticator();
    out += "'|servicesClusterDir='";
    out += mark_file.services_cluster_dir();
    out += "'";
    return out;
}

std::string recording_log_text(const RecordingLog& log)
{
    std::string out = "RecordingLog{entries=[";
    const auto& entries = log.entries();
    for (std::size_t i = 0; i < entries.size(); ++i)
    {
        if (0 != i)
        {
            out += ", ";
        }
        out += entries[i].to_string();
    }
    out += "], cacheIndex={";
    bool first = true;
    for (std::size_t i = 0; i < entries.size(); ++i)
    {
        if (!entries[i].is_term())
        {
            continue;
        }
        if (!first)
        {
            out += ", ";
        }
        first = false;
        out += std::to_string(entries[i].leadership_term_id);
        out += "=";
        out += std::to_string(i);
    }
    out += "}}";
    return out;
}

std::string recovery_plan_text(const RecordingLog::RecoveryPlan& plan)
{
    std::string out = "RecoveryPlan[lastLeadershipTermId=";
    append(out, plan.last_leadership_term_id);
    out += ", lastTermBaseLogPosition=";
    append(out, plan.last_term_base_log_position);
    out += ", appendedLogPosition=";
    append(out, plan.appended_log_position);
    out += ", committedLogPosition=";
    append(out, plan.committed_log_position);
    out += ", snapshots=[";
    for (std::size_t i = 0; i < plan.snapshots.size(); ++i)
    {
        const auto& snapshot = plan.snapshots[i];
        if (0 != i)
        {
            out += ", ";
        }
        out += "Snapshot[recordingId=";
        append(out, snapshot.recording_id);
        out += ", leadershipTermId=";
        append(out, snapshot.leadership_term_id);
        out += ", termBaseLogPosition=";
        append(out, snapshot.term_base_log_position);
        out += ", logPosition=";
        append(out, snapshot.log_position);
        out += ", timestamp=";
        append(out, snapshot.timestamp);
        out += ", serviceId=";
        append(out, snapshot.service_id);
        out += "]";
    }
    out += "], log=";
    if (!plan.log.has_value())
    {
        out += "null";
    }
    else
    {
        const auto& log = *plan.log;
        out += "Log[recordingId=";
        append(out, log.recording_id);
        out += ", leadershipTermId=";
        append(out, log.leadership_term_id);
        out += ", termBaseLogPosition=";
        append(out, log.term_base_log_position);
        out += ", logPosition=";
        append(out, log.log_position);
        out += ", startPosition=";
        append(out, log.start_position);
        out += ", stopPosition=";
        append(out, log.stop_position);
        out += ", initialTermId=";
        append(out, log.initial_term_id);
        out += ", termBufferLength=";
        append(out, log.term_buffer_length);
        out += ", mtuLength=";
        append(out, log.mtu_length);
        out += ", sessionId=";
        append(out, log.session_id);
        out += "]";
    }
    out += "]";
    return out;
}

namespace
{

/// `new String(bytes, UTF_8)`'s decoding with the JDK's replacement character
/// for malformed input (`ClusterToolOperator`'s printer does exactly this).
std::string decode_utf8_replace(std::span<const std::uint8_t> bytes)
{
    std::string out;
    std::size_t i = 0;
    while (i < bytes.size())
    {
        const std::uint8_t first = bytes[i];
        if (first < 0x80)
        {
            out += static_cast<char>(first);
            ++i;
            continue;
        }

        std::size_t required = 0;
        if (first >= 0xC2 && first <= 0xDF)
        {
            required = 1;
        }
        else if (first >= 0xE0 && first <= 0xEF)
        {
            required = 2;
        }
        else if (first >= 0xF0 && first <= 0xF4)
        {
            required = 3;
        }
        else
        {
            out += "\xEF\xBF\xBD";
            ++i;
            continue;
        }

        std::size_t available = 0;
        while (available < required && i + available + 1 < bytes.size() && (bytes[i + available + 1] & 0xC0) == 0x80)
        {
            ++available;
        }
        if (available == required)
        {
            out.append(reinterpret_cast<const char*>(bytes.data() + i), required + 1);
            i += required + 1;
        }
        else
        {
            out += "\xEF\xBF\xBD";
            i += available + 1;
        }
    }
    return out;
}

}  // namespace

std::string hex_dump(std::span<const std::uint8_t> bytes)
{
    constexpr std::size_t WIDTH = 16;
    std::string out;
    const std::size_t length = bytes.size();

    for (std::size_t row = 0; row < length; row += WIDTH)
    {
        char offset_text[32];
        std::snprintf(offset_text, sizeof(offset_text), "%06zu:  ", row);
        out += offset_text;

        for (std::size_t index = 0; index < WIDTH; ++index)
        {
            if (row + index < length)
            {
                char byte_text[4];
                std::snprintf(byte_text, sizeof(byte_text), "%02x ", static_cast<unsigned>(bytes[row + index]));
                out += byte_text;
            }
            else
            {
                out += "   ";
            }
        }

        out += "  |  ";
        const std::size_t ascii_width = length - row < WIDTH ? length - row : WIDTH;
        std::string decoded = decode_utf8_replace(bytes.subspan(row, ascii_width));
        // `replaceAll("\r\n", " ").replaceAll("\n", " ")`: a CRLF pair is one
        // space, a lone LF is a space, and a lone CR is left alone.
        std::string ascii;
        for (std::size_t index = 0; index < decoded.size(); ++index)
        {
            if ('\r' == decoded[index] && index + 1 < decoded.size() && '\n' == decoded[index + 1])
            {
                ascii += ' ';
                ++index;
            }
            else if ('\n' == decoded[index])
            {
                ascii += ' ';
            }
            else
            {
                ascii += decoded[index];
            }
        }
        out += ascii;
        out += "\n";
    }
    return out;
}

}  // namespace deepmsg::tools
