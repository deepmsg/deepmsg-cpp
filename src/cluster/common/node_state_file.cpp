#include "cluster/common/node_state_file.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

#include "cluster/common/cluster_versions.h"
#include "core/io/fs.h"
#include "io_aeron_cluster_codecs_node/CandidateTerm.h"
#include "io_aeron_cluster_codecs_node/MessageHeader.h"
#include "io_aeron_cluster_codecs_node/NodeStateFooter.h"
#include "io_aeron_cluster_codecs_node/NodeStateHeader.h"

namespace deepmsg::cluster::common
{

using axc::core::ErrorCode;
using axc::core::make_error;

namespace
{

namespace node = ::io::aeron::cluster::codecs::node;

constexpr std::int32_t NULL_OFFSET = -1;
constexpr std::int64_t NULL_VALUE = -1;
constexpr std::size_t RECORD_ALIGNMENT = 8;  // UnsafeBuffer.ALIGNMENT

[[nodiscard]] constexpr std::size_t align(std::size_t value, std::size_t alignment) noexcept
{
    return (value + (alignment - 1)) & ~(alignment - 1);
}

[[nodiscard]] std::string join_path(std::string_view dir, std::string_view name)
{
    std::string path{dir};
    if (!path.empty() && '/' != path.back())
    {
        path.push_back('/');
    }
    path.append(name);
    return path;
}

[[nodiscard]] std::int64_t get_le64(const std::uint8_t* data) noexcept
{
    std::uint64_t value = 0;
    for (int i = 7; i >= 0; --i)
    {
        value = (value << 8) | data[i];
    }
    return static_cast<std::int64_t>(value);
}

void put_le64(std::uint8_t* data, std::int64_t value) noexcept
{
    const auto value_u = static_cast<std::uint64_t>(value);
    for (int i = 0; i < 8; ++i)
    {
        data[i] = static_cast<std::uint8_t>(value_u >> (8 * i));
    }
}

}  // namespace

std::expected<NodeStateFile, Error> NodeStateFile::open(std::string_view cluster_dir, bool create_new,
                                                        std::int32_t file_sync_level)
{
    const std::string path = join_path(cluster_dir, FILE_NAME);
    NodeStateFile node_state;
    node_state.file_sync_level_ = file_sync_level;

    if (!axc::core::file_exists(path))
    {
        if (!create_new)
        {
            return std::unexpected(make_error(ErrorCode::io_error, "NodeStateFile does not exist and createNew=false"));
        }

        auto mapped = axc::core::MappedFile::create(path, MINIMUM_FILE_LENGTH);
        if (!mapped)
        {
            return std::unexpected(mapped.error());
        }
        node_state.mapped_ = std::move(*mapped);

        try
        {
            auto initialised = node_state.initialise();
            if (!initialised)
            {
                return std::unexpected(initialised.error());
            }
        }
        catch (const std::exception& ex)
        {
            return std::unexpected(make_error(ErrorCode::internal_error, ex.what()));
        }
    }
    else
    {
        auto mapped = axc::core::MappedFile::open(path, true);
        if (!mapped)
        {
            return std::unexpected(mapped.error());
        }
        node_state.mapped_ = std::move(*mapped);

        try
        {
            auto loaded = node_state.load();
            if (!loaded)
            {
                return std::unexpected(loaded.error());
            }
        }
        catch (const std::exception& ex)
        {
            return std::unexpected(make_error(ErrorCode::internal_error, ex.what()));
        }
    }

    auto synced = node_state.sync_file();
    if (!synced)
    {
        return std::unexpected(synced.error());
    }

    return node_state;
}

std::expected<void, Error> NodeStateFile::initialise()
{
    auto mapping = mapped_.mapping();
    auto* data = reinterpret_cast<char*>(mapping.data());
    const auto length = static_cast<std::uint64_t>(mapping.size());

    node::NodeStateHeader header;
    header.wrapForEncode(data, 0, length);
    header.version(CLUSTER_MARK_SEMANTIC_VERSION);

    const auto candidate_term_frame_offset = static_cast<std::int32_t>(node::NodeStateHeader::SBE_BLOCK_LENGTH);
    if (0 != (candidate_term_frame_offset & static_cast<std::int32_t>(RECORD_ALIGNMENT - 1)))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "candidate term frame is not 8-byte aligned"));
    }

    node::CandidateTerm candidate_term;
    candidate_term.wrapAndApplyHeader(data, static_cast<std::uint64_t>(candidate_term_frame_offset), length);
    candidate_term.logPosition(NULL_VALUE).timestamp(NULL_VALUE).candidateTermId(NULL_VALUE);

    node::MessageHeader candidate_term_header(data, static_cast<std::uint64_t>(candidate_term_frame_offset), length,
                                              node::MessageHeader::sbeSchemaVersion());
    candidate_term_header.frameLength(
        static_cast<std::int32_t>(node::MessageHeader::encodedLength() + node::CandidateTerm::SBE_BLOCK_LENGTH));

    const auto footer_offset = static_cast<std::int32_t>(
        static_cast<std::size_t>(candidate_term_frame_offset) +
        align(static_cast<std::size_t>(candidate_term_header.frameLength()), RECORD_ALIGNMENT));

    node::NodeStateFooter footer;
    footer.wrapAndApplyHeader(data, static_cast<std::uint64_t>(footer_offset), length);

    node::MessageHeader footer_header(data, static_cast<std::uint64_t>(footer_offset), length,
                                      node::MessageHeader::sbeSchemaVersion());
    footer_header.frameLength(
        static_cast<std::int32_t>(node::MessageHeader::encodedLength() + node::NodeStateFooter::SBE_BLOCK_LENGTH));

    semantic_version_ = CLUSTER_MARK_SEMANTIC_VERSION;
    candidate_term_body_offset_ =
        candidate_term_frame_offset + static_cast<std::int32_t>(node::MessageHeader::encodedLength());
    candidate_term_id_offset_ =
        candidate_term_body_offset_ + static_cast<std::int32_t>(node::CandidateTerm::candidateTermIdEncodingOffset());

    if (0 != (candidate_term_id_offset_ & static_cast<std::int32_t>(RECORD_ALIGNMENT - 1)))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "candidateTermId is not 8-byte aligned"));
    }

    // `NodeStateFile.java:124` writes the initial value again through a volatile store.
    auto& candidate_term_id = *reinterpret_cast<std::int64_t*>(mapping.data() + candidate_term_id_offset_);
    std::atomic_ref<std::int64_t>{candidate_term_id}.store(NULL_VALUE, std::memory_order_release);

    return {};
}

std::expected<void, Error> NodeStateFile::load()
{
    auto mapping = mapped_.mapping();
    auto* data = reinterpret_cast<char*>(mapping.data());
    const auto length = static_cast<std::uint64_t>(mapping.size());

    node::NodeStateHeader header;
    header.wrapForDecode(data, 0, node::NodeStateHeader::SBE_BLOCK_LENGTH, node::NodeStateHeader::SBE_SCHEMA_VERSION,
                         length);

    semantic_version_ = header.version();
    if (semantic_major(semantic_version_) != CLUSTER_MARK_MAJOR_VERSION)
    {
        return std::unexpected(make_error(
            ErrorCode::protocol_error, "mark file major version " + std::to_string(semantic_major(semantic_version_)) +
                                           " does not match software: " + std::to_string(CLUSTER_MARK_MAJOR_VERSION)));
    }

    const auto start_position = static_cast<std::int32_t>(node::NodeStateHeader::SBE_BLOCK_LENGTH);

    const std::int32_t footer_offset = find_message_type_offset(start_position, node::NodeStateFooter::SBE_TEMPLATE_ID);
    if (NULL_OFFSET == footer_offset)
    {
        return std::unexpected(
            make_error(ErrorCode::protocol_error, "failed to find NodeStateFooter entry, file corrupt?"));
    }

    const std::int32_t candidate_term_offset =
        find_message_type_offset(start_position, node::CandidateTerm::SBE_TEMPLATE_ID);
    if (NULL_OFFSET == candidate_term_offset)
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "failed to find CandidateTerm entry"));
    }

    candidate_term_body_offset_ =
        candidate_term_offset + static_cast<std::int32_t>(node::MessageHeader::encodedLength());
    candidate_term_id_offset_ =
        candidate_term_body_offset_ + static_cast<std::int32_t>(node::CandidateTerm::candidateTermIdEncodingOffset());

    return {};
}

std::int32_t NodeStateFile::find_message_type_offset(std::int32_t start_position, std::uint16_t template_id) const
{
    auto mapping = mapped_.mapping();
    auto* data = reinterpret_cast<char*>(const_cast<std::uint8_t*>(mapping.data()));
    const auto length = static_cast<std::uint64_t>(mapping.size());

    if (0 != (start_position & static_cast<std::int32_t>(RECORD_ALIGNMENT - 1)))
    {
        throw std::runtime_error("start position is not correctly aligned");
    }

    std::int32_t position = start_position;

    while (static_cast<std::uint64_t>(position) < length)
    {
        node::MessageHeader header(data, static_cast<std::uint64_t>(position), length,
                                   node::MessageHeader::sbeSchemaVersion());

        const std::int32_t message_length = header.frameLength();

        if (template_id == header.templateId())
        {
            return position;
        }
        if (node::NodeStateFooter::SBE_TEMPLATE_ID == header.templateId())
        {
            return NULL_OFFSET;
        }

        if (message_length < 0)
        {
            throw std::runtime_error("Message length < 0, file corrupt?");
        }
        if (0 == message_length)
        {
            return NULL_OFFSET;
        }

        position += static_cast<std::int32_t>(align(static_cast<std::size_t>(message_length), RECORD_ALIGNMENT));
    }

    return NULL_OFFSET;
}

std::expected<void, Error> NodeStateFile::update_candidate_term_id(std::int64_t candidate_term_id,
                                                                   std::int64_t log_position,
                                                                   std::int64_t timestamp_ms) noexcept
{
    auto mapping = mapped_.mapping();
    auto* data = mapping.data();

    put_le64(data + candidate_term_body_offset_ + node::CandidateTerm::logPositionEncodingOffset(), log_position);
    put_le64(data + candidate_term_body_offset_ + node::CandidateTerm::timestampEncodingOffset(), timestamp_ms);

    auto& candidate_term_id_ref = *reinterpret_cast<std::int64_t*>(data + candidate_term_id_offset_);
    std::atomic_ref<std::int64_t>{candidate_term_id_ref}.store(candidate_term_id, std::memory_order_release);

    return sync_file();
}

std::expected<std::int64_t, Error> NodeStateFile::propose_max_candidate_term_id(std::int64_t candidate_term_id,
                                                                                std::int64_t log_position,
                                                                                std::int64_t timestamp_ms) noexcept
{
    const std::int64_t existing_candidate_term_id = candidate_term().candidate_term_id;

    if (candidate_term_id > existing_candidate_term_id)
    {
        auto updated = update_candidate_term_id(candidate_term_id, log_position, timestamp_ms);
        if (!updated)
        {
            return std::unexpected(updated.error());
        }
        return candidate_term_id;
    }

    return existing_candidate_term_id;
}

NodeStateFile::CandidateTerm NodeStateFile::candidate_term() const noexcept
{
    auto mapping = mapped_.mapping();
    const std::uint8_t* data = mapping.data();

    CandidateTerm candidate_term;
    candidate_term.log_position =
        get_le64(data + candidate_term_body_offset_ + node::CandidateTerm::logPositionEncodingOffset());
    candidate_term.timestamp =
        get_le64(data + candidate_term_body_offset_ + node::CandidateTerm::timestampEncodingOffset());

    auto& candidate_term_id_ref =
        *reinterpret_cast<std::int64_t*>(const_cast<std::uint8_t*>(data) + candidate_term_id_offset_);
    candidate_term.candidate_term_id =
        std::atomic_ref<std::int64_t>{candidate_term_id_ref}.load(std::memory_order_acquire);

    return candidate_term;
}

std::expected<void, Error> NodeStateFile::sync_file() noexcept
{
    if (file_sync_level_ > 0)
    {
        return mapped_.flush();
    }

    return {};
}

}  // namespace deepmsg::cluster::common
