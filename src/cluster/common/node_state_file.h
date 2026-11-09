#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"
#include "core/io/mapped_file.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

/// `node-state.dat`: node-specific persistent state that must not live in a
/// snapshot (the election safety parameter `candidateTermId`).
///
/// Transcribed from `NodeStateFile.java` at Aeron 1.54.0 (`454da1e4e6`).
/// Layout: a bare `NodeStateHeader` body (8 bytes), then standard framed
/// records, then `NodeStateFooter`; the file is at least 1 MiB. The record
/// header is the 16-byte node-state variant of the SBE message header
/// (`aeron-cluster-node-state-codecs.xml`).
class NodeStateFile
{
public:
    static constexpr std::string_view FILE_NAME = "node-state.dat";
    static constexpr std::size_t MINIMUM_FILE_LENGTH = 1U << 20;

    /// `NodeStateFile.CandidateTerm` view: values are read from the mapping on
    /// each call (`NodeStateFile.java:351-386`).
    struct CandidateTerm
    {
        std::int64_t candidate_term_id{0};
        std::int64_t timestamp{0};
        std::int64_t log_position{0};
    };

    /// Creates the file when it does not exist (`create_new`), otherwise opens
    /// and validates it instead (`NodeStateFile.java:97-147`).
    [[nodiscard]] static std::expected<NodeStateFile, Error> open(std::string_view cluster_dir, bool create_new,
                                                                  std::int32_t file_sync_level);

    NodeStateFile() = default;
    NodeStateFile(const NodeStateFile&) = delete;
    NodeStateFile& operator=(const NodeStateFile&) = delete;
    NodeStateFile(NodeStateFile&&) noexcept = default;
    NodeStateFile& operator=(NodeStateFile&&) noexcept = default;
    ~NodeStateFile() = default;

    /// Plain writes of `log_position` and `timestamp`, then a release store of
    /// `candidate_term_id`, then `sync` when `file_sync_level > 0`
    /// (`NodeStateFile.java:258-264`). The order is an election safety property.
    [[nodiscard]] std::expected<void, Error> update_candidate_term_id(std::int64_t candidate_term_id,
                                                                      std::int64_t log_position,
                                                                      std::int64_t timestamp_ms) noexcept;

    /// Updates only when `candidate_term_id` is greater (`NodeStateFile.java:274-285`).
    [[nodiscard]] std::expected<std::int64_t, Error> propose_max_candidate_term_id(std::int64_t candidate_term_id,
                                                                                   std::int64_t log_position,
                                                                                   std::int64_t timestamp_ms) noexcept;

    [[nodiscard]] CandidateTerm candidate_term() const noexcept;

    /// `NodeStateHeader.version()` (0.3.0 = 768).
    [[nodiscard]] std::int32_t semantic_version() const noexcept
    {
        return semantic_version_;
    }

private:
    [[nodiscard]] std::expected<void, Error> initialise();
    [[nodiscard]] std::expected<void, Error> load();
    [[nodiscard]] std::expected<void, Error> sync_file() noexcept;

    [[nodiscard]] std::int32_t find_message_type_offset(std::int32_t start_position, std::uint16_t template_id) const;

    axc::core::MappedFile mapped_{};
    std::int32_t file_sync_level_{0};
    std::int32_t semantic_version_{0};
    std::int32_t candidate_term_body_offset_{0};
    std::int32_t candidate_term_id_offset_{0};
};

}  // namespace deepmsg::cluster::common
