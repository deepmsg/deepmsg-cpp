#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace deepmsg::archive::server
{

/// Recording id to catalog-file-offset map, transcribed from
/// `CatalogIndex.java:23-224`.
///
/// Entries live in one flat array as `[recordingId, descriptorOffset, …]` with
/// ids strictly increasing, so lookup is the reference's interpolation search
/// and removal shifts everything after the hole to the left. The layout is not
/// observable on disk: the index is rebuilt from the catalog file on open
/// (`Catalog.buildIndex`, `Catalog.java:949`).
class CatalogIndex
{
public:
    static constexpr int DEFAULT_INDEX_SIZE = 10;
    static constexpr std::int64_t NULL_VALUE = -1;

    CatalogIndex() : index_(DEFAULT_INDEX_SIZE << 1, 0) {}

    /// `CatalogIndex.add` (`CatalogIndex.java:46`). Throws `std::invalid_argument`
    /// for a negative id/offset and for a non-increasing id, mirroring the
    /// reference's `IllegalArgumentException`.
    void add(std::int64_t recording_id, std::int64_t recording_descriptor_offset)
    {
        ensure_positive(recording_id, "recordingId");
        ensure_positive(recording_descriptor_offset, "recordingDescriptorOffset");

        const int next_position = count_ << 1;
        if (next_position > 0)
        {
            if (recording_id <= index_[static_cast<std::size_t>(next_position - 2)])
            {
                throw std::invalid_argument("recordingId " + std::to_string(recording_id) +
                                            " is less than or equal to the last recordingId " +
                                            std::to_string(index_[static_cast<std::size_t>(next_position - 2)]));
            }
            if (static_cast<std::size_t>(next_position) == index_.size())
            {
                index_ = expand(index_);
            }
        }

        index_[static_cast<std::size_t>(next_position)] = recording_id;
        index_[static_cast<std::size_t>(next_position) + 1] = recording_descriptor_offset;
        count_++;
    }

    /// `CatalogIndex.remove` (`CatalogIndex.java:78`). Returns the offset or
    /// `NULL_VALUE`.
    std::int64_t remove(std::int64_t recording_id)
    {
        ensure_positive(recording_id, "recordingId");

        const int last = last_position();
        const int position = find(index_, recording_id, last);
        if (position < 0)
        {
            return NULL_VALUE;
        }

        const std::int64_t recording_descriptor_offset = index_[static_cast<std::size_t>(position) + 1];
        count_--;

        for (int slot = position; slot < last; slot += 2)
        {
            index_[static_cast<std::size_t>(slot)] = index_[static_cast<std::size_t>(slot) + 2];
            index_[static_cast<std::size_t>(slot) + 1] = index_[static_cast<std::size_t>(slot) + 3];
        }
        index_[static_cast<std::size_t>(last)] = 0;
        index_[static_cast<std::size_t>(last) + 1] = 0;

        return recording_descriptor_offset;
    }

    /// `CatalogIndex.recordingOffset` (`CatalogIndex.java:113`).
    [[nodiscard]] std::int64_t recording_offset(std::int64_t recording_id) const
    {
        ensure_positive(recording_id, "recordingId");

        const int position = find(index_, recording_id, last_position());
        if (position < 0)
        {
            return NULL_VALUE;
        }
        return index_[static_cast<std::size_t>(position) + 1];
    }

    [[nodiscard]] int size() const noexcept
    {
        return count_;
    }

    /// `CatalogIndex.lastPosition` (`CatalogIndex.java:143`): index of the last
    /// id slot, negative when empty.
    [[nodiscard]] int last_position() const noexcept
    {
        return (count_ << 1) - 2;
    }

    /// Flat `[recordingId, offset, …]` pairs (`CatalogIndex.index()`).
    [[nodiscard]] std::span<const std::int64_t> index() const noexcept
    {
        return index_;
    }

    /// The same pairs, writable: `Catalog.replaceRecording` fixes up the offsets
    /// of every entry after the replaced one in place (`Catalog.java:1029`).
    [[nodiscard]] std::span<std::int64_t> mutable_index() noexcept
    {
        return index_;
    }

    /// `CatalogIndex.find` (`CatalogIndex.java:158`): interpolation search that
    /// returns the slot of `recording_id` or -1.
    [[nodiscard]] static int find(std::span<const std::int64_t> index, std::int64_t recording_id,
                                  int last_position) noexcept
    {
        if (last_position > 0 && recording_id >= index[0] &&
            recording_id <= index[static_cast<std::size_t>(last_position)])
        {
            const std::int64_t first = index[0];
            const std::int64_t last = index[static_cast<std::size_t>(last_position)];
            int position = static_cast<int>((recording_id - first) * last_position / (last - first));
            position = 0 == (position & 1) ? position : position + 1;

            if (recording_id == index[static_cast<std::size_t>(position)])
            {
                return position;
            }
            if (recording_id > index[static_cast<std::size_t>(position)])
            {
                for (int slot = position + 2; slot <= last_position; slot += 2)
                {
                    const std::int64_t id = index[static_cast<std::size_t>(slot)];
                    if (recording_id == id)
                    {
                        return slot;
                    }
                    if (id > recording_id)
                    {
                        break;
                    }
                }
            }
            else
            {
                for (int slot = position - 2; slot >= 0; slot -= 2)
                {
                    const std::int64_t id = index[static_cast<std::size_t>(slot)];
                    if (recording_id == id)
                    {
                        return slot;
                    }
                    if (id < recording_id)
                    {
                        break;
                    }
                }
            }
        }
        else if (0 == last_position && recording_id == index[0])
        {
            return 0;
        }

        return -1;
    }

private:
    /// `CatalogIndex.expand` (`CatalogIndex.java:208`): one and a half times the
    /// entry count, rounded to pairs.
    static std::vector<std::int64_t> expand(const std::vector<std::int64_t>& index)
    {
        const auto length = static_cast<int>(index.size());
        const int entries = length >> 1;
        const auto new_length = static_cast<std::size_t>((entries + (entries >> 1)) << 1);

        std::vector<std::int64_t> expanded(new_length, 0);
        for (std::size_t slot = 0; slot < index.size(); slot++)
        {
            expanded[slot] = index[slot];
        }
        return expanded;
    }

    static void ensure_positive(std::int64_t value, const char* name)
    {
        if (value < 0)
        {
            throw std::invalid_argument(std::string(name) + " cannot be negative: value=" + std::to_string(value));
        }
    }

    std::vector<std::int64_t> index_;
    int count_{0};
};

}  // namespace deepmsg::archive::server
