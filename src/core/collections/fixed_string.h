#pragma once

#include <array>
#include <cstddef>
#include <string_view>

namespace axc::core
{

/// Fixed-capacity, heap-free string. Assignment truncates; it never allocates
/// and never throws.
template <std::size_t Capacity>
class FixedString
{
public:
    static_assert(Capacity > 0, "FixedString needs room for the terminator");

    constexpr FixedString() noexcept = default;

    FixedString(const char* text) noexcept
    {
        assign(text);
    }

    FixedString(std::string_view text) noexcept
    {
        assign(text);
    }

    FixedString& operator=(const char* text) noexcept
    {
        assign(text);
        return *this;
    }

    FixedString& operator=(std::string_view text) noexcept
    {
        assign(text);
        return *this;
    }

    void assign(const char* text) noexcept
    {
        assign(nullptr == text ? std::string_view{} : std::string_view{text});
    }

    void assign(std::string_view text) noexcept
    {
        const std::size_t length = text.size() < (Capacity - 1) ? text.size() : (Capacity - 1);
        for (std::size_t i = 0; i < length; ++i)
        {
            buffer_[i] = text[i];
        }
        buffer_[length] = '\0';
        size_ = length;
    }

    void clear() noexcept
    {
        buffer_[0] = '\0';
        size_ = 0;
    }

    [[nodiscard]] const char* c_str() const noexcept
    {
        return buffer_.data();
    }

    [[nodiscard]] char* data() noexcept
    {
        return buffer_.data();
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return size_;
    }

    [[nodiscard]] bool empty() const noexcept
    {
        return 0 == size_;
    }

    [[nodiscard]] std::string_view view() const noexcept
    {
        return {buffer_.data(), size_};
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept
    {
        return Capacity;
    }

    friend bool operator==(const FixedString& lhs, const FixedString& rhs) noexcept
    {
        return lhs.view() == rhs.view();
    }

    friend bool operator==(const FixedString& lhs, std::string_view rhs) noexcept
    {
        return lhs.view() == rhs;
    }

private:
    std::array<char, Capacity> buffer_{};
    std::size_t size_{0};
};

}  // namespace axc::core
