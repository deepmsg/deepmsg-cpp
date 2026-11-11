#include "core/agent/cpu_affinity.h"

#include <sched.h>
#include <cerrno>

namespace axc::core
{
namespace
{

bool parse_non_negative_int(std::string_view text, int& value) noexcept
{
    if (text.empty())
    {
        return false;
    }

    long parsed = 0;
    for (const char c : text)
    {
        if (c < '0' || c > '9')
        {
            return false;
        }
        parsed = parsed * 10 + (c - '0');
        if (parsed > CpuAffinity::MAX_CPUS)
        {
            return false;
        }
    }

    value = static_cast<int>(parsed);
    return true;
}

bool has_separator(std::string_view spec) noexcept
{
    return spec.find(',') != std::string_view::npos || spec.find('-') != std::string_view::npos;
}

}  // namespace

std::expected<CpuAffinity, Error> CpuAffinity::parse(std::string_view spec) noexcept
{
    if (spec.empty() || spec == "-1")
    {
        return CpuAffinity::none();
    }

    if (!has_separator(spec))
    {
        int index = 0;
        if (!parse_non_negative_int(spec, index))
        {
            return std::unexpected(make_error(ErrorCode::config_error, "cpu affinity must be an index, list or range"));
        }

        CpuAffinity affinity;
        affinity.mode_ = Mode::index;
        affinity.index_ = index;
        return affinity;
    }

    CpuAffinity affinity;
    affinity.mode_ = Mode::set;

    std::size_t cursor = 0;
    while (cursor < spec.size())
    {
        const std::size_t comma = spec.find(',', cursor);
        const std::size_t end = comma == std::string_view::npos ? spec.size() : comma;
        const std::string_view token = spec.substr(cursor, end - cursor);

        const std::size_t dash = token.find('-');
        int first = 0;
        int last = 0;

        if (dash == std::string_view::npos)
        {
            if (!parse_non_negative_int(token, first))
            {
                return std::unexpected(make_error(ErrorCode::config_error, "bad cpu affinity token"));
            }
            last = first;
        }
        else
        {
            if (!parse_non_negative_int(token.substr(0, dash), first) ||
                !parse_non_negative_int(token.substr(dash + 1), last) || first > last)
            {
                return std::unexpected(make_error(ErrorCode::config_error, "bad cpu affinity range"));
            }
        }

        if (last >= MAX_CPUS)
        {
            return std::unexpected(make_error(ErrorCode::config_error, "cpu affinity out of range"));
        }

        for (int cpu = first; cpu <= last; ++cpu)
        {
            const auto word = static_cast<std::size_t>(cpu / 64);
            const auto bit = static_cast<std::uint64_t>(1) << (cpu % 64);
            affinity.bits_[word] |= bit;
        }

        cursor = end + 1;
    }

    if (0 == affinity.count())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "empty cpu affinity set"));
    }

    return affinity;
}

bool CpuAffinity::contains(int cpu) const noexcept
{
    if (mode_ != Mode::set || cpu < 0 || cpu >= MAX_CPUS)
    {
        return false;
    }

    const auto word = static_cast<std::size_t>(cpu / 64);
    const auto bit = static_cast<std::uint64_t>(1) << (cpu % 64);
    return 0 != (bits_[word] & bit);
}

std::size_t CpuAffinity::count() const noexcept
{
    std::size_t total = 0;
    for (const std::uint64_t word : bits_)
    {
        total += static_cast<std::size_t>(__builtin_popcountll(word));
    }
    return total;
}

CpuAffinity CpuAffinity::nth(std::size_t n) const noexcept
{
    if (mode_ == Mode::none)
    {
        return none();
    }

    if (mode_ == Mode::index)
    {
        // `n` behind the configured index; the allowed-cpuset bounds are
        // checked when the affinity is applied.
        CpuAffinity affinity;
        affinity.mode_ = Mode::index;
        affinity.index_ = index_ + static_cast<int>(n);
        return affinity;
    }

    std::size_t seen = 0;
    for (std::size_t word = 0; word < bits_.size(); ++word)
    {
        std::uint64_t remaining = bits_[word];
        while (0 != remaining)
        {
            const int bit = static_cast<int>(__builtin_ctzll(remaining));
            if (seen == n)
            {
                CpuAffinity affinity;
                affinity.mode_ = Mode::set;
                affinity.bits_[word] = static_cast<std::uint64_t>(1) << bit;
                return affinity;
            }
            ++seen;
            remaining &= remaining - 1;
        }
    }

    return none();
}

bool CpuAffinity::apply_to_current_thread() const noexcept
{
    if (mode_ == Mode::none)
    {
        return true;
    }

    cpu_set_t set;
    CPU_ZERO(&set);

    if (mode_ == Mode::index)
    {
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        if (0 != ::sched_getaffinity(0, sizeof(allowed), &allowed))
        {
            return false;
        }

        int seen = 0;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        {
            if (!CPU_ISSET(cpu, &allowed))
            {
                continue;
            }
            if (seen == index_)
            {
                CPU_SET(cpu, &set);
                return 0 == ::sched_setaffinity(0, sizeof(set), &set);
            }
            ++seen;
        }

        return false;
    }

    for (int cpu = 0; cpu < MAX_CPUS; ++cpu)
    {
        if (contains(cpu))
        {
            CPU_SET(cpu, &set);
        }
    }

    return 0 == ::sched_setaffinity(0, sizeof(set), &set);
}

}  // namespace axc::core
