#include "core/counters/counter_directory.h"

#include <cstddef>
#include <cstring>

namespace axc::core
{
namespace
{

struct FindContext
{
    std::int32_t type_id;
    std::span<const std::uint8_t> key;
    std::int32_t found{AERON_NULL_COUNTER_ID};
};

void on_counter(int64_t /*value*/, int32_t id, int32_t type_id, const uint8_t* key, size_t key_length,
                const char* /*label*/, size_t /*label_length*/, void* clientd)
{
    auto* context = static_cast<FindContext*>(clientd);
    if (context->found != AERON_NULL_COUNTER_ID || type_id != context->type_id || key_length != context->key.size())
    {
        return;
    }

    if (0 == std::memcmp(key, context->key.data(), key_length))
    {
        context->found = id;
    }
}

}  // namespace

std::int32_t find_counter_id_by_type_and_key(aeron_counters_reader_t* reader, std::int32_t type_id,
                                             std::span<const std::uint8_t> key) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }

    FindContext context{type_id, key};
    aeron_counters_reader_foreach_counter(reader, &on_counter, &context);
    return context.found;
}

}  // namespace axc::core
