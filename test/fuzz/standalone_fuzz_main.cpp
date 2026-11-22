// The non-libFuzzer runner (gcc debug / asan presets, and ctest).
//
// It replays every seed in full, then truncates it position by position up to
// 512 bytes (sampled at power-of-two and tail boundaries beyond that), then
// flips the first 64 bytes one at a time. On an SBE message that is the whole
// fixed block; on a disk file it is the header plus every structural boundary
// the length checks can trip over.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "fuzz_common.h"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);

namespace
{

constexpr std::size_t MAX_SEED_SIZE = 8U * 1024U * 1024U;
constexpr std::size_t BYTEWISE_PREFIX_LIMIT = 512;
constexpr std::size_t FLIP_LIMIT = 64;

std::size_t executions = 0;

void run(const std::uint8_t* data, std::size_t size)
{
    (void)LLVMFuzzerTestOneInput(data, size);
    ++executions;
}

bool is_seed_file(const std::filesystem::path& path)
{
    const std::string name = path.filename().string();
    if ("README.md" == name || "expected.txt" == name)
    {
        return false;
    }
    const std::string extension = path.extension().string();
    return ".md" != extension && ".txt" != extension;
}

std::vector<std::uint8_t> read_file(const std::filesystem::path& path)
{
    std::vector<std::uint8_t> bytes{};
    std::ifstream stream{path, std::ios::binary};
    if (!stream)
    {
        return bytes;
    }
    stream.seekg(0, std::ios::end);
    const std::streamoff length = stream.tellg();
    if (length <= 0 || static_cast<std::uint64_t>(length) > MAX_SEED_SIZE)
    {
        return bytes;
    }
    bytes.resize(static_cast<std::size_t>(length));
    stream.seekg(0, std::ios::beg);
    stream.read(reinterpret_cast<char*>(bytes.data()), length);
    return bytes;
}

void scan_seed(const std::vector<std::uint8_t>& seed)
{
    const std::uint8_t* data = seed.data();
    const std::size_t size = seed.size();

    run(data, size);

    // Every prefix up to 512 bytes catches a truncated fixed block, a var-data
    // length that runs past the end, and a disk header cut mid-field.
    const std::size_t bytewise = std::min(size, BYTEWISE_PREFIX_LIMIT);
    for (std::size_t length = 0; length <= bytewise; ++length)
    {
        run(data, length);
    }

    // A 1 MiB file cannot be truncated position by position, so sample the
    // structural boundaries: powers of two and the last few bytes.
    if (size > BYTEWISE_PREFIX_LIMIT)
    {
        for (std::size_t length = BYTEWISE_PREFIX_LIMIT * 2; length < size; length *= 2)
        {
            run(data, length);
        }
        for (std::size_t back = 1; back <= 8 && back < size; back *= 2)
        {
            run(data, size - back);
        }
    }

    // Header/schema/template/blockLength live in the first eight bytes; one
    // flipped byte at a time is what turns a valid frame into a hostile one.
    const std::size_t flips = std::min(size, FLIP_LIMIT);
    for (std::size_t index = 0; index < flips; ++index)
    {
        std::vector<std::uint8_t> mutated = seed;
        mutated[index] ^= 0xFF;
        run(mutated.data(), mutated.size());
        mutated[index] ^= 0xFF;
        mutated[index] ^= 0x7F;
        run(mutated.data(), mutated.size());
    }
}

void scan_path(const std::filesystem::path& path)
{
    std::error_code ec;
    if (std::filesystem::is_directory(path, ec))
    {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(path, ec))
        {
            if (entry.is_regular_file(ec) && is_seed_file(entry.path()))
            {
                scan_seed(read_file(entry.path()));
            }
        }
        return;
    }
    if (std::filesystem::is_regular_file(path, ec))
    {
        scan_seed(read_file(path));
    }
}

void run_fixed_edge_cases()
{
    run(nullptr, 0);

    std::vector<std::uint8_t> zeros(16, 0x00);
    std::vector<std::uint8_t> ones(16, 0xFF);
    for (std::size_t size = 1; size <= 16; ++size)
    {
        run(zeros.data(), size);
        run(ones.data(), size);
    }

    // A forged SBE header: short body, blockLength that cannot fit, schema 101.
    std::vector<std::uint8_t> forged = {0xFF, 0xFF, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00};
    run(forged.data(), forged.size());
}

}  // namespace

int main(int argc, char** argv)
{
    if (axc::fuzz::emit_seeds_if_requested(argc, argv))
    {
        return 0;
    }

    run_fixed_edge_cases();

    bool explicit_path = false;
    for (int i = 1; i < argc; ++i)
    {
        explicit_path = true;
        scan_path(std::filesystem::path{argv[i]});
    }

    if (!explicit_path)
    {
        // The target's own structurally valid seeds.
        axc::fuzz::ShmSandbox emitted{};
        axc::fuzz::emit_seeds(emitted.dir());
        scan_path(emitted.dir());

#ifdef AXC_PROJECT_SOURCE_DIR
#ifdef AXC_FUZZ_SEED_SUBDIR
        const std::string subdir = AXC_FUZZ_SEED_SUBDIR;
        if (!subdir.empty())
        {
            scan_path(std::filesystem::path{AXC_PROJECT_SOURCE_DIR} / subdir);
        }
#endif
#endif
    }

    std::fprintf(stderr, "fuzz-regression: %zu executions\n", executions);
    return 0;
}
