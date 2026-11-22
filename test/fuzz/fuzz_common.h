// Shared foundation for the libFuzzer targets and their standalone runner.
//
// Two constraints shape this file:
//   * the standalone mode must build on gcc, where <fuzzer/FuzzedDataProvider.h>
//     is not installed, so the byte splitter is ours;
//   * file parsing targets write to /dev/shm, never to a physical disk.

#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace axc::fuzz
{

/// A per-process directory under /dev/shm, removed on destruction.
class ShmSandbox
{
public:
    ShmSandbox()
    {
        const std::filesystem::path base = std::filesystem::exists("/dev/shm") ? std::filesystem::path{"/dev/shm"}
                                                                               : std::filesystem::temp_directory_path();
        dir_ = base / ("axc-fuzz-" + std::to_string(::getpid()));
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
    }

    ~ShmSandbox()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    ShmSandbox(const ShmSandbox&) = delete;
    ShmSandbox& operator=(const ShmSandbox&) = delete;

    [[nodiscard]] const std::filesystem::path& dir() const noexcept
    {
        return dir_;
    }

    [[nodiscard]] std::filesystem::path write_file(const char* name, std::span<const std::uint8_t> bytes) const
    {
        const std::filesystem::path path = dir_ / name;
        std::FILE* file = std::fopen(path.c_str(), "wb");
        if (nullptr != file)
        {
            if (!bytes.empty())
            {
                (void)std::fwrite(bytes.data(), 1, bytes.size(), file);
            }
            (void)std::fclose(file);
        }
        return path;
    }

private:
    std::filesystem::path dir_{};
};

/// First byte is a selector; the rest is one message.
class ByteCursor
{
public:
    explicit ByteCursor(std::span<const std::uint8_t> bytes) noexcept : bytes_(bytes) {}

    [[nodiscard]] bool empty() const noexcept
    {
        return bytes_.empty();
    }

    [[nodiscard]] std::uint8_t consume_u8() noexcept
    {
        if (bytes_.empty())
        {
            return 0;
        }
        const std::uint8_t value = bytes_[0];
        bytes_ = bytes_.subspan(1);
        return value;
    }

    [[nodiscard]] std::span<const std::uint8_t> remaining() const noexcept
    {
        return bytes_;
    }

private:
    std::span<const std::uint8_t> bytes_;
};

/// Defined once by each fuzz target: structurally valid seeds for its surface.
/// Targets whose seeds are the golden files write nothing.
void emit_seeds(const std::filesystem::path& dir);

/// Copies a checked-in regression input (`test/fuzz/artifacts/`) into a corpus.
inline void copy_seed_file(const std::filesystem::path& source, const std::filesystem::path& dir)
{
    std::error_code ec;
    std::filesystem::copy_file(source, dir / source.filename(), std::filesystem::copy_options::overwrite_existing, ec);
}

/// Shared by `LLVMFuzzerInitialize` and the standalone main. Returns true when
/// `--emit-seeds <dir>` was answered; the caller then leaves.
inline bool emit_seeds_if_requested(int argc, char** argv)
{
    // libFuzzer rejects unknown flags before the fuzzer starts, so the script
    // can ask for seeds through the environment and give libFuzzer a `-runs=0`.
    if (const char* directory = std::getenv("AXC_FUZZ_EMIT_SEEDS"); nullptr != directory)
    {
        emit_seeds(std::filesystem::path{directory});
        return true;
    }
    for (int i = 1; i + 1 < argc; ++i)
    {
        if (0 == std::strcmp(argv[i], "--emit-seeds"))
        {
            emit_seeds(std::filesystem::path{argv[i + 1]});
            return true;
        }
    }
    return false;
}

inline int seed_emitter_initialize(int* argc, char*** argv)
{
    if (emit_seeds_if_requested(*argc, *argv))
    {
        std::exit(0);
    }
    return 0;
}

}  // namespace axc::fuzz
