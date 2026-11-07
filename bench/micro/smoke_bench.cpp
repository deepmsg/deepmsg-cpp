#include <benchmark/benchmark.h>

#include "core/version.h"

static void BmVersionString(benchmark::State& state)
{
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(axc::core::version_string());
    }
}

BENCHMARK(BmVersionString);
BENCHMARK_MAIN();
