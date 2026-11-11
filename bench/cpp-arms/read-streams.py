#!/usr/bin/env python3
"""Read a multi-stream (A6f) `cpp-arms/run-batch.sh` tree.

Usage:  read-streams.py <results-dir>

`read.py` assumes one histogram per `run-<n>/` and one cell naming scheme; the
A6f cells have `--streams N` (N client/node pairs against one archive and one
driver) and/or `--recorder-threads N` (the archive-side extension), write
`run-<n>/stream-<i>/`, and carry `_streams=<n>`/`_threads=<n>` in the cell name.
This reader keeps the same reading (`p50`, `p99`, `E[X|X<p99]`) and adds the two
things a multi-stream cell is for:

- **the per-stream rows**, so a stream that stalls on its own is visible, and
- **the aggregate rate**: every client is asked for the same per-stream rate,
  the harness's own histogram count is what it actually sent, and the sum over
  the streams divided by the measured iterations is the achieved aggregate.

A ratio between `threads=1` and `threads=N` is printed only when both rounds
agree in sign, exactly like `read.py`; the raw pair is always printed too.
"""

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"

WANTED = ("p50", "p99", "e_lt_p99")

CELL = re.compile(
    r"(?P<scenario>[A-Za-z0-9]+)_(?P<arm>j-c|x-c|x-j|j)"
    r"_fsync=(?P<fsync>[0-2]+)_length=(?P<length>\d+)_rate=(?P<rate>[0-9A-Za-z]+)"
    r"(?P<extra>(?:_streams=[0-9]+)?(?:_threads=[0-9]+)?)$"
)


def rate_as_number(text):
    text = text.upper()
    if text.endswith("K"):
        return int(text[:-1]) * 1000
    if text.endswith("M"):
        return int(text[:-1]) * 1_000_000
    return int(text)


def parse_run_info(path):
    info = {}
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            info[parts[0]] = parts[1].strip()
    return info


def iterations_of(info):
    match = re.search(r"(\d+) iterations", info.get("rate/length", ""))
    return int(match.group(1)) if match else None


def stream_dirs(cell_dir):
    """`(stream_index, dir)` for every run of a cell; single-stream cells keep
    the A6a layout `<cell>/run-<n>/`."""
    found = []
    for run_dir in sorted(cell_dir.glob("run-*")):
        if not run_dir.is_dir():
            continue
        nested = sorted(run_dir.glob("stream-*"))
        if nested:
            for index, directory in enumerate(nested):
                found.append((index, directory))
        else:
            found.append((0, run_dir))
    return found


def read_histograms(paths):
    """One `quantiles.sh` call; rows keyed by label."""
    if not paths:
        return {}
    args = [f"{label}={path}" for label, path in paths]
    proc = subprocess.run([str(QUANTILES), *args], capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr.strip(), file=sys.stderr)
        return {}

    lines = proc.stdout.splitlines()
    if not lines:
        return {}
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    rows = {}
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) < len(index):
            continue
        rows[fields[0]] = {name: float(fields[index[name]]) for name in WANTED if name in index}
        rows[fields[0]]["count"] = int(fields[index["count"]])
    return rows


def collect(root):
    """`row` per (round, cell): name parts plus per-stream quantiles."""
    rows = []
    # `quantiles.sh` splits `LABEL=PATH` at the first `=`, and a cell name is
    # full of them (`fsync=0_length=32...`), so the tags are `round:id:stream`.
    for cell_index, cell_dir in enumerate(sorted(root.glob("round-*/**/"))):
        if not (cell_dir / "RUN-INFO.txt").is_file():
            continue
        match = CELL.match(cell_dir.name)
        if match is None:
            continue
        round_name = cell_dir.parent.name
        info = parse_run_info(cell_dir / "RUN-INFO.txt")
        extra = match.group("extra") or ""
        streams_match = re.search(r"_streams=([0-9]+)", extra)
        threads_match = re.search(r"_threads=([0-9]+)", extra)
        streams = int(streams_match.group(1)) if streams_match else 1
        threads = int(threads_match.group(1)) if threads_match else 1
        iterations = iterations_of(info)

        tagged = []
        for stream, directory in stream_dirs(cell_dir):
            hdr = sorted(directory.glob("*.hdr"))
            if len(hdr) != 1:
                continue
            tagged.append((f"{round_name}:{cell_index}:{stream}", hdr[0]))
        quantiles = read_histograms(tagged)
        if not quantiles:
            continue

        per_stream = {}
        for stream, _ in stream_dirs(cell_dir):
            tag = f"{round_name}:{cell_index}:{stream}"
            if tag in quantiles:
                per_stream[stream] = quantiles[tag]
        if not per_stream:
            continue

        total = sum(q["count"] for q in per_stream.values())
        rows.append(
            {
                "round": round_name,
                "scenario": match.group("scenario"),
                "arm": match.group("arm"),
                "fsync": int(match.group("fsync")),
                "length": int(match.group("length")),
                "rate": match.group("rate"),
                "streams": streams,
                "threads": threads,
                "per_stream": per_stream,
                "count": total,
                "achieved": (total / iterations) if iterations else None,
                "worst_p99": max(q["p99"] for q in per_stream.values()),
                "worst_e_lt_p99": max(q["e_lt_p99"] for q in per_stream.values()),
            }
        )
    return rows


def key_of(row):
    return (row["round"], row["scenario"], row["arm"], row["fsync"], row["length"], row["rate"], row["streams"])


def print_rows(rows):
    print("cell                                        threads  count      achieved/s  worst p50  worst p99  worst E[X|X<p99]")
    for row in rows:
        name = f"{row['scenario']}_{row['arm']}_fsync={row['fsync']}_len={row['length']}_rate={row['rate']}_streams={row['streams']}"
        achieved = f"{row['achieved']:.0f}" if row["achieved"] is not None else "-"
        p50 = max(q["p50"] for q in row["per_stream"].values())
        print(
            f"{name:<44} {row['threads']:>7}  {row['count']:>9}  {achieved:>10}  "
            f"{p50:>9.0f}  {row['worst_p99']:>9.0f}  {row['worst_e_lt_p99']:>15.0f}"
        )


def print_ratios(rows):
    """`threads=N / threads=1` per cell key, one line per (key, round)."""
    by_key = defaultdict(dict)
    for row in rows:
        by_key[key_of(row)].setdefault(row["threads"], {})[row["round"]] = row

    if not any(len(threads) > 1 for threads in by_key.values()):
        return

    print()
    print("threads ratios (N / 1): aggregate rate, worst E[X|X<p99]")
    for key, threads in sorted(by_key.items()):
        baseline = threads.get(1)
        if baseline is None:
            continue
        for count, rows_for_count in sorted(threads.items()):
            if count == 1:
                continue
            for round_name, row in sorted(rows_for_count.items()):
                base = baseline.get(round_name)
                if base is None:
                    continue
                rate_ratio = row["achieved"] / base["achieved"] if base["achieved"] and row["achieved"] else None
                latency_ratio = (
                    row["worst_e_lt_p99"] / base["worst_e_lt_p99"] if base["worst_e_lt_p99"] else None
                )
                cell = f"{key[1]}_{key[2]}_fsync={key[3]}_len={key[4]}_rate={key[5]}_streams={key[6]}"
                rate_text = f"{rate_ratio:.3f}" if rate_ratio is not None else "-"
                latency_text = f"{latency_ratio:.3f}" if latency_ratio is not None else "-"
                print(f"{key[0]:<8} {cell:<48} threads {count}/{1}  rate {rate_text}  latency {latency_text}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()

    rows = collect(args.results)
    if not rows:
        print(f"no multi-stream cells under {args.results}", file=sys.stderr)
        return 1
    print_rows(rows)
    print_ratios(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
