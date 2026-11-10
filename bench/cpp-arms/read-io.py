#!/usr/bin/env python3
"""Read the A6b io-backend grid (`run-batch.sh` with `DEEPMSG_IO_BACKEND`).

Usage:  read-io.py <root>

`<root>` holds one batch per backend and round, each a normal `run-batch.sh`
tree:

    <root>/<backend>-<round>/round-1/s2_x-c_fsync=<0|1|2>_length=32_rate=25K/
        run-1/*.hdr  counters.txt  RUN-INFO.txt

The arms are the same (`x-c`); only `deepmsg.archive.file.io.backend` moves, so
a ratio is the backend and nothing else. Like the sibling readers, a ratio is
quoted only when the two round pairs agree in sign: `sync-1 -> uring-1` and
`uring-2 -> sync-2` (the batch order is S,U,U,S).
"""

import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"
CELL = re.compile(
    r"(?P<scenario>[a-z0-9]+)_(?P<arm>[a-z-]+)_fsync=(?P<fsync>\d+)"
    r"_length=(?P<length>\d+)_rate=(?P<rate>[0-9]+[KMG]?)"
)

WANTED = ("p40", "p50", "p90", "p99", "p99.9", "p99.99", "e_lt_p99")
COUNTERS = (
    "archive-recorder max write time in ns",
    "archive-recorder total write time in ns",
    "archive-recorder total write bytes",
    "archive-recorder max cycle time in ns",
)


def cells(root: Path):
    """(backend, round, fsync, cell_dir, hdr_paths) for every cell."""
    found = []
    for batch in sorted(root.iterdir()):
        if not batch.is_dir():
            continue
        backend, _, round_name = batch.name.rpartition("-")
        for hdr in sorted(batch.glob("round-*/**/run-*/**/*.hdr")):
            m = CELL.search(str(hdr))
            if m is None:
                continue
            found.append((backend, round_name, int(m.group("fsync")), hdr.parent.parent, [hdr]))
    return found


def histograms(all_cells):
    args = []
    keys = {}
    for backend, round_name, fsync, _, paths in all_cells:
        for path in paths:
            label = f"{backend}:{round_name}:{fsync}"
            args.append(f"{label}={path}")
            keys[label] = (backend, round_name, fsync)

    proc = subprocess.run(
        [str(QUANTILES), "--quantiles", "99.9,99.99", *args],
        capture_output=True, text=True, check=True)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    rows = {}
    for line in lines[1:]:
        fields = line.split("\t")
        key = keys[fields[0]]
        rows[key] = {name: float(fields[index[name]]) for name in WANTED if name in index}
    return rows


def counters(cell_dir: Path):
    path = cell_dir / "run-1" / "counters.txt"
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text().splitlines():
        label, _, value = line.partition("\t")
        for name in COUNTERS:
            # The published label carries the archive id suffix
            # (` - archiveId=<id>`), so match by name.
            if name in label:
                values[name] = float(value)
    return values


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    root = Path(sys.argv[1])
    found = cells(root)
    if not found:
        print(f"no cells under {root}", file=sys.stderr)
        return 2

    rows = histograms(found)
    by_cell = {(b, r, f): row for (b, r, f), row in rows.items()}

    print(f"{'backend':8} {'round':6} {'fsync':5} {'E[X|X<p99]':>14} {'p50':>10} {'p99':>10} {'p99.99':>12}")
    for backend, round_name, fsync, _, _ in found:
        row = by_cell.get((backend, round_name, fsync))
        if row is None:
            continue
        print(f"{backend:8} {round_name:6} {fsync:<5} {row['e_lt_p99']:>14.0f} {row['p50']:>10.0f} "
              f"{row['p99']:>10.0f} {row['p99.99']:>12.0f}")

    print()
    print(f"{'fsync':5} {'pair':22} {'uring/sync':>11}")
    # Both pairs are normalised to uring/sync: the batch order is sync, uring,
    # uring, sync, so the second pair reads backwards.
    pairs = [("sync", "1", "uring", "1"), ("sync", "2", "uring", "2")]
    for fsync in sorted({f for _, _, f, _, _ in found}):
        values = []
        for sync_round, uring_round in (("1", "1"), ("2", "2")):
            sync_row = by_cell.get(("sync", sync_round, fsync))
            uring_row = by_cell.get(("uring", uring_round, fsync))
            if sync_row is None or uring_row is None:
                continue
            ratio = uring_row["e_lt_p99"] / sync_row["e_lt_p99"]
            values.append(ratio)
            print(f"{fsync:<5} {'sync-'+sync_round+' -> uring-'+uring_round:22} {ratio:>11.4f}")
        if len(values) == 2:
            verdict = "agrees" if (values[0] - 1) * (values[1] - 1) > 0 else "DISAGREES"
            print(f"{fsync:<5} {'sign':22} {verdict:>11}")

    print()
    print(f"{'backend':8} {'round':6} {'fsync':5} " +
          " ".join(f"{name.replace('archive-recorder ', ''):>28}" for name in COUNTERS))
    for backend, round_name, fsync, cell_dir, _ in found:
        values = counters(cell_dir)
        row = " ".join(f"{values.get(name, float('nan')):>28.0f}" for name in COUNTERS)
        print(f"{backend:8} {round_name:6} {fsync:<5} {row}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
