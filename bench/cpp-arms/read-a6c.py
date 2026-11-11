#!/usr/bin/env python3
"""Read the A6c storage grid (`run-batch.sh --fsync-catalog` / `DEEPMSG_IO_FALLOCATE`).

Usage:  read-a6c.py <root>

Every cell is a normal `run-batch.sh` tree; the axes are read from its
`RUN-INFO.txt` (`fallocate` and `fsync file=X catalog=Y`), so the directory
layout is free. Prints the latency cells, the E-D on/off contrast and each
cell's allocation picture (`filefrag.txt`: `st_blocks` + extent count).
"""

import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"
FSYNC = re.compile(r"file=(?P<file>\d+) catalog=(?P<catalog>\d+)")
WANTED = ("p40", "p50", "p90", "p99", "p99.9", "p99.99", "e_lt_p99")


def read_field(text, name):
    for line in text.splitlines():
        fields = line.split(None, 1)
        if len(fields) == 2 and fields[0] == name:
            return fields[1].strip()
    return None


def cells(root: Path):
    found = []
    for info in sorted(root.glob("**/RUN-INFO.txt")):
        cell = info.parent
        text = info.read_text()
        fsync = FSYNC.search(read_field(text, "fsync") or "")
        if fsync is None:
            continue
        histograms = sorted(cell.glob("run-*/**/*.hdr"))
        if not histograms:
            continue
        parts = cell.relative_to(root).parts
        round_index = next((i for i, part in enumerate(parts) if part.startswith("round-")), len(parts) - 1)
        batch = "/".join(parts[:round_index]) or "."
        round_name = parts[round_index] if round_index < len(parts) else "round"
        found.append(
            {
                "batch": batch,
                "cell": cell,
                "hdr": histograms[0],
                "fallocate": read_field(text, "fallocate") or "-",
                "file": int(fsync.group("file")),
                "catalog": int(fsync.group("catalog")),
                # `quantiles.sh` splits LABEL=PATH on the first `=`, so the
                # label must not carry one (the cell name has `fsync=`).
                "label": f"{batch}:{round_name}:{cell.name}".replace("=", "-").replace("/", "_"),
            }
        )
    return found


def histograms(found):
    args, keys = [], {}
    for cell in found:
        args.append(f"{cell['label']}={cell['hdr']}")
        keys[cell["label"]] = cell
    proc = subprocess.run(
        [str(QUANTILES), "--quantiles", "99.9,99.99", *args],
        capture_output=True, text=True, check=True)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    for line in lines[1:]:
        fields = line.split("\t")
        keys[fields[0]]["stats"] = {name: float(fields[index[name]]) for name in WANTED if name in index}


def counters(cell_dir):
    path = cell_dir / "run-1" / "counters.txt"
    values = {"total write time in ns": float("nan"), "max write time in ns": float("nan")}
    if path.exists():
        for line in path.read_text().splitlines():
            label, _, value = line.partition("\t")
            for name in values:
                if name in label:
                    values[name] = float(value)
    return values


def filefrag(cell_dir):
    path = cell_dir / "filefrag.txt"
    values = {"blocks": float("nan"), "extents": float("nan")}
    if path.exists():
        for line in path.read_text().splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] in values:
                try:
                    values[fields[0]] = float(fields[1])
                except ValueError:
                    pass
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
    histograms(found)

    print(f"{'batch':10} {'fallocate':10} {'file/catalog':13} {'E[X|X<p99]':>12} {'p50':>10} {'p99':>10} "
          f"{'blocks':>10} {'extents':>8} {'total write ns':>16} {'max write ns':>14}")
    rows = []
    for cell in found:
        stats = cell.get("stats", {})
        frag = filefrag(cell["cell"])
        count = counters(cell["cell"])
        rows.append((cell, stats, frag, count))
        print(f"{cell['batch']:10} {cell['fallocate']:10} {str(cell['file']) + '/' + str(cell['catalog']):13} "
              f"{stats.get('e_lt_p99', float('nan')):>12.0f} {stats.get('p50', float('nan')):>10.0f} "
              f"{stats.get('p99', float('nan')):>10.0f} {frag['blocks']:>10.0f} {frag['extents']:>8.0f} "
              f"{count['total write time in ns']:>16.0f} {count['max write time in ns']:>14.0f}")

    print()
    print(f"{'fsync':13} {'pair':24} {'on/off':>8}")
    # The batch order is off, on, on, off (ABBA); the trailing round number in
    # the batch name is what pairs them.
    suffix = re.compile(r"-(\d+)$")
    pairs = {}
    for cell, stats, _, _ in rows:
        round_name = suffix.search(cell["batch"])
        if round_name is None:
            continue
        key = (cell["file"], cell["catalog"], round_name.group(1))
        pairs.setdefault(key, {})[cell["fallocate"]] = cell
    for (file_level, catalog_level, round_name), pair in sorted(pairs.items()):
        off, on = pair.get("false"), pair.get("true")
        if off is None or on is None:
            continue
        ratio = on.get("stats", {}).get("e_lt_p99", float("nan")) / off.get("stats", {}).get("e_lt_p99", float("nan"))
        print(f"{str(file_level) + '/' + str(catalog_level):13} "
              f"{off['batch'] + ' -> ' + on['batch']:24} {ratio:>8.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
