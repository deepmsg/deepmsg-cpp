#!/usr/bin/env python3
"""Read the A6d E-E grid (`run-batch.sh` with DEEPMSG_REPLAY_IO_BACKEND/FADVISE).

Usage:  read-a6d.py <root>

Every cell is a normal `run-batch.sh` tree; the axes are read from its
`RUN-INFO.txt` (`replay io backend=… fadvise=…`), so the directory layout is
free. Prints each cell's latency and page footprint (`rss.txt` from the
archive's `smaps_rollup`), then the two paired contrasts of the E-E grid:

  * mmap vs pread at the same fadvise,
  * willneed vs none at the same backend,

paired by the round in the batch path (ABBA order). A live replay — the S2
scenario — always reads through `pread` (the private mapping freezes its page
on the first header rewrite), so the mmap/pread ratio there measures the
fallback and should sit at 1.
"""

import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"
REPLAY_IO = re.compile(r"^replay io\s+backend=(?P<backend>\w+) fadvise=(?P<fadvise>\w+)$", re.MULTILINE)
WANTED = ("p40", "p50", "p90", "p99", "p99.9", "p99.99", "e_lt_p99")
RSS_FIELDS = ("Rss", "Pss", "Pss_File", "Pss_Anon")


def cells(root: Path):
    found = []
    for info in sorted(root.glob("**/RUN-INFO.txt")):
        cell = info.parent
        text = info.read_text()
        replay_io = REPLAY_IO.search(text)
        if replay_io is None:
            continue
        histograms = sorted(cell.glob("run-*/**/*.hdr"))
        if not histograms:
            continue
        parts = cell.relative_to(root).parts
        round_index = next((i for i, part in enumerate(parts) if part.startswith("round-")), 0)
        batch = "/".join(parts[:round_index]) or "."
        round_name = parts[round_index] if round_index < len(parts) else "round"
        found.append(
            {
                "batch": batch,
                "round": round_name,
                "cell": cell,
                "hdr": histograms[0],
                "backend": replay_io.group("backend"),
                "fadvise": replay_io.group("fadvise"),
                # `quantiles.sh` splits LABEL=PATH on the first `=`, so the
                # label must not carry one (the cell name has `fsync=`), and it
                # must be unique across the tree (every batch has the same leaf
                # name, so the label is the whole relative path).
                "label": "_".join(parts).replace("=", "-"),
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


def rss(cell_dir):
    values = {name: float("nan") for name in RSS_FIELDS}
    path = cell_dir / "rss.txt"
    if path.exists():
        for line in path.read_text().splitlines():
            label, _, rest = line.partition(":")
            name = label.strip()
            if name in values:
                try:
                    values[name] = float(rest.split()[0])
                except (IndexError, ValueError):
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

    print(f"{'round':8} {'backend':8} {'fadvise':9} {'E[X|X<p99]':>12} {'p50':>9} {'p99':>9} "
          f"{'Rss kB':>10} {'Pss kB':>9} {'Pss_File':>9} {'Pss_Anon':>9}   (ns from quantiles.sh)")
    rows = []
    for cell in found:
        stats = cell.get("stats", {})
        page = rss(cell["cell"])
        rows.append((cell, stats))
        print(f"{cell['round']:8} {cell['backend']:8} {cell['fadvise']:9} "
              f"{stats.get('e_lt_p99', float('nan')):>12.0f} {stats.get('p50', float('nan')):>9.0f} "
              f"{stats.get('p99', float('nan')):>9.0f} {page['Rss']:>10.0f} {page['Pss']:>9.0f} "
              f"{page['Pss_File']:>9.0f} {page['Pss_Anon']:>9.0f}")

    def pairs(numerator, denominator):
        """Pair cells by round and fadvise, then print numerator/denominator."""
        print()
        print(f"{'round':8} {'fadvise':9} {numerator:>12} {denominator:>12} {'ratio':>8}")
        by_round = {}
        for cell, stats in rows:
            by_round.setdefault(cell["round"], {})[(cell["backend"], cell["fadvise"])] = stats
        for round_name, group in sorted(by_round.items()):
            for (backend, fadvise), top in sorted(group.items()):
                if backend != numerator:
                    continue
                bottom = group.get((denominator, fadvise))
                if bottom is None:
                    continue
                top_value = top.get("e_lt_p99", float("nan"))
                bottom_value = bottom.get("e_lt_p99", float("nan"))
                ratio = top_value / bottom_value if bottom_value else float("nan")
                print(f"{round_name:8} {fadvise:9} {top_value:>12.0f} {bottom_value:>12.0f} {ratio:>8.4f}")

    def floor():
        """The same combo in round 2 over round 1: what a ratio must beat."""
        print()
        print(f"{'backend':8} {'fadvise':9} {'round-2':>12} {'round-1':>12} {'ratio':>8}")
        by_combo = {}
        for cell, stats in rows:
            by_combo.setdefault((cell["backend"], cell["fadvise"]), {})[cell["round"]] = stats
        for (backend, fadvise), group in sorted(by_combo.items()):
            first, second = group.get("round-1"), group.get("round-2")
            if first is None or second is None:
                continue
            first_value = first.get("e_lt_p99", float("nan"))
            second_value = second.get("e_lt_p99", float("nan"))
            ratio = second_value / first_value if first_value else float("nan")
            print(f"{backend:8} {fadvise:9} {second_value:>12.0f} {first_value:>12.0f} {ratio:>8.4f}")

    floor()
    print()
    print("E [X | X < p99] contrast: mmap / pread")
    pairs("mmap", "pread")
    print()
    print("E [X | X < p99] contrast: willneed / none (same backend)")
    for backend in ("pread", "mmap"):
        print(f"  backend={backend}")
        print(f"{'round':10} {'willneed':>12} {'none':>12} {'ratio':>8}")
        by_round = {}
        for cell, stats in rows:
            if cell["backend"] == backend:
                by_round.setdefault(cell["round"], {})[cell["fadvise"]] = stats
        for round_name, group in sorted(by_round.items()):
            on, off = group.get("willneed"), group.get("none")
            if on is None or off is None:
                continue
            off_value = off.get("e_lt_p99", float("nan"))
            on_value = on.get("e_lt_p99", float("nan"))
            ratio = on_value / off_value if off_value else float("nan")
            print(f"{round_name:10} {on_value:>12.0f} {off_value:>12.0f} {ratio:>8.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
