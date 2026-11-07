#!/usr/bin/env python3
"""Check `path<TAB>line` citations against the pinned reference checkout.

The plan documents live outside this repo (analysis/cpp). X0-6 turns every
`file:line` citation into a TSV and runs this script, so a stale line number
fails loudly instead of silently pointing at the wrong code.

Usage:
    tools/repo/check-citations.py citations.tsv [--reference DIR]

The reference defaults to $AXC_REFERENCE_DIR or third_party/aeron.
Exit code 1 if any cited file is missing or the line is out of range.
"""

import argparse
import os
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tsv")
    parser.add_argument(
        "--reference",
        default=os.environ.get("AXC_REFERENCE_DIR", "third_party/aeron"),
    )
    args = parser.parse_args()

    bad = 0
    with open(args.tsv, encoding="utf-8") as table:
        for number, raw in enumerate(table, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 2:
                print(f"{args.tsv}:{number}: expected path<TAB>line", file=sys.stderr)
                bad += 1
                continue
            path, lineno = fields[0], fields[1]
            try:
                with open(os.path.join(args.reference, path), "rb") as source:
                    total = sum(1 for _ in source)
            except OSError as exc:
                print(f"{args.tsv}:{number}: {path}: {exc}", file=sys.stderr)
                bad += 1
                continue
            try:
                wanted = int(lineno)
            except ValueError:
                print(f"{args.tsv}:{number}: bad line number {lineno!r}", file=sys.stderr)
                bad += 1
                continue
            if wanted < 1 or wanted > total:
                print(
                    f"{args.tsv}:{number}: {path}: line {wanted} out of range (1..{total})",
                    file=sys.stderr,
                )
                bad += 1

    if bad:
        print(f"check-citations: {bad} stale citation(s)", file=sys.stderr)
        return 1
    print("check-citations: all citations resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
