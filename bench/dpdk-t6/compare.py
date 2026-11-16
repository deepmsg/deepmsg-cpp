#!/usr/bin/env python3
"""T6's readings: turn JUnit XML into per-case statuses and diff two arms.

    compare.py collect --xml-dir DIR --out status.tsv [--classes TSV]
    compare.py diff --base status.tsv --other status.tsv --out DIR [--allow-deltas]
    compare.py ctest --a a.xml --b b.xml --out DIR [--allow-deltas]

`collect` reads Gradle's `TEST-<class>.xml` (one `<testcase>` per method) and
writes `class, method, status, message`. `diff` compares two of those files on
the keys present in both and writes `deltas.tsv`; it exits 1 when there are
deltas unless `--allow-deltas`. `ctest` does the same for CTest's
`--output-junit` files (one `<testsuite>` per test, name in `name`).

A row missing from one side is not a delta: a class can be filtered out (area,
timeout) on one arm, and the counts are printed so that gap is visible.
"""

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET


UNSTABLE = [
    # JUnit parameterized names embed things that change between runs: the
    # driver's aeron directory, a UUID, a Java identity hash, an object's
    # toString. Without folding them the same case has two names and the diff
    # loses it (seen in MediaDriverThreadNamingTest, TaggedFlowControlSystemTest).
    (re.compile(r"aeron-gavin-[0-9a-f-]+"), "aeron-<dir>"),
    (re.compile(r"/dev/shm/aeron-[^\s,\]]+"), "/dev/shm/aeron-<dir>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"), "<uuid>"),
    (re.compile(r"@[0-9a-f]{6,}\b"), "@<hash>"),
]


def stable(name):
    for pattern, replacement in UNSTABLE:
        name = pattern.sub(replacement, name)
    return name


def clean(text):
    if not text:
        return ""
    return " ".join(text.split())[:300].replace("\t", " ")


def gradle_cases(path):
    root = ET.parse(path).getroot()
    class_name = root.get("name") or os.path.basename(path)
    cases = []
    for case in root.findall("testcase"):
        failure = case.find("failure")
        error = case.find("error")
        skipped = case.find("skipped")
        if failure is not None or error is not None:
            status = "failed"
        elif skipped is not None:
            status = "skipped"
        else:
            status = "passed"
        message = ""
        if failure is not None:
            message = failure.get("message") or (failure.text or "")
        elif error is not None:
            message = error.get("message") or (error.text or "")
        cases.append((class_name, case.get("name") or "?", status, clean(message)))
    return cases


def ctest_cases(path):
    root = ET.parse(path).getroot()
    cases = []
    for case in root.iter("testcase"):
        status = "passed"
        message = ""
        if case.find("failure") is not None:
            status = "failed"
            message = case.find("failure").get("message") or ""
        elif case.find("error") is not None:
            status = "failed"
            message = case.find("error").get("message") or ""
        elif case.find("skipped") is not None:
            status = "skipped"
        cases.append((case.get("classname") or "ctest", case.get("name") or "?", status, clean(message)))
    return cases


def read_rows(path):
    rows = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            rows.append((parts[0], parts[1], parts[2], parts[3] if len(parts) > 3 else ""))
    return rows


def status_counts(rows):
    """Per key, how many cases ended in each status.

    JUnit names a parameterized invocation by its display name only (`[1]
    channel = "aeron:ipc"`), and several distinct test methods can share that
    name, so a key can appear many times in one class. Counting statuses per key
    keeps every case in the reading instead of silently keeping the last one.
    """
    result = {}
    for class_name, method, status, _ in rows:
        counts = result.setdefault((class_name, stable(method)), {})
        counts[status] = counts.get(status, 0) + 1
    return result


def write_status(path, cases):
    with open(path, "w", encoding="utf-8") as handle:
        for class_name, method, status, message in cases:
            handle.write(f"{class_name}\t{method}\t{status}\t{message}\n")


def counts(rows):
    result = {"passed": 0, "failed": 0, "skipped": 0}
    for _, _, status, _ in rows:
        result[status] = result.get(status, 0) + 1
    return result


def diff(base_label, base_rows, other_label, other_rows, out_dir, allow_deltas, classes_filter=None):
    base = status_counts(base_rows)
    other = status_counts(other_rows)
    common = sorted(set(base) & set(other))
    only_base = sorted(set(base) - set(other))
    only_other = sorted(set(other) - set(base))
    deltas = []
    for key in common:
        if classes_filter is not None and key[0] not in classes_filter:
            continue
        if base[key] != other[key]:
            message = ""
            for row in other_rows:
                if row[0] == key[0] and stable(row[1]) == key[1] and row[2] != "passed":
                    message = row[3]
                    break
            deltas.append((key[0], key[1], str(base[key]), str(other[key]), clean(message)))

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "deltas.tsv")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"class\tmethod\t{base_label}_statuses\t{other_label}_statuses\t{other_label}_message\n")
        for row in deltas:
            handle.write("\t".join(row) + "\n")

    print(f"{base_label}: {len(base_rows)} cases in {len(base)} keys {counts(base_rows)}")
    print(f"{other_label}: {len(other_rows)} cases in {len(other)} keys {counts(other_rows)}")
    print(f"common keys: {len(common)}, only in {base_label}: {len(only_base)}, only in {other_label}: {len(only_other)}")
    for name in only_base[:10]:
        print(f"  only {base_label}: {name[0]}#{name[1]}")
    for name in only_other[:10]:
        print(f"  only {other_label}: {name[0]}#{name[1]}")
    print(f"deltas: {len(deltas)} -> {path}")
    for row in deltas[:40]:
        print(f"  {row[0]}#{row[1]}: {row[2]} vs {row[3]}  {row[4][:120]}")
    if len(deltas) > 40:
        print(f"  ... {len(deltas) - 40} more in {path}")
    if deltas and not allow_deltas:
        print("FAIL: the two arms disagree", file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect")
    collect.add_argument("--xml-dir", required=True)
    collect.add_argument("--out", required=True)

    diff_parser = sub.add_parser("diff")
    diff_parser.add_argument("--base", required=True)
    diff_parser.add_argument("--other", required=True)
    diff_parser.add_argument("--base-label", default="base")
    diff_parser.add_argument("--other-label", default="other")
    diff_parser.add_argument("--out", required=True)
    diff_parser.add_argument("--allow-deltas", action="store_true")
    diff_parser.add_argument("--classes", help="TSV whose first column is the classes to keep")

    ctest = sub.add_parser("ctest")
    ctest.add_argument("--a", required=True)
    ctest.add_argument("--b", required=True)
    ctest.add_argument("--out", required=True)
    ctest.add_argument("--allow-deltas", action="store_true")

    args = parser.parse_args()

    if args.command == "collect":
        cases = []
        for name in sorted(os.listdir(args.xml_dir)):
            if name.startswith("TEST-") and name.endswith(".xml"):
                cases.extend(gradle_cases(os.path.join(args.xml_dir, name)))
        write_status(args.out, cases)
        rows = read_rows(args.out)
        print(f"{args.out}: {len(rows)} cases {counts(rows)}")
        return 0

    if args.command == "diff":
        classes = None
        if args.classes:
            with open(args.classes, encoding="utf-8") as handle:
                classes = {line.split("\t")[0].strip() for line in handle if line.strip()}
        return diff(
            args.base_label,
            read_rows(args.base),
            args.other_label,
            read_rows(args.other),
            args.out,
            args.allow_deltas,
            classes,
        )

    if args.command == "ctest":
        os.makedirs(args.out, exist_ok=True)
        a_path = os.path.join(args.out, "a.tsv")
        b_path = os.path.join(args.out, "b.tsv")
        write_status(a_path, ctest_cases(args.a))
        write_status(b_path, ctest_cases(args.b))
        return diff("a", read_rows(a_path), "b", read_rows(b_path), args.out, args.allow_deltas)

    return 2


if __name__ == "__main__":
    sys.exit(main())
