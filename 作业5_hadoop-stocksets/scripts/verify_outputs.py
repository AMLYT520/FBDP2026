#!/usr/bin/env python3
"""Independent set-based checker for Hadoop homework 5 outputs.

Usage:
  python3 scripts/verify_outputs.py A.txt B.txt \
      output/output_task1/part-r-00000 \
      output/output_task2/part-r-00000

This verifies the Java MapReduce results independently; it does not modify files.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from itertools import zip_longest

VALID_INDEX = {"101", "102", "103"}
TAG_ORDER = ("BOTH", "A_ONLY", "B_ONLY")


def load_input(path: Path) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            fields = line.split()
            if not fields:
                continue
            if len(fields) != 2 or fields[0] not in VALID_INDEX:
                raise ValueError(f"{path}:{line_number}: invalid input {line.rstrip()!r}")
            pairs.add((fields[0], fields[1]))
    return pairs


def build_expected(a: set[tuple[str, str]], b: set[tuple[str, str]]) -> tuple[list[str], list[str]]:
    relations: list[str] = []
    counts: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for index, ticker in sorted(a | b):
        if (index, ticker) in a and (index, ticker) in b:
            relation = "BOTH"
        elif (index, ticker) in a:
            relation = "A_ONLY"
        else:
            relation = "B_ONLY"
        relations.append(f"{index}\t{ticker}\t{relation}")
        counts[index][TAG_ORDER.index(relation)] += 1
    summaries: list[str] = []
    for index in sorted(counts):
        both, a_only, b_only = counts[index]
        summaries.append(f"{index}\t{both}\t{a_only}\t{b_only}\t{both + a_only + b_only}")
    return relations, summaries


def read_actual(path: Path, columns: int) -> list[str]:
    records: list[str] = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            fields = line.split()
            if len(fields) != columns:
                raise ValueError(
                    f"{path}:{line_number}: expected {columns} columns, got {len(fields)}: {line.rstrip()!r}"
                )
            records.append("\t".join(fields))
    return records


def assert_matches(label: str, expected: list[str], actual: list[str]) -> None:
    if expected == actual:
        return
    details = [f"{label} mismatch: expected {len(expected)} rows, got {len(actual)} rows"]
    for position, (want, have) in enumerate(zip_longest(expected, actual), 1):
        if want != have:
            details.append(f"First difference at row {position}:")
            details.append(f"  expected: {want!r}")
            details.append(f"  actual:   {have!r}")
            break
    raise AssertionError("\n".join(details))


def verify(a_file: Path, b_file: Path, task1_file: Path, task2_file: Path) -> None:
    a = load_input(a_file)
    b = load_input(b_file)
    expect1, expect2 = build_expected(a, b)
    assert_matches("Task 1", expect1, read_actual(task1_file, 3))
    assert_matches("Task 2", expect2, read_actual(task2_file, 5))
    print("PASS: both task outputs match independent Python set calculations.")
    print(f"A distinct index-stock pairs: {len(a)}")
    print(f"B distinct index-stock pairs: {len(b)}")
    print(f"Union distinct index-stock pairs: {len(expect1)}")
    print(f"Index summary rows: {len(expect2)}")
    for row in expect2:
        print("  " + row)


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print(
            "Usage: python3 scripts/verify_outputs.py A.txt B.txt "
            "output/output_task1/part-r-00000 output/output_task2/part-r-00000",
            file=sys.stderr,
        )
        return 2
    try:
        verify(*(Path(arg) for arg in argv[1:]))
    except (OSError, ValueError, AssertionError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
