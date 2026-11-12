#!/usr/bin/env python3
"""Summarizes repeated benchmark logs, which must come from the same command.

    scripts/summarize_bench.py run1.log run2.log run3.log

Prints each table with the median of every numeric cell and the spread
between passes.
"""
import statistics
import sys


def tables(path):
    """Yields (header, rows) for each Markdown table in a log."""
    header, rows = None, []
    for line in open(path):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if not line.startswith("|"):
            if header:
                yield header, rows
            header, rows = None, []
        elif set(cells[0]) <= set("-"):
            pass
        elif header is None:
            header = cells
        else:
            rows.append(cells)
    if header:
        yield header, rows


def number(cell):
    try:
        return float(cell)
    except ValueError:
        return None


def median_row(rows):
    out = []
    for cells in zip(*rows):
        values = [number(c) for c in cells]
        out.append(cells[0] if None in values else statistics.median(values))
    return out


def spread(header, passes):
    """Prints, per column, how far apart the passes are, as (max - min) / min."""
    for column, name in enumerate(header):
        gaps = []
        for rows in zip(*passes):
            values = [number(r[column]) for r in rows]
            if None not in values and min(values) > 0:
                gaps.append((max(values) - min(values)) / min(values))
        if gaps and max(gaps) > 0:
            gaps.sort()
            print(f"  {name}: median {statistics.median(gaps):.1%}, max {gaps[-1]:.1%}")


def main(paths):
    logs = [list(tables(path)) for path in paths]
    if len({len(log) for log in logs}) != 1:
        sys.exit("logs have different numbers of tables")
    for group in zip(*logs):
        header = group[0][0]
        passes = [rows for _, rows in group]
        if {len(rows) for rows in passes} != {len(passes[0])}:
            sys.exit(f"tables differ in length: {header}")
        medians = [median_row(rows) for rows in zip(*passes)]
        print("| " + " | ".join(header) + " |")
        print("|" + "---|" * len(header))
        for row in medians:
            print("| " + " | ".join(f"{c:g}" if isinstance(c, float) else c for c in row) + " |")
        print(f"spread between {len(paths)} passes:")
        spread(header, passes)
        print()


if __name__ == "__main__":
    main(sys.argv[1:] or sys.exit(__doc__))
