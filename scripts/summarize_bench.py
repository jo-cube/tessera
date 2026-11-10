#!/usr/bin/env python3
"""Summarizes repeated benchmark logs, which must come from the same command.

    scripts/summarize_bench.py run1.log run2.log run3.log

Prints each table with the median of every numeric cell, the spread between
passes, and, for geometry logs, the model of Zstandard decode time.
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


def solve(rows):
    """Least squares of the last column on the others, by normal equations."""
    n = len(rows[0]) - 1
    system = [
        [sum(r[i] * r[j] for r in rows) for j in range(n)]
        + [sum(r[i] * r[-1] for r in rows)]
        for i in range(n)
    ]
    for i in range(n):
        pivot = max(range(i, n), key=lambda r: abs(system[r][i]))
        system[i], system[pivot] = system[pivot], system[i]
        for r in range(n):
            if r != i:
                factor = system[r][i] / system[i][i]
                system[r] = [a - factor * b for a, b in zip(system[r], system[i])]
    return [system[i][n] / system[i][i] for i in range(n)]


def decode_model(found):
    """Fits per-frame decode time on real blocks and tests it on controls."""
    def point(row):
        return [1.0, row[column["entropy literals"]], row[column["sequences"]], row[column["zstd d"]]]

    if len(found) < 2:
        return
    (header, real), (_, controlled) = found[:2]
    column = {name: i for i, name in enumerate(header)}
    # A frame of raw blocks has no sequences and skips the entropy tables.
    fit = [point(row) for row in real if row[column["sequences"]] > 0]
    base, literal, sequence = solve(fit)
    predicted = [base + literal * p[1] + sequence * p[2] for p in fit]
    mean = statistics.mean(p[3] for p in fit)
    residual = sum((p[3] - q) ** 2 for p, q in zip(fit, predicted))
    total = sum((p[3] - mean) ** 2 for p in fit)
    print(
        f"\nZstandard decode model on {len(fit)} real block sets: {base:.2f} us per frame"
        f" + {literal * 1000:.3f} ns per entropy-coded literal"
        f" + {sequence * 1000:.2f} ns per sequence; R^2 {1 - residual / total:.4f}"
    )
    print("controlled blocks, actual vs predicted us per block:")
    for row in controlled:
        _, literals, sequences, actual = point(row)
        print(f"  {row[0]}: {actual:.2f} vs {base + literal * literals + sequence * sequences:.2f}")


def main(paths):
    logs = [list(tables(path)) for path in paths]
    if len({len(log) for log in logs}) != 1:
        sys.exit("logs have different numbers of tables")
    models = []
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
        if "sequences" in header:
            models.append((header, medians))
    decode_model(models)


if __name__ == "__main__":
    main(sys.argv[1:] or sys.exit(__doc__))
