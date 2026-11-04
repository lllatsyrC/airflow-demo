"""Summarize filtered sales into a text report. Standard library only."""
import argparse, csv, sys
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()

if a.dry_run:
    print(f"[dry run] would summarize {a.input} into {a.output}")
    sys.exit(0)

totals = defaultdict(float)
with open(a.input, newline="") as f:
    rows = list(csv.DictReader(f))
for r in rows:
    totals[r["product"]] += float(r["amount"])

lines = [f"Rows: {len(rows)}", f"Total: {sum(totals.values()):,.2f}", "", "By product:"]
lines += [f"  {p:<10} {v:>10,.2f}" for p, v in sorted(totals.items(), key=lambda x: -x[1])]
report = "\n".join(lines)

with open(a.output, "w") as f:
    f.write(report + "\n")
print(report)
