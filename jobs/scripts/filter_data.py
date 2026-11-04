"""Filter sales rows by region and minimum amount. Standard library only."""
import argparse, csv, sys

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--region", default="All")
ap.add_argument("--min-amount", type=float, default=0)
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()

with open(a.input, newline="") as f:
    rows = [r for r in csv.DictReader(f)
            if (a.region == "All" or r["region"] == a.region)
            and float(r["amount"]) >= a.min_amount]

print(f"{len(rows)} rows match region={a.region}, min_amount={a.min_amount}")
if a.dry_run:
    print(f"[dry run] would write {a.output}")
    sys.exit(0)

with open(a.output, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["date", "region", "product", "amount"])
    w.writeheader()
    w.writerows(rows)
print(f"Wrote {a.output}")
