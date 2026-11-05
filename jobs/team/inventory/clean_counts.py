"""Step 1: clean a batch of store inventory counts.

Stands in for an existing team script that is normally run by hand:
    python clean_counts.py --input /data/batches/B1/inventory_counts.csv \
        --output /tmp/B1/counts_clean.csv --batch-id B1
"""
import argparse

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--batch-id", required=True)
a = ap.parse_args()

df = pd.read_csv(a.input)
n_raw = len(df)
df["store"] = df["store"].str.strip()
df["sku"] = df["sku"].str.strip().str.upper()
df = df.drop_duplicates()
bad = df["quantity"] < 0
if bad.any():
    print(f"Dropping {int(bad.sum())} row(s) with a negative quantity")
    df = df[~bad]
df.insert(0, "batch_id", a.batch_id)
df.to_csv(a.output, index=False)
print(f"inventory_counts: {n_raw} raw rows -> {len(df)} clean rows")
