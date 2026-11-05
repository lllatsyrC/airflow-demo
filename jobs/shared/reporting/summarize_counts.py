"""Step 2: total the cleaned counts by store. Lives in a shared reporting folder.
    python summarize_counts.py --input counts_clean.csv --output counts_by_store.csv --batch-id B1
"""
import argparse

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--batch-id", required=True)
a = ap.parse_args()

df = pd.read_csv(a.input)
out = (df.groupby("store", as_index=False)
         .agg(skus=("sku", "nunique"), total_quantity=("quantity", "sum"))
         .sort_values("total_quantity", ascending=False))
out.insert(0, "batch_id", a.batch_id)
out.to_csv(a.output, index=False)
print(out.to_string(index=False))
