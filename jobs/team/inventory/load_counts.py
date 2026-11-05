"""Step 3 (optional): insert the cleaned counts into the database.

Replaces this batch's rows (delete, then insert, in one transaction), so loading the
same batch twice never creates duplicates.
    python load_counts.py --input counts_clean.csv --batch-id B1 --db-config ~/team_db.cnf
"""
import argparse

import pandas as pd
import pymysql

TABLE = "inventory_counts"
COLUMNS = ["batch_id", "store", "sku", "counted_at", "quantity"]

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--batch-id", required=True)
ap.add_argument("--db-config", required=True, help="MySQL option file with [client] host/user/password/database")
a = ap.parse_args()

df = pd.read_csv(a.input)[COLUMNS]
conn = pymysql.connect(read_default_file=a.db_config)
try:
    with conn.cursor() as cur:
        removed = cur.execute(f"DELETE FROM {TABLE} WHERE batch_id = %s", (a.batch_id,))
        cur.executemany(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})",
            [tuple(str(v) for v in r) for r in df.itertuples(index=False)])
    conn.commit()
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()
print(f"{TABLE}: removed {removed} old row(s) for {a.batch_id}, inserted {len(df)}")
