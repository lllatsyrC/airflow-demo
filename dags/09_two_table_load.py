"""
Example 9 · Two-table load: trips, then trip points
===================================================
Two Python tasks, each building a DataFrame from a batch file and inserting it.
The second table depends on the first: trip points are joined to the trips that
task 1 inserted, read back from the database.

    load_trips >> load_trip_points

    load_trips         build_trips_df(trips file)                     -> DataFrame
                       load_df_to_table(DataFrame)                    -> trips table
    load_trip_points   read trips for this batch from the database
                       build_trip_points_df(points file, trips)       -> DataFrame
                       load_df_to_table(DataFrame)                    -> trip_points table

Why the DataFrame never leaves its task: each task runs in its own process (possibly on a
different machine), so tasks can't hand each other a DataFrame. Building and loading inside
the same task avoids that. Task 2 gets task 1's data from the database, and the only thing
passed between them is a small summary (batch ID and row count).

What it teaches
  - Python tasks (@task) that call ordinary functions, one task per table
  - Batch file locations as inputs
  - A task reading rows a previous task wrote (dependency through the database)
  - Database access through an Airflow Connection (no password in the code)
  - Checking every input file before the first insert, so a missing file can't leave
    the batch half loaded
  - Safe reruns: each load replaces the batch's rows in one transaction

Setup (once)
  1. Create the tables:  mysql -u <admin> -p < jobs/sql/create_batch_tables.sql
  2. Add an Airflow connection (as in example 6), e.g. mysql_default.
  3. Airflow's own Python needs pandas and the MySQL provider, because these
     functions run inside Airflow (unlike example 8's scripts).

Try it
  airflow dags trigger batch_load_trips --conf '{"batch_dir": "/opt/jobs/data/batches/batch_2026_11_04"}'
"""
import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
from airflow.exceptions import AirflowFailException


# ---------------------------------------------------------------------------
# The functions. These could equally be imported from an existing team module.
# pandas is imported inside them so the job file loads quickly.
# ---------------------------------------------------------------------------
def build_trips_df(path, batch_id):
    import pandas as pd
    df = pd.read_csv(path)
    n_raw = len(df)
    df["depot"] = df["depot"].str.strip()
    df["vehicle_type"] = df["vehicle_type"].str.strip().str.lower()
    df["start_time"] = pd.to_datetime(df["start_time"])
    df = df.drop_duplicates()
    bad = ~df["heading_deg"].between(0, 360)
    if bad.any():
        print(f"Dropping {int(bad.sum())} trip(s) with heading outside 0-360: {df.loc[bad, 'trip_id'].tolist()}")
        df = df[~bad]
    df.insert(0, "batch_id", batch_id)
    print(f"trips: {n_raw} raw rows -> {len(df)} clean rows")
    return df


def build_trip_points_df(path, trips, batch_id):
    import numpy as np
    import pandas as pd
    points = pd.read_csv(path)
    df = points.merge(trips, on="trip_id", how="left", validate="many_to_one")
    orphans = df["start_time"].isna()
    if orphans.any():
        print(f"Dropping {int(orphans.sum())} point(s) with no matching trip: {df.loc[orphans, 'point_id'].tolist()}")
        df = df[~orphans]
    df["distance_from_depot_km"] = np.hypot(df["x_km"], df["y_km"]).round(2)
    df.insert(0, "batch_id", batch_id)
    print(f"trip_points: {len(points)} raw rows -> {len(df)} rows")
    return df


def read_trips_from_db(conn_id, batch_id):
    import pandas as pd
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    rows = MySqlHook(mysql_conn_id=conn_id).get_records(
        "SELECT trip_id, start_time, vehicle_type FROM trips WHERE batch_id = %s", parameters=(batch_id,))
    if not rows:
        raise AirflowFailException(f"No trips in the database for batch {batch_id}")
    print(f"Read {len(rows)} trip(s) for {batch_id} from the database")
    return pd.DataFrame(rows, columns=["trip_id", "start_time", "vehicle_type"])


def load_df_to_table(df, table, columns, conn_id, batch_id):
    """Replace this batch's rows: delete, then insert, in one transaction."""
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    conn = MySqlHook(mysql_conn_id=conn_id).get_conn()
    try:
        cur = conn.cursor()
        removed = cur.execute(f"DELETE FROM {table} WHERE batch_id = %s", (batch_id,))
        cur.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
            [tuple(str(v) for v in r) for r in df[columns].itertuples(index=False)])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"{table}: removed {removed} old row(s) for {batch_id}, inserted {len(df)}")


def batch_file(params, key):
    """A file name inside the batch folder, or a full path given in the form."""
    batch_dir = os.path.abspath(os.path.expanduser(params["batch_dir"].strip()))
    name = os.path.expanduser(params[key].strip())
    path = name if os.path.isabs(name) else os.path.join(batch_dir, name)
    if not os.path.isfile(path):
        raise AirflowFailException(f"File not found: {path}")
    batch_id = params["batch_id"].strip() or os.path.basename(batch_dir.rstrip("/"))
    return path, batch_id


TRIP_COLUMNS = ["batch_id", "trip_id", "start_time", "depot", "vehicle_type", "heading_deg", "planned_km"]
POINT_COLUMNS = ["batch_id", "point_id", "trip_id", "t_min", "x_km", "y_km", "elevation_m",
                 "start_time", "vehicle_type", "distance_from_depot_km"]


@dag(
    dag_id="batch_load_trips",
    dag_display_name="Example 9 · Two-table load: trips, then trip points",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-3", "batch", "mysql"],
    doc_md=__doc__,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},  # safe: loads replace the batch
    params={
        "batch_dir": Param(type="string", minLength=1, title="Batch folder",
                           description="Folder the batch was copied into", section="Batch"),
        "trips_file": Param("trips.csv", type="string", title="Trips file",
                            description="File name inside the batch folder, or a full path", section="Batch"),
        "trip_points_file": Param("trip_points.csv", type="string", title="Trip points file",
                                  description="File name inside the batch folder, or a full path", section="Batch"),
        "batch_id": Param("", type="string", title="Batch ID",
                          description="Leave empty to use the folder name", section="Batch"),
        "mysql_conn_id": Param("mysql_default", type="string", title="Airflow connection",
                               section="Advanced"),
    },
)
def batch_load_trips():

    @task
    def load_trips(**context):
        p = context["params"]
        path, batch_id = batch_file(p, "trips_file")
        batch_file(p, "trip_points_file")   # check task 2's file too, before inserting anything
        df = build_trips_df(path, batch_id)
        load_df_to_table(df, "trips", TRIP_COLUMNS, p["mysql_conn_id"], batch_id)
        return {"batch_id": batch_id, "rows": len(df)}   # small summary only, not the DataFrame

    @task
    def load_trip_points(trips_loaded, **context):
        p = context["params"]
        path, batch_id = batch_file(p, "trip_points_file")
        trips = read_trips_from_db(p["mysql_conn_id"], trips_loaded["batch_id"])
        df = build_trip_points_df(path, trips, batch_id)
        load_df_to_table(df, "trip_points", POINT_COLUMNS, p["mysql_conn_id"], batch_id)
        return {"batch_id": batch_id, "rows": len(df)}

    # Passing load_trips' result in is what sets the order: load_trips >> load_trip_points
    load_trip_points(load_trips())


batch_load_trips()
