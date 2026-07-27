"""Batch leg: downloads the RetailRocket historical dataset and transforms it
into the bronze layer, conforming to the same event schema the streaming path
uses (simulator/schemas.py).

Manually triggered, not scheduled -- this is a one-time historical backfill of
a static public dataset, not a recurring feed, so a fake daily cron would be
dishonest. Re-triggering is safe: both steps are idempotent (download skips if
already present; the transform overwrites deterministic output files rather
than accumulating duplicates -- see batch_source/transform_to_bronze.py).

The final task declares a Dataset outlet, which data_quality_dag.py is
scheduled on -- Airflow's data-aware scheduling triggers the quality checks
automatically once this DAG finishes writing, no second manual trigger needed.

Imports of batch_source modules are kept inside the task callables, not at
DAG-file top level -- the scheduler/webserver re-parse every DAG file
repeatedly, and pandas/kaggle are too heavy to import on every parse cycle.
"""

from __future__ import annotations

import pendulum
from _shared import HISTORICAL_BRONZE_DATASET
from airflow.decorators import dag, task


@dag(
    dag_id="batch_ingest_dag",
    description="Download the RetailRocket historical dataset and land it in the bronze layer",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "bronze"],
)
def batch_ingest_dag():
    @task
    def download_dataset():
        from batch_source.download_dataset import run

        run()

    @task(outlets=[HISTORICAL_BRONZE_DATASET])
    def transform_to_bronze():
        from batch_source.transform_to_bronze import run

        run()

    download_dataset() >> transform_to_bronze()


batch_ingest_dag()
