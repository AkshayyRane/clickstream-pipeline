"""Runs data-quality checks against the historical bronze partition. Scheduled
on the Dataset batch_ingest_dag emits -- Airflow triggers this automatically
once the batch ingest finishes writing, no second manual step needed.

Each check is its own task. A failing check surfaces as a red task in the
Airflow UI -- that *is* the quality gate, no separate pass/fail-aggregation
task required. Each task reloads the bronze DataFrame independently rather
than passing it through XCom: XCom is metadata-DB-backed and meant for small
values, not a multi-hundred-MB DataFrame.

Checks run SEQUENTIALLY (`>>` chained), not in parallel. They were originally
independent, parallel tasks -- correct-looking, but wrong at this dataset's
real scale: the full RetailRocket partition loads as a ~1.5GB DataFrame
(`df.memory_usage(deep=True)`), and four concurrent loads got OOM-killed
(`return code -9`) against the Docker VM's shared memory budget. A tiny
synthetic test fixture passed cleanly and completely missed this -- it only
surfaced against the real ~2.75M-row dataset. Chaining the checks bounds peak
memory to roughly one DataFrame load at a time, trading some wall-clock time
for reliability.
"""

from __future__ import annotations

import pendulum
from _shared import BRONZE_PROJECT_PATH, HISTORICAL_BRONZE_DATASET
from airflow.decorators import dag, task


def _run_check(check_name: str) -> None:
    import batch_source.quality_checks as qc

    df = qc.load_bronze_events(BRONZE_PROJECT_PATH)
    getattr(qc, check_name)(df)


@dag(
    dag_id="data_quality_dag",
    description="Quality-check the historical bronze partition after batch_ingest_dag writes it",
    schedule=[HISTORICAL_BRONZE_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "quality"],
)
def data_quality_dag():
    @task
    def check_row_count_positive():
        _run_check("check_row_count_positive")

    @task
    def check_required_fields_not_null():
        _run_check("check_required_fields_not_null")

    @task
    def check_event_type_values_valid():
        _run_check("check_event_type_values_valid")

    @task
    def check_no_duplicate_event_ids():
        _run_check("check_no_duplicate_event_ids")

    (
        check_row_count_positive()
        >> check_required_fields_not_null()
        >> check_event_type_values_valid()
        >> check_no_duplicate_event_ids()
    )


data_quality_dag()
