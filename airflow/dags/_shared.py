"""Shared constants between batch_ingest_dag.py and data_quality_dag.py.

Airflow Datasets match on exact URI string equality between a task's outlets
and a DAG's schedule -- defining this once, rather than duplicating the string
in both DAG files, avoids a real (and easy to make) class of Airflow bug: a
typo'd URI in one file silently breaks the data-aware scheduling with no error.

Deriving the path from BatchConfig (rather than hardcoding it a second time)
means the Dataset URI and the path data_quality_dag actually reads from can
never drift apart.
"""

from airflow.datasets import Dataset

from batch_source.config import BatchConfig

BRONZE_PROJECT_PATH = str(BatchConfig.from_env().historical_bronze_output_path)

HISTORICAL_BRONZE_DATASET = Dataset(f"file://{BRONZE_PROJECT_PATH}")
