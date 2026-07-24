"""Downloads the RetailRocket e-commerce dataset from Kaggle into batch_source/raw/.

Idempotent: skips the download if events.csv already exists locally. Requires
free Kaggle API credentials (KAGGLE_USERNAME / KAGGLE_KEY in .env -- see README
for how to generate a token).

Run with:  python -m batch_source.download_dataset
"""

from __future__ import annotations

import logging
from pathlib import Path

from batch_source.config import BatchConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("batch_source.download_dataset")

DATASET = "retailrocket/ecommerce-dataset"


def run() -> Path:
    config = BatchConfig.from_env()
    events_file = config.raw_dataset_dir / "events.csv"

    if events_file.exists():
        logger.info("Dataset already present at %s, skipping download", events_file)
        return events_file

    # Imported lazily: the kaggle package validates credentials as a side
    # effect of import, which would break anything importing this module
    # without KAGGLE_USERNAME/KAGGLE_KEY set (e.g. quality_checks.py's tests).
    from kaggle.api.kaggle_api_extended import KaggleApi

    config.raw_dataset_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s from Kaggle into %s ...", DATASET, config.raw_dataset_dir)

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=str(config.raw_dataset_dir), unzip=True)

    if not events_file.exists():
        raise FileNotFoundError(f"Expected {events_file} after download, but it's missing")

    logger.info("Download complete: %s", events_file)
    return events_file


if __name__ == "__main__":
    run()
