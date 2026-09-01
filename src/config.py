import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_STORAGE = os.getenv(
    "DATA_STORAGE",
    "local"
)

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "f1-data-platform-fabio"
)


# ============================================================
# LOCAL PATHS
# ============================================================

DATA_DIR = PROJECT_ROOT / "data"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver_spark"
GOLD_DIR = DATA_DIR / "gold_spark"


def get_bronze_path(season, dataset):

    if DATA_STORAGE == "s3":
        return (
            f"s3://{S3_BUCKET}/bronze/"
            f"season={season}/"
            f"{dataset}.json"
        )

    return (
        BRONZE_DIR
        / f"season={season}"
        / f"{dataset}.json"
    )


def get_silver_path(season, dataset):

    if DATA_STORAGE == "s3":
        return (
            f"s3://{S3_BUCKET}/silver/"
            f"season={season}/"
            f"{dataset}"
        )

    return (
        SILVER_DIR
        / f"season={season}"
        / dataset
    )


def get_gold_path(season, dataset):

    if DATA_STORAGE == "s3":
        return (
            f"s3://{S3_BUCKET}/gold/"
            f"season={season}/"
            f"{dataset}"
        )

    return (
        GOLD_DIR
        / f"season={season}"
        / dataset
    )