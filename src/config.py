from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver_spark"
GOLD_DIR = DATA_DIR / "gold_spark"


def get_bronze_path(season, dataset):
    return (
        BRONZE_DIR
        / f"season={season}"
        / f"{dataset}.json"
    )


def get_silver_path(season, dataset):
    return (
        SILVER_DIR
        / f"season={season}"
        / dataset
    )


def get_gold_path(season, dataset):
    return (
        GOLD_DIR
        / f"season={season}"
        / dataset
    )