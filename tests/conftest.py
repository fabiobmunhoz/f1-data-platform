from pathlib import Path
import sys

import pandas as pd
import pytest


sys.path.append(
    str(Path(__file__).resolve().parent.parent / "src")
)

from config import get_gold_path


SEASON = 2025


@pytest.fixture(scope="session")
def gold_df():

    gold_path = get_gold_path(
        SEASON,
        "fact_race_results"
    )

    return pd.read_parquet(
        gold_path
    )