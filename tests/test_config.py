import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent / "src")
)

from config import (
    get_bronze_path,
    get_silver_path,
    get_gold_path
)


def test_bronze_path():

    path = get_bronze_path(
        2025,
        "drivers"
    )

    assert str(path).endswith(
        "data\\bronze\\season=2025\\drivers.json"
    )


def test_silver_path():

    path = get_silver_path(
        2025,
        "drivers"
    )

    assert str(path).endswith(
        "data\\silver_spark\\season=2025\\drivers"
    )


def test_gold_path():

    path = get_gold_path(
        2025,
        "fact_race_results"
    )

    assert str(path).endswith(
        "data\\gold_spark\\season=2025\\fact_race_results"
    )
    
