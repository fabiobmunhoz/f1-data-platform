from pathlib import Path
import sys

import pyarrow.parquet as pq


sys.path.append(
    str(Path(__file__).resolve().parent.parent / "src")
)

from config import get_gold_path


SEASON = 2025


def test_gold_has_expected_columns():

    gold_path = get_gold_path(
        SEASON,
        "fact_race_results"
    )

    parquet_file = Path(gold_path)

    files = list(
        parquet_file.glob("*.parquet")
    )

    assert len(files) > 0, (
        f"Nenhum arquivo parquet encontrado em {gold_path}"
    )

    table = pq.read_table(
        files[0]
    )

    columns = table.column_names

    expected_columns = [
        "season",
        "round",
        "race_name",
        "race_date",
        "circuit_id",
        "circuit_name",
        "country",
        "driver_id",
        "driver_name",
        "driver_nationality",
        "constructor_id",
        "constructor_name",
        "grid",
        "position",
        "position_text",
        "points",
        "laps",
        "status",
        "fastest_lap_rank"
    ]

    for column in expected_columns:
        assert column in columns, (
            f"Coluna esperada não encontrada: {column}"
        )