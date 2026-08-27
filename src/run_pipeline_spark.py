import subprocess
import sys
from pathlib import Path


if len(sys.argv) < 2:
    print("Uso: python src/run_pipeline_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

SRC_DIR = Path(__file__).parent

steps = [
    {
        "name": "Drivers Silver Spark",
        "script": SRC_DIR / "transformations" / "transform_drivers_spark.py"
    },
    {
        "name": "Constructors Silver Spark",
        "script": SRC_DIR / "transformations" / "transform_constructors_spark.py"
    },
    {
        "name": "Races Silver Spark",
        "script": SRC_DIR / "transformations" / "transform_races_spark.py"
    },
    {
        "name": "Results Silver Spark",
        "script": SRC_DIR / "transformations" / "transform_results_spark.py"
    },
    {
        "name": "Gold Spark",
        "script": SRC_DIR / "transformations" / "build_gold_results_spark.py"
    },
    {
        "name": "Gold Quality Check",
        "script": SRC_DIR / "quality" / "check_gold_results_spark.py"
    }
]


for step in steps:
    print()
    print("=" * 70)
    print(f"Etapa: {step['name']} | Temporada: {SEASON}")
    print("=" * 70)

    subprocess.run(
        [
            sys.executable,
            str(step["script"]),
            SEASON
        ],
        check=True
    )


print()
print("=" * 70)
print(f"✅ Pipeline Spark da temporada {SEASON} concluído!")
print("=" * 70)