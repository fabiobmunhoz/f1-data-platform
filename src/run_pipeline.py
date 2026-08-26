import subprocess
import sys
from pathlib import Path


if len(sys.argv) < 2:
    print("Uso: python src/run_pipeline.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

SRC_DIR = Path(__file__).parent

pipeline_steps = [
    {
        "name": "Ingestão Bronze",
        "script": SRC_DIR / "ingestion" / "run_season.py"
    },
    {
        "name": "Transformações Silver",
        "script": SRC_DIR / "transformations" / "run_transformations.py"
    },
    {
        "name": "Validações de Qualidade",
        "script": SRC_DIR / "quality" / "run_quality_checks.py"
    }
]


for step in pipeline_steps:
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
print(f"✅ Pipeline da temporada {SEASON} concluído com sucesso!")
print("=" * 70)