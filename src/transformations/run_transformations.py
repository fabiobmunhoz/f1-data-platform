import subprocess
import sys
from pathlib import Path


if len(sys.argv) < 2:
    print("Uso: python run_transformations.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

SCRIPT_DIR = Path(__file__).parent

scripts = [
    "transform_drivers.py",
    "transform_constructors.py",
]


for script in scripts:
    script_path = SCRIPT_DIR / script

    print()
    print("=" * 60)
    print(f"Executando: {script} | Temporada: {SEASON}")
    print("=" * 60)

    subprocess.run(
        [
            sys.executable,
            str(script_path),
            SEASON
        ],
        check=True
    )


print()
print("=" * 60)
print(f"Transformações da temporada {SEASON} concluídas!")
print("=" * 60)