import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from api_client import fetch_paginated_data
from config import get_bronze_path
from storage import save_json


if len(sys.argv) < 2:
    print("Uso: python ingest_constructors.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]

url = (
    f"https://api.jolpi.ca/ergast/f1/"
    f"{SEASON}/constructors/"
)


constructors = fetch_paginated_data(
    url=url,
    table_key="ConstructorTable",
    records_key="Constructors"
)


final_data = {
    "season": SEASON,
    "total": len(constructors),
    "constructors": constructors
}


output_path = get_bronze_path(
    SEASON,
    "constructors"
)


save_json(
    final_data,
    output_path
)


print()
print(
    f"Total salvo: {len(constructors)} construtores"
)
print(
    f"Arquivo salvo em: {output_path}"
)