import json
import sys
from pathlib import Path
from api_client import fetch_paginated_data




if len(sys.argv) < 2:
    print("Uso: python <script>.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

url = f"https://api.jolpi.ca/ergast/f1/{SEASON}/constructors/"

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

output_path = Path(
    f"data/bronze/season={SEASON}/constructors.json"
)

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    output_path,
    "w",
    encoding="utf-8"
) as file:
    json.dump(
        final_data,
        file,
        ensure_ascii=False,
        indent=4
    )

print()
print(f"Total salvo: {len(constructors)} construtores")
print(f"Arquivo salvo em: {output_path}")