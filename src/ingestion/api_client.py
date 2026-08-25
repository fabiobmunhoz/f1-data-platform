import requests


LIMIT = 30


def fetch_paginated_data(url, table_key, records_key):
    all_records = []
    offset = 0

    while True:
        params = {
            "limit": LIMIT,
            "offset": offset
        }

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        mrdata = data["MRData"]

        records = mrdata[table_key][records_key]

        all_records.extend(records)

        total = int(mrdata["total"])

        print(
            f"Offset {offset}: "
            f"{len(records)} registros recebidos"
        )

        if len(all_records) >= total:
            break

        offset += LIMIT

    return all_records