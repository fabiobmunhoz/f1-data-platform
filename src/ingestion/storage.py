import json
from pathlib import Path

import boto3

from config import DATA_STORAGE


def save_json(data, output_path):

    if DATA_STORAGE == "s3":

        s3 = boto3.client("s3")

        path = str(output_path)

        if not path.startswith("s3://"):
            raise ValueError(
                f"Caminho S3 inválido: {path}"
            )

        path_without_prefix = path.replace(
            "s3://",
            "",
            1
        )

        bucket, key = path_without_prefix.split(
            "/",
            1
        )

        body = json.dumps(
            data,
            ensure_ascii=False,
            indent=4
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        return


    output_path = Path(output_path)

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
            data,
            file,
            ensure_ascii=False,
            indent=4
        )