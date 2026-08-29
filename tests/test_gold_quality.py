def test_gold_not_empty(gold_df):

    assert len(gold_df) > 0, (
        "A Gold está vazia."
    )


def test_gold_has_no_duplicate_keys(gold_df):

    duplicate_count = (
        gold_df
        .duplicated(
            subset=[
                "season",
                "round",
                "driver_id"
            ]
        )
        .sum()
    )

    assert duplicate_count == 0, (
        f"Foram encontrados {duplicate_count} registros duplicados."
    )


def test_gold_critical_columns_not_null(gold_df):

    critical_columns = [
        "season",
        "round",
        "driver_id",
        "driver_name",
        "constructor_id",
        "constructor_name",
        "race_name"
    ]

    for column in critical_columns:

        null_count = (
            gold_df[column]
            .isna()
            .sum()
        )

        assert null_count == 0, (
            f"A coluna {column} possui {null_count} valores nulos."
        )