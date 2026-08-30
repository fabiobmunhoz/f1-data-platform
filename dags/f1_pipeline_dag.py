from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "fabio",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="f1_data_pipeline",
    description="Pipeline de dados de Formula 1 com PySpark",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["f1", "spark", "data-engineering"],
) as dag:

    transform_drivers = BashOperator(
        task_id="transform_drivers",
        bash_command="python /opt/airflow/project/src/transformations/transform_drivers_spark.py 2025",
    )

    transform_constructors = BashOperator(
        task_id="transform_constructors",
        bash_command="python /opt/airflow/project/src/transformations/transform_constructors_spark.py 2025",
    )

    transform_races = BashOperator(
        task_id="transform_races",
        bash_command="python /opt/airflow/project/src/transformations/transform_races_spark.py 2025",
    )

    transform_results = BashOperator(
        task_id="transform_results",
        bash_command="python /opt/airflow/project/src/transformations/transform_results_spark.py 2025",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python /opt/airflow/project/src/transformations/build_gold_results_spark.py 2025",
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="python /opt/airflow/project/src/quality/check_gold_results_spark.py 2025",
    )


    [
        transform_drivers,
        transform_constructors,
        transform_races,
        transform_results,
    ] >> build_gold >> quality_check