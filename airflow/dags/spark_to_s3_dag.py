from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_to_s3_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 22),
    catchup=False,
    tags=["spark", "s3"],
) as dag:

    run_spark_to_s3 = BashOperator(
        task_id="run_spark_to_s3",
        bash_command=(
            "docker exec spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/bitnami/spark/scripts/to_s3.py"
        ),
    )
