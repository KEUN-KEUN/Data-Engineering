from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 19),
    "retries": 1,
}

dag = DAG(
    dag_id="mysql_to_hive_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="MySQL → Hive ETL with Spark",
)

etl_task = SparkSubmitOperator(
    task_id='run_spark_etl',
    application='/home/hadoop-hdfs-user/spark_script/mysql_to_hive.py',
    conn_id='spark_default',
    verbose=True
)
