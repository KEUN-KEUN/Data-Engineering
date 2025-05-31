from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'eddi',
    'start_date': datetime(2025, 5, 19),
    'retries': 1,
}

with DAG(
    dag_id='spark_mysql_mirror_wsl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_submit',
        bash_command="""
        /home/eddi/spark/bin/spark-submit \
        --master spark://172.20.113.119:7077 \
        --jars /home/eddi/jars/mysql-connector-java-8.0.33.jar \
        /home/eddi/projects/mysql_mirror.py
        """
    )
