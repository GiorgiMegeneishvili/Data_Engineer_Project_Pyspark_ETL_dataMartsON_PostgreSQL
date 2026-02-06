'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl import Pipline

with DAG(
    dag_id='medallion_etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    bronze = PythonOperator(
        task_id='bronze_layer',
        python_callable=Pipline.bronze_layer
    )

    silver = PythonOperator(
        task_id='silver_layer',
        python_callable=Pipline.silver_layer
    )

    gold = PythonOperator(
        task_id='gold_layer',
        python_callable=Pipline.gold_layer
    )

    bronze >> silver >> gold
'''
