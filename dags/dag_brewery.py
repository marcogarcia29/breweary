
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id="brewery_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description="Brewery dag",
    default_args=default_args
) as dag:

    t1 = EmptyOperator(
        task_id='start_task',
        dag=dag
    )

    bronze_layer = BashOperator(
        task_id='run_bronze',
        bash_command= "python3 /opt/airflow/includes/extract_data.py"
    )

    silver_layer = BashOperator(
        task_id='run_silver',
        bash_command= "python3 /opt/airflow/includes/silver_layer.py"
    )

    gold_layer = BashOperator(
        task_id='run_gold',
        bash_command= "python3 /opt/airflow/includes/gold_layer.py"
    )

    t1 >> bronze_layer >> silver_layer >> gold_layer