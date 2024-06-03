from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def process_file():
    
    print("Processing file data.json.zip")

with DAG('process_data_json_zip', 
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')

    # File sensor to monitor the folder for the file
    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath='/opt/airflow/input/data.json.zip',  
        poke_interval=10,  # Check every 10 seconds for the file
        timeout=600,  # Timeout after 10 minutes if file is not found
    )

    process_file_task = PythonOperator(
        task_id='process_file_task',
        python_callable=process_file
    )

    trigger_data_preparation = TriggerDagRunOperator(
        task_id='trigger_data_preparation',
        trigger_dag_id='data_preparation_for_etl',
        dag=dag
    )

    # Define task dependencies
    start_task >> file_sensor_task >> process_file_task >> trigger_data_preparation
