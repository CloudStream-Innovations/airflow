from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import json
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function definitions

def extract_data(input, taxi_group_name):
    result = []
    rank = input["metadata"]["rank"]
    drivers = input["metadata"]["employee_data"]["driver_details"]

    for driver in drivers:
        row = [
            taxi_group_name,
            rank,
            driver["driver_profile"]["firstName"],
            driver["driver_profile"]["lastName"],
            driver["driver_metrics"]["vehicle_brand"],
            driver["driver_metrics"]["driver_experience_group"],
            driver["driver_metrics"]["special_achievements_awarded"],
            driver["driver_metrics"]["driver_endurance_score"],
            driver["driver_metrics"]["driver_profitabilty_score"],
            driver["driver_metrics"]["driver_safety_adherence_score"],
            driver["driver_metrics"]["driving_efficiency_score"],
            driver["driver_metrics"]["Number_of_1_star_ratings"],
            driver["driver_metrics"]["Number_of_2_star_ratings"],
            driver["driver_metrics"]["Number_of_3_star_ratings"],
            driver["driver_metrics"]["Number_of_4_star_ratings"],
            driver["driver_metrics"]["Number_of_5_star_ratings"],
            driver["driver_id"],
            driver["disabled"],
            driver["deleted"]
        ]
        result.append(row)

    if "nodes" in input:
        for node in input["nodes"]:
            subresults = extract_data(node, taxi_group_name)
            for subresult in subresults:
                result.append(subresult)

    return result

def extract_taxi_groups(file_path_copy):
    try:
        with open(file_path_copy, 'r') as f:
            data = json.load(f)
            taxi_groups = [taxi_group for taxi_group in data]
        return taxi_groups
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
        return None

def process_taxi_groups(taxi_groups):
    rows = []
    for taxi_group in taxi_groups:
        columns = [taxi_group["taxi_group_name"], taxi_group["taxi_org_data"]["depot_data"]]
        rows.append(columns)

    result = []
    for row in rows:
        extracts = extract_data(row[1]["root"], row[0])
        if extracts is not None:
            for extract in extracts:
                result.append(extract)
        else:
            print(f"Error processing taxi group '{row[0]}'")

    return result

def create_dataframe(result, column_names):
    return pd.DataFrame(result, columns=column_names)

def process_taxi_data():
    file_path_copy = "/temp/input/data_copy.json"
    taxi_groups = extract_taxi_groups(file_path_copy)
    if taxi_groups:
        result = process_taxi_groups(taxi_groups)
        column_names = [
            'taxi_group_name',
            'rank',
            'first_name',
            'last_name',
            'vehicle_brand',
            'driver_experience_group',
            'special_achievements_awarded',
            'driver_endurance_score',
            'driver_profitabilty_score',
            'driver_safety_adherence_score',
            'driving_efficiency_score',
            'number_of_1_star_ratings',
            'number_of_2_star_ratings',
            'number_of_3_star_ratings',
            'number_of_4_star_ratings',
            'number_of_5_star_ratings',
            'driver_id',
            'disabled',
            'deleted'
        ]
        df = create_dataframe(result, column_names)
        df.to_pickle('/temp/input/data2.pkl')
    else:
        print("Error extracting taxi groups.")

# Define the DAG
with DAG(
    'taxi_data_processing',
    default_args=default_args,
    description='Process taxi data after preparation',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    task_process_taxi_data = PythonOperator(
        task_id='process_taxi_data',
        python_callable=process_taxi_data
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_next_dag',
        trigger_dag_id='next_dag_id',  # TODO: Replace 'next_dag_id' with the actual DAG ID of the next DAG in the pipeline
    )

    task_process_taxi_data >> trigger_next_dag
