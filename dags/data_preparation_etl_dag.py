from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
import re
import shutil
import json

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

def extract_zip(zip_file_path, extract_to_directory):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_directory)
    print("Extraction completed.")

def copy_original_file(file_path_original, file_path_copy):
    shutil.copyfile(file_path_original, file_path_copy)
    print("File copied successfully.")

def update_json_structure(file_path):
    search_text = r"}{"
    replace_text = r"},{"

    try:
        with open(file_path, 'r+') as f:
            file_content = f.read()
            file_content = re.sub(search_text, replace_text, file_content)
            f.seek(0)
            f.write(file_content)
            f.truncate()
        print("Text replacement completed.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"No permission to access or modify '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def square_brackets(file_path):
    try:
        with open(file_path, 'r+') as f:
            original_content = f.read()
            f.seek(0)
            f.write("[" + original_content + "]")
            f.truncate()
        print("Square brackets added successfully.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"No permission to access or modify '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def pretty_print(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

        print("File pretty printing completed.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"No permission to access or modify '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Define the DAG
with DAG(
    'data_preparation_etl',
    default_args=default_args,
    description='Data Preparation for ETL Process',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define tasks
    task_extract_zip = PythonOperator(
        task_id='extract_zip',
        python_callable=extract_zip,
        op_kwargs={'zip_file_path': 'data.json.zip', 'extract_to_directory': './'}
    )

    task_copy_original_file = PythonOperator(
        task_id='copy_original_file',
        python_callable=copy_original_file,
        op_kwargs={'file_path_original': 'data.json', 'file_path_copy': 'data_copy.json'}
    )

    task_update_json_structure = PythonOperator(
        task_id='update_json_structure',
        python_callable=update_json_structure,
        op_kwargs={'file_path': 'data_copy.json'}
    )

    task_square_brackets = PythonOperator(
        task_id='square_brackets',
        python_callable=square_brackets,
        op_kwargs={'file_path': 'data_copy.json'}
    )

    task_pretty_print = PythonOperator(
        task_id='pretty_print',
        python_callable=pretty_print,
        op_kwargs={'file_path': 'data_copy.json'}
    )

    # Set task dependencies
    task_extract_zip >> task_copy_original_file >> task_update_json_structure >> task_square_brackets >> task_pretty_print
