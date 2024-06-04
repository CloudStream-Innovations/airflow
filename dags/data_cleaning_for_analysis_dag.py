from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'data_cleaning_for_analysis',
    default_args=default_args,
    description='DAG for cleaning taxi data for analysis',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Function to load data
    def load_data(**kwargs):
        df = pd.read_pickle('/temp/input/data.pkl')
        kwargs['ti'].xcom_push(key='dataframe', value=df)
    
    # Function to handle empty fields
    def handle_empty_fields(df):
        empty_columns = df.columns[df.isnull().any()]
        df = df.fillna(np.nan)
        return df
    
    # Function to remove duplicates
    def remove_duplicates(df):
        df = df.drop_duplicates()
        return df
    
    # Function to convert data types
    def convert_data_types(df):
        data_types = {
            'taxi_group_name': 'object',
            'rank': 'int64',
            'first_name': 'object',
            'last_name': 'object',
            'vehicle_brand': 'int64',
            'driver_experience_group': 'int64',
            'special_achievements_awarded': 'bool',
            'driver_endurance_score': 'float64',
            'driver_profitabilty_score': 'float64',
            'driver_safety_adherence_score': 'float64',
            'driving_efficiency_score': 'float64',
            'number_of_1_star_ratings': 'int64',
            'number_of_2_star_ratings': 'int64',
            'number_of_3_star_ratings': 'int64',
            'number_of_4_star_ratings': 'int64',
            'number_of_5_star_ratings': 'int64',
            'driver_id': 'object',
            'disabled': 'bool',
            'deleted': 'bool'
        }
        for col, dtype in data_types.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        return df
    
    # Function to replace negative values
    def replace_negative_values(df):
        columns = ['number_of_1_star_ratings', 'number_of_2_star_ratings', 'number_of_3_star_ratings', 'number_of_4_star_ratings', 'number_of_5_star_ratings']
        for column in columns:
            negative_indices = df.index[df[column] < 0]
            df.loc[negative_indices, column] = df.loc[negative_indices, column].interpolate(method='linear')
        return df
    
    # Function to flag outliers
    def flag_outliers(df):
        include_columns = ['driver_endurance_score', 'driver_profitabilty_score', 'driver_safety_adherence_score', 'driving_efficiency_score']
        threshold = 0.99
        df['is_outlier'] = False
        for column in include_columns:
            percentile_value = df[column].quantile(threshold)
            df.loc[df[column] > percentile_value, 'is_outlier'] = True
        return df
    
    # Function to handle rows with numbers in names
    def handle_rows_with_numbers(df):
        df['first_name'] = df['first_name'].str.replace(r'\d', '')
        df['last_name'] = df['last_name'].str.replace(r'\d', '')
        return df
    
    # Function to create full name column
    def create_full_name_column(df):
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        df.drop(columns=['first_name', 'last_name'], inplace=True)
        return df
    
    # Function to reorder columns
    def reorder_columns(df):
        desired_columns_order = [
            'taxi_group_name', 'rank', 'full_name', 'vehicle_brand', 'driver_experience_group',
            'special_achievements_awarded', 'driver_endurance_score', 'driver_profitabilty_score',
            'driver_safety_adherence_score', 'driving_efficiency_score',
            'number_of_1_star_ratings', 'number_of_2_star_ratings', 'number_of_3_star_ratings',
            'number_of_4_star_ratings', 'number_of_5_star_ratings'
        ]
        df = df.reindex(columns=desired_columns_order)
        return df
    
    # Function to rename columns
    def rename_columns(df):
        column_mapping = {
            'taxi_group_name': 'Taxi Group Name',
            'rank': 'Taxi Division Rank',
            'full_name': 'Driver Full Name',
            'vehicle_brand': 'Vehicle Brand',
            'driver_experience_group': 'Experience Group',
            'special_achievements_awarded': 'Special Awards',
            'driver_endurance_score': 'Endurance Score',
            'driver_profitabilty_score': 'Profitability Score',
            'driver_safety_adherence_score': 'Safety Score',
            'driving_efficiency_score': 'Efficiency Score',
            'number_of_1_star_ratings': '#of1Star Ratings',
            'number_of_2_star_ratings': '#of2Star Ratings',
            'number_of_3_star_ratings': '#of3Star Ratings',
            'number_of_4_star_ratings': '#of4Star Ratings',
            'number_of_5_star_ratings': '#of5Star Ratings'
        }
        df = df.rename(columns=column_mapping)
        return df
    
    # Function to save data
    def save_data(**kwargs):
        df = kwargs['ti'].xcom_pull(key='dataframe')
        df.to_pickle('/temp/input/data3.pkl')
        df.to_csv('/temp/output/output.csv', index=False)
    
    # Define the tasks
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    
    task_handle_empty_fields = PythonOperator(
        task_id='handle_empty_fields',
        python_callable=lambda **kwargs: handle_empty_fields(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_remove_duplicates = PythonOperator(
        task_id='remove_duplicates',
        python_callable=lambda **kwargs: remove_duplicates(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_convert_data_types = PythonOperator(
        task_id='convert_data_types',
        python_callable=lambda **kwargs: convert_data_types(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_replace_negative_values = PythonOperator(
        task_id='replace_negative_values',
        python_callable=lambda **kwargs: replace_negative_values(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_flag_outliers = PythonOperator(
        task_id='flag_outliers',
        python_callable=lambda **kwargs: flag_outliers(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_handle_rows_with_numbers = PythonOperator(
        task_id='handle_rows_with_numbers',
        python_callable=lambda **kwargs: handle_rows_with_numbers(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_create_full_name_column = PythonOperator(
        task_id='create_full_name_column',
        python_callable=lambda **kwargs: create_full_name_column(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_reorder_columns = PythonOperator(
        task_id='reorder_columns',
        python_callable=lambda **kwargs: reorder_columns(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_rename_columns = PythonOperator(
        task_id='rename_columns',
        python_callable=lambda **kwargs: rename_columns(kwargs['ti'].xcom_pull(key='dataframe')),
        provide_context=True
    )
    
    task_save_data = PythonOperator(
        task_id='save_data',
        python_callable=save_data,
        provide_context=True
    )
    
    # Set task dependencies
    task_load_data >> task_handle_empty_fields >> task_remove_duplicates >> task_convert_data_types
    task_convert_data_types >> task_replace_negative_values >> task_flag_outliers >> task_handle_rows_with_numbers
    task_handle_rows_with_numbers >> task_create_full_name_column >> task_reorder_columns >> task_rename_columns
    task_rename_columns >> task_save_data
