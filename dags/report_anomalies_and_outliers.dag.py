from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import warnings
import datapane as dp

# Suppress specific FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning, module='datapane.common.df_processor')

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'anomalies_and_outliers_report',
    default_args=default_args,
    description='Generate report for anomalies and outliers in driver performance metrics',
    schedule_interval='@daily',
)

# Functions from the notebook
def install_packages():
    import subprocess
    subprocess.check_call(['pip', 'install', 'datapane'])

def prepare_data():
    # Load the data from the pickle file
    df = pd.read_pickle('data2.pkl')

    # Calculate the 99th and 1st percentiles for each metric column
    metrics = [
        'driver_endurance_score', 'driver_profitabilty_score',
        'driver_safety_adherence_score', 'driving_efficiency_score'
    ]
    percentile_99 = df[metrics].quantile(0.99)
    percentile_01 = df[metrics].quantile(0.01)

    # Filter rows where is_outlier is True
    anomalies_df = df[df['is_outlier']].copy()

    # Perform sanity check
    sanity_check_results = sanity_check(anomalies_df, metrics, percentile_99, percentile_01)
    anomalies_df.loc[:, 'sanity_check'] = sanity_check_results

    # Add the 99th and 1st percentiles to the anomalies_df
    for metric in metrics:
        anomalies_df.loc[:, f'{metric}_99th_percentile'] = percentile_99[metric]
        anomalies_df.loc[:, f'{metric}_1st_percentile'] = percentile_01[metric]

    # Rename the columns
    rename_dict = {
        'driver_id': 'Driver ID',
        'full_name': 'Full Name',
        'driver_endurance_score': 'Endurance Score',
        'driver_profitabilty_score': 'Profitability Score',
        'driver_safety_adherence_score': 'Safety Adherence Score',
        'driving_efficiency_score': 'Efficiency Score',
        'driver_endurance_score_99th_percentile': 'Endurance Score 99th Percentile',
        'driver_profitabilty_score_99th_percentile': 'Profitability Score 99th Percentile',
        'driver_safety_adherence_score_99th_percentile': 'Safety Adherence Score 99th Percentile',
        'driving_efficiency_score_99th_percentile': 'Efficiency Score 99th Percentile',
        'driver_endurance_score_1st_percentile': 'Endurance Score 1st Percentile',
        'driver_profitabilty_score_1st_percentile': 'Profitability Score 1st Percentile',
        'driver_safety_adherence_score_1st_percentile': 'Safety Adherence Score 1st Percentile',
        'driving_efficiency_score_1st_percentile': 'Efficiency Score 1st Percentile',
        'sanity_check': 'Sanity Check'
    }
    anomalies_df.rename(columns=rename_dict, inplace=True)

    # Update the percentile dictionaries with new column names
    new_percentile_99 = percentile_99.rename(index=rename_dict)
    new_percentile_01 = percentile_01.rename(index=rename_dict)

    # Select the specified columns along with the percentiles
    new_metrics = [
        'Endurance Score', 'Profitability Score',
        'Safety Adherence Score', 'Efficiency Score'
    ]
    columns_to_display = [
        'Driver ID', 'Full Name'
    ] + new_metrics + [
        f'{metric} 99th Percentile' for metric in new_metrics
    ] + [
        f'{metric} 1st Percentile' for metric in new_metrics
    ] + ['Sanity Check']

    anomalies_df = anomalies_df[columns_to_display]

    # Apply the highlight function only to the metric columns
    styled_anomalies_df = anomalies_df.style.apply(
        highlight_anomalies,
        subset=new_metrics,
        percentile_99=new_percentile_99,
        percentile_01=new_percentile_01
    )

    # Save the styled DataFrame as HTML
    styled_html = styled_anomalies_df.to_html()
    description = """
    # 2b. Anomalies and Outliers Report
    
    ## Overview
    This report displays anomalies in driver performance metrics.
    
    ## Columns Included
    - **Driver ID**: Identifier for the driver.
    - **Full Name**: Name of the driver.
    - **Endurance Score**: Score indicating the driver's endurance.
    - **Profitability Score**: Score indicating the driver's profitability.
    - **Safety Adherence Score**: Score indicating the driver's safety adherence.
    - **Efficiency Score**: Score indicating the driver's efficiency.
    - **Endurance Score 99th Percentile**: 99th percentile value for the endurance score.
    - **Profitability Score 99th Percentile**: 99th percentile value for the profitability score.
    - **Safety Adherence Score 99th Percentile**: 99th percentile value for the safety adherence score.
    - **Efficiency Score 99th Percentile**: 99th percentile value for the efficiency score.
    - **Endurance Score 1st Percentile**: 1st percentile value for the endurance score.
    - **Profitability Score 1st Percentile**: 1st percentile value for the profitability score.
    - **Safety Adherence Score 1st Percentile**: 1st percentile value for the safety adherence score.
    - **Efficiency Score 1st Percentile**: 1st percentile value for the efficiency score.
    - **Sanity Check**: Indicates whether the anomaly is confirmed based on percentile thresholds.
    
    ## How to Use
    1. **Identify Anomalies**: Look for rows where the "Sanity Check" column is marked as True. These rows represent anomalies in the driver performance metrics.
    2. **Analyze Metric Scores**: Focus on metrics with highlighted cells (background color yellow). These cells indicate that the corresponding metric value falls outside the normal range (outside 1st or 99th percentile).
    3. **Compare with Percentiles**: Compare the metric values with their respective 1st and 99th percentile values provided in the report. This helps in understanding how far the metric values deviate from the normal range.
    
    By following these steps, users can effectively identify and investigate anomalies and outliers in driver performance metrics.
    
    > **_NOTE:_** The sanity check should ideally be true for all entries in this report. However, in future iterations of this report, it will be possible to toggle the percentile values used for analyzing the data. Users will be able to customize both the percentile value that defines the normal range and the percentile value that identifies an anomaly. This feature will be particularly useful for examining extreme values that fall within the required percentile range but still may be of interest for further investigation.
    
    """
    report = dp.Report(
        dp.Text(description),
        dp.HTML(styled_html)
    )
    report.save(path='2b_report_anomalies_and_outliers.html')

# Define the sanity check function
def sanity_check(df, metrics, percentile_99, percentile_01):
    anomalies_confirmed = []
    for index, row in df.iterrows():
        is_anomaly = False
        for metric in metrics:
            if row[metric] > percentile_99[metric] or row[metric] < percentile_01[metric]:
                is_anomaly = True
                break
        anomalies_confirmed.append(is_anomaly)
    return anomalies_confirmed

# Define the highlight function
def highlight_anomalies(s, percentile_99, percentile_01):
    new_metrics = [
        'Endurance Score', 'Profitability Score',
        'Safety Adherence Score', 'Efficiency Score'
    ]
    if s.name in new_metrics:
        return [
            'background-color: yellow' if (v > percentile_99.get(s.name, float('inf'))
                                            or v < percentile_01.get(s.name, float('-inf')))
            else ''
            for v in s
        ]
    return ['' for _ in s]

# Task to install packages
install_packages_task = PythonOperator(
    task_id='install_packages',
    python_callable=install_packages,
    dag=dag,
)

# Task to prepare the data and generate the report
prepare_data_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag,
)

# Set task dependencies
install_packages_task >> prepare_data_task
