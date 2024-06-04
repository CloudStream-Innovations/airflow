from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'top_10_active_drivers_report',
    default_args=default_args,
    description='A DAG to generate a report showcasing the top 10 active drivers.',
    schedule_interval='@daily',
)

# Dummy data for the sake of example
dummy_data = {
    'driver_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'deleted': [False]*10,
    'disabled': [False]*10,
    'driver_endurance_score': np.random.rand(10),
    'driver_profitabilty_score': np.random.rand(10),
    'driver_safety_adherence_score': np.random.rand(10),
    'driving_efficiency_score': np.random.rand(10),
    'special_achievements_awarded': [False, True, False, True, False, False, True, False, False, True],
    'number_of_1_star_ratings': np.random.randint(0, 5, size=10),
    'number_of_2_star_ratings': np.random.randint(0, 5, size=10),
    'number_of_3_star_ratings': np.random.randint(0, 5, size=10),
    'number_of_4_star_ratings': np.random.randint(0, 5, size=10),
    'number_of_5_star_ratings': np.random.randint(0, 5, size=10),
    'driver_experience_group': np.random.randint(1, 10, size=10)
}
df = pd.DataFrame(dummy_data)

def filter_active_drivers(df):
    return df[(df['deleted'] == False) & (df['disabled'] == False)]

def calculate_combined_score(df):
    operational_score = (
        (df['driver_endurance_score'] * 20 +
         df['driver_profitabilty_score'] * 40 +
         df['driver_safety_adherence_score'] * 50 +
         df['driving_efficiency_score'] * 30) / 100 )

    recognition_score = df['special_achievements_awarded'].apply(lambda x: 100 if x else 0)

    customer_satisfaction_score = (
        (df['number_of_1_star_ratings'] * 0.2 +
        df['number_of_2_star_ratings'] * 0.4 +
        df['number_of_3_star_ratings'] * 0.6 +
        df['number_of_4_star_ratings'] * 0.8 +
        df['number_of_5_star_ratings'] * 1.0)
        /
        (df['number_of_1_star_ratings'] +
          df['number_of_2_star_ratings'] +
          df['number_of_3_star_ratings'] +
          df['number_of_4_star_ratings'] +
          df['number_of_5_star_ratings'])
        * 100
    )
    customer_satisfaction_score = customer_satisfaction_score.clip(lower=0, upper=100)

    experience_score = (df['driver_experience_group']) * 10

    df['combined_score'] = (operational_score + recognition_score + customer_satisfaction_score + experience_score) / 4

    df['operational_score'] = operational_score
    df['recognition_score'] = recognition_score
    df['customer_satisfaction_score'] = customer_satisfaction_score
    df['experience_score'] = experience_score

    return df

def get_top_n_drivers(df, n, sort_by):
    return df.sort_values(by=sort_by, ascending=False).head(n)

def plot_driver_scores_all_pillars(top_10_df, pillars):
    top_10_df = top_10_df.sort_values(by='combined_score', ascending=False)
    bar_width = 0.2
    gap = 0.2
    index = np.arange(len(top_10_df)) * (len(pillars) + 1) * (bar_width + gap)
    fig, ax = plt.subplots(figsize=(14, 8))

    colors = np.random.rand(len(pillars) + 1, 3)

    for i, pillar in enumerate(pillars):
        ax.bar(index + bar_width * i, top_10_df[pillar], bar_width, label=pillar, color=colors[i])

        for j, value in enumerate(top_10_df[pillar]):
            ax.text(index[j] + bar_width * i, value + 0.1, str(round(value, 2)), ha='center', va='bottom')

    combined_score_values = top_10_df['combined_score']
    combined_score_labels = [f'{round(value, 2)}' for value in combined_score_values]

    combined_bars = ax.bar(index + bar_width * len(pillars), combined_score_values, bar_width, label='Combined Score', color=colors[-1])

    for bar, label in zip(combined_bars, combined_score_labels):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height + 0.1, label, ha='center', va='bottom', color='black')

    ax.set_xlabel('Drivers')
    ax.set_ylabel('Scores (%)')
    ax.set_title('Top 10 Active Drivers by Combined Score and the Pillars of Excellence')
    ax.set_xticks(index + bar_width * len(pillars) / 2)
    ax.set_xticklabels(top_10_df['driver_id'], rotation=45, ha='right')

    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join("/tmp", 'top_10_drivers_scores.png'))
    plt.close()

# Define the tasks
def task_filter_active_drivers():
    global df
    df = filter_active_drivers(df)

def task_calculate_combined_score():
    global df
    df = calculate_combined_score(df)

def task_get_top_n_drivers():
    global top_10_df
    top_10_df = get_top_n_drivers(df, 10, 'combined_score')

def task_plot_driver_scores_all_pillars():
    pillars = ['operational_score', 'recognition_score', 'customer_satisfaction_score', 'experience_score']
    plot_driver_scores_all_pillars(top_10_df, pillars)

# Creating tasks
t1 = PythonOperator(
    task_id='filter_active_drivers',
    python_callable=task_filter_active_drivers,
    dag=dag,
)

t2 = PythonOperator(
    task_id='calculate_combined_score',
    python_callable=task_calculate_combined_score,
    dag=dag,
)

t3 = PythonOperator(
    task_id='get_top_n_drivers',
    python_callable=task_get_top_n_drivers,
    dag=dag,
)

t4 = PythonOperator(
    task_id='plot_driver_scores_all_pillars',
    python_callable=task_plot_driver_scores_all_pillars,
    dag=dag,
)

# Defining task dependencies
t1 >> t2 >> t3 >> t4
