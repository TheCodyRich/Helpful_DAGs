from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
from datetime import datetime

# Configuration
BASE_URL = "<INSERT_BASE_URL>/api/v1/" #Change this
CSV_FILE_PATH = '<INSERT_CSV_FILE_PATH>>' #Change this
TOKEN = "<INSERT_DEPLOYMENT_TOKEN>" #Change this
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def airflow_request(url):
    return requests.get(
        url,
        headers=HEADERS
    )

def get_dags_info():
    # Get list of all DAGs
    url = f'{BASE_URL}dags'
    response = airflow_request(url)
    response.raise_for_status()
    dags = response.json()['dags']

    # Prepare data for CSV
    dag_info_list = []
    for dag in dags:
        dag_id = dag['dag_id']
        dag_status = dag['is_paused']

        # Get tasks for each DAG
        tasks_url = f'{BASE_URL}dags/{dag_id}/tasks'
        tasks_response = airflow_request(tasks_url)
        print(tasks_response)
        tasks_response.raise_for_status()
        tasks = tasks_response.json()['tasks']
        task_ids = [task['task_id'] for task in tasks]

        # Get last DAG run info
        dag_runs_url = f'{BASE_URL}dags/{dag_id}/dagRuns'
        dag_runs_response = airflow_request(dag_runs_url)
        dag_runs_response.raise_for_status()
        dag_runs = dag_runs_response.json()['dag_runs']
        if dag_runs:
            last_dag_run = dag_runs[-1]
            last_start_time = last_dag_run['start_date']
            last_end_time = last_dag_run['end_date']

        else:
            last_start_time = None
            last_end_time = None

        dag_info_list.append([dag_id, dag_status, task_ids, last_start_time, last_end_time])

    # Write data to CSV
    with open(CSV_FILE_PATH, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['DAG ID', 'Status', 'Tasks', 'Last Start Time', 'Last End Time'])
        for dag_info in dag_info_list:
            writer.writerow(dag_info)


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 15)
}

with DAG(
        'get_dags_metadata',
        default_args=default_args,
        description='A simple DAG to get info about all DAGs',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    get_dags_info_task = PythonOperator(
        task_id='get_dags_info',
        python_callable=get_dags_info,
    )

    get_dags_info_task
