# USAGE: A simple dag that checks the metadb to ensure it is the only DAG running, then hibernates the deployment if no other dags are running.
# If there are other dags running the 1st task fails. It will try to run every 5 minuites throughout the night until it can hibernate.  
# If there are no other dags running the next tasks hibernates the deployment until the OVERRIDE_DATE.  
#  
# Update the following parameters
#     API_TOKEN - Any valid token (including Workspace level)
#     DEPLOYMENT ID
#     ORG_ID
#     OVERRIDE_DATE - The time that you want your deployment to wake back up. Currently set for 7am
#
# There are no guarentees or warrenties associated with this DAG

from airflow import DAG
from airflow import settings
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils.timezone import make_aware
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowFailException
from sqlalchemy import func
from datetime import datetime, timedelta
import logging
import os
import requests

API_TOKEN = os.environ.get("API_TOKEN")
DEPLOYMENT_ID = os.environ.get("DEPLOYMENT_ID")
ORG_ID = os.environ.get("ORG_ID")
OVERRIDE_DATE = (datetime.now() + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')

def check_running_dags():
    session = settings.Session()
    running_dags_count = session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
    print("running dags count is:")
    print(running_dags_count)
    session.close()

    # Raise AirflowFailException if running_dags_count is less than or equal to 1
    if running_dags_count > 1:
        raise AirflowFailException("DAGs still running. Waiting to hibernate.")


def hibernate_deployments(API_TOKEN,DEPLOYMENT_ID,ORG_ID, OVERRIDE_DATE):
    response = requests.post(
        f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/deployments/{DEPLOYMENT_ID}/hibernation-override",
        headers={
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "isHibernating": True,
            "overrideUntil": OVERRIDE_DATE

        },
    )
    print(response.status_code)
    print(response.json())

default_args = {
    'owner': 'Cody',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': make_aware(datetime(2023, 1, 1, 17, 0, 0))  # Set to the desired start date
}

dag = DAG(
    'hibernation_dag',
    default_args=default_args,
    description='DAG that checks if other DAGs are running every 5 minutes from 5pm to 7am Monday to Friday',
    schedule_interval='*/5 17-23,0-7 * * 1-5',  # Every 5 minutes from 5pm to 7am, Monday to Friday
    catchup=False,
    max_active_runs=1
)

check_dags_task = PythonOperator(
    task_id='check_running_dags',
    python_callable=check_running_dags,
    provide_context=True,
    dag=dag,
)

hibernate_deployment_task = PythonOperator(
    task_id='hibernate_deployments',
    python_callable=hibernate_deployments,
    op_args=[API_TOKEN, DEPLOYMENT_ID, ORG_ID, OVERRIDE_DATE],
    dag=dag,
)

check_dags_task >> hibernate_deployment_task
