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
# There are no guarantees associated with this DAG

from airflow import DAG
from airflow import settings
from airflow.operators.python import get_current_context
from airflow.decorators import task, dag
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils.timezone import make_aware
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowSkipException
from sqlalchemy import func
from datetime import datetime, timedelta
import logging
import os
import requests

API_TOKEN = os.environ.get("API_TOKEN")
DEPLOYMENT_ID = os.environ.get("DEPLOYMENT_ID")
ORG_ID = os.environ.get("ORG_ID")
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL")
OVERRIDE_DATE = (
    (datetime.now() + timedelta(days=1))
    .replace(hour=7, minute=0, second=0, microsecond=0)
    .strftime("%Y-%m-%dT%H:%M:%SZ")
)

@task
def get_running_dags(
    page_limit=100,
    page_offset=0,
):
    ctx = get_current_context()
    """
    gets DAGRuns by request to the Airflow REST API
    """
    json = {
        "page_limit": page_limit,
        "page_offset": page_offset,
        "states": ["running"],
    }

    resp = requests.post(f"{AIRFLOW_API_URL}/dags/~/dagRuns/list", json=json)
    if resp.status_code != 200:
        raise Exception(f"Failed to get running DAG runs: {resp.status_code} {resp.text}")
    dag_runs = resp.json()['dag_runs']
    all_dag_runs = list(
            filter(lambda x: x["dag_id"] != ctx["dag"].dag_id, dag_runs)
        )
    
    if len(all_dag_runs) > 0:
        raise AirflowSkipException("DAGs still running. Waiting to hibernate.")
    
    print("No other DAGs running")
    return True


@task
def hibernate_deployments(API_TOKEN, DEPLOYMENT_ID, ORG_ID, OVERRIDE_DATE):
    response = requests.post(
        f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/deployments/{DEPLOYMENT_ID}/hibernation-override",
        headers={
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"isHibernating": True, "overrideUntil": OVERRIDE_DATE},
    )
    print(response.status_code)
    print(response.json())


default_args = {
    "owner": "Anand",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": make_aware(
        datetime(2025, 1, 1, 17, 0, 0)
    ),  # Set to the desired start date
}

@dag(
    default_args=default_args,
    description="DAG that checks if other DAGs are running every 5 minutes from 5pm to 7am Monday to Friday",
    schedule_interval="*/5 17-23,0-6 * * 1-5",  # Every 5 minutes from 5pm to 7am, Monday to Friday
    catchup=False,
    max_active_runs=1,
)
def hibernation_dag():
    get_running_dags() >> hibernate_deployments()

hibernation_dag()
