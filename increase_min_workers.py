# This DAG is used to change the minimum number of workers in a deployment (or any deployment setting) for times of high demand.
#  You can create 2 different versions of this dag one that increases the  minimum number of workers and one the reduces it back 
#  to zero after the high demand time is completed.  To gather all of the required details, you can use the GET Deployment API on 
#  deployment you plan to update.

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'Cody',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('dag_update', default_args=default_args, catchup=False, schedule_interval=timedelta(days=1),tags=["clean up"])

ASTRO_API_TOKEN = os.environ.get("ASTRO_API_TOKEN") #Deployment Admin API Token stored as an environment variable
ORG_ID = '<INSERT_ORG_ID>'
DEPLOYMENT_ID='<INSERT_DEPLOYMENT_ID>'

update_deployment = SimpleHttpOperator(
        task_id='update_deployment',
        dag=dag,
        method='POST',
        http_conn_id='astro_api',
        endpoint=f'/v1beta1/organizations/{ORG_ID}/deployments/{DEPLOYMENT_ID}',
        headers={"Authorization":f"Bearer {ASTRO_API_TOKEN}",
             "Content-Type": "application/json"
            },
    data=json.dumps({
        "contactEmails": [
            "user1@company.com"
        ],
        "defaultTaskPodCpu": "0.25",
        "defaultTaskPodMemory": "0.5Gi",
        "description": "Insert Description Here",
        "environmentVariables": [],
        "executor": "CELERY",
        "isCicdEnforced": False,
        "isDagDeployEnabled": True,
        "isHighAvailability": False,
        "name": "Deployment Name",
        "resourceQuotaCpu": "27",
        "resourceQuotaMemory": "54Gi",
        "schedulerSize": "LARGE",
        "type": "STANDARD",
        "workerQueues":[
            {
                "id": "<INSERT_QUEUE_ID>",
                "name": "default",
                "isDefault": True,
                "maxWorkerCount": 10,
                "minWorkerCount": 1,
                "workerConcurrency": 5,
                "podCpu": "1",
                "podMemory": "2Gi",
                "astroMachine": "A5"
            }
        ],
        "workloadIdentity": "<INSERT_WORKOAD_IDENTITY>",
        "workspaceId": "<INSERT_WORKSPACE_ID>"
    }),
)

update_deployment
