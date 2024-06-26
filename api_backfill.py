# USAGE: A simple dag that allows users to run a backfill but tilizes the Airflow API rathar than the CLI so the 
#  scheduler and it's rules are not ignored.
#  
# There are no guarantees associated with this DAG


import json
import requests
from datetime import datetime
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models import Param



# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["backfill"],
    params={
        "backfill_start_date": Param(
            type="string",
            description="Start date for backfill (YYYY-MM-DD)",
            default="2024-04-01"
        ),
        "backfill_end_date": Param(
            type="string",
            description="End date for backfill (YYYY-MM-DD)",
            default="2024-04-02"
        ),
        "backfill_dag_id": Param(
            type="string",
            description="DAG to backfill",
            default="example_dag_basic"),
        "deployment_url": Param(
            type="string",
            description="Astro Deployment URL",
            default="https://clvv1pwr9089901qkuqz5vgl4.astronomer.run/d675lyym"
        ),
        "token": Param(
            type="string",
            description="the url of the astro deployment",
            default="Insert your token here"
        ),
    },
)
def api_backfill():
    @task
    def get_dates_to_backfill(backfill_start_date, backfill_end_date):
        print(backfill_start_date)
        start_date = datetime.strptime(backfill_start_date, "%Y-%m-%d")
        end_date = datetime.strptime(backfill_end_date, "%Y-%m-%d")
        dates = []
        current_date = start_date

        while current_date <= end_date:
            dates.append(current_date.strftime("%Y-%m-%dT%H:%M:%SZ"))
            current_date += timedelta(days=1)

        return dates

    @task
    def trigger_backfill(
        backfill_logical_date,
        **context,
    ):
        dag_id = context["params"]["backfill_dag_id"]
        deployment_url = context["params"]["deployment_url"]
        token = context["params"]["token"]

        ### Local Backfill
        # response = requests.post(
        #     url=f"http://host.docker.internal:8080/api/v1/dags/{dag_id}/dagRuns",
        #     auth=("admin", "admin"),
        #     data=json.dumps({"logical_date": backfill_logical_date}),
        #     headers={"Content-Type": "application/json"},
        # )

        ### Astro Backfill
        response = requests.post(
            url=f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns",
            data=json.dumps({"logical_date": backfill_logical_date}),
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
        )

    dates_to_backfill = get_dates_to_backfill(
        backfill_start_date="{{ params.backfill_start_date }}",
        backfill_end_date="{{ params.backfill_end_date }}",
    )
    trigger_backfill.expand(backfill_logical_date=dates_to_backfill)


# Instantiate the DAG
api_backfill()
