# USAGE: This dag runs daily and automates the hibernation of development Astro deployments and deletion of non-development deployments. 
# Any deployments that do not have hibernation enabled will be deleted unless they contain "_dnd" or "-dnd" at the end of their 
# deployment name (ex: "test_deployment_dnd"). Adjust the dag schedule to adjust the time of automatic hibernation and deletion.
# 
#  
# Update the following parameters
#     ORGANIZATION_ID
#     ASTRO_API_TOKEN
#     WOKRPSACE_ID
#    
# There are no guarantees associated with this DAG


import logging
from datetime import timedelta
from airflow.operators.python import get_current_context

import pendulum
import requests
from airflow.decorators import dag, task  # pyre-ignore
from airflow.models import Variable


def get_organization_workspace_and_token():
    organization_id = "<CHANGE_HERE>"  # example: cl17wsdpe08233o0ghhj3e76
    astro_api_token = Variable.get("AUTH_TOKEN") # org level token
    workspace_id = "<CHANGE_HERE>"  # example: cp305ign4si445i565n5
    return organization_id, workspace_id, astro_api_token


@dag(
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "airflow",
    },
    start_date=pendulum.datetime(2025, 5, 5, tz="UTC"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["Hibernation"],
)
def daily_deployment_hibernation() -> None:
    @task
    def get_deployments():
        organization_id, workspace_id, astro_api_token = (
            get_organization_workspace_and_token()
        )
        session = requests.session()
        session.headers = {"Authorization": f"Bearer {astro_api_token}"}
        try:
            response = session.get(
                url=f"https://api.astronomer.io/platform/v1beta1/organizations/{organization_id}/deployments?workspaceIds={workspace_id}",
            )
            response.raise_for_status()
            data = response.json()
            deployments = data["deployments"]
        except requests.exceptions.HTTPError as e:
            logging.exception(e.response.json()["message"])
            raise e
        else:
            return deployments

    @task(map_index_template="{{ custom_map_index }}")
    def delete_non_dev_deployments(deployment):
        context = get_current_context()
        context["custom_map_index"] = deployment["name"]
        organization_id, _, astro_api_token = get_organization_workspace_and_token()
        session = requests.session()
        session.headers = {"Authorization": f"Bearer {astro_api_token}"}
        try:
            if deployment["name"].lower().endswith(("_dnd", "-dnd")):
                logging.info(
                    "Deployment %s has Do Not Disturb mode enabled. Skipping deletion.",
                    deployment["name"],
                )
            elif not deployment["isDevelopmentMode"]:
                response = session.delete(
                    url=f"https://api.astronomer.io/platform/v1beta1/organizations/{organization_id}/deployments/{deployment["id"]}",
                )
                response.raise_for_status()
                logging.info("Deleting: %s", deployment["name"])
        except requests.exceptions.HTTPError as e:
            logging.exception(e.response.json()["message"])
            raise e

    @task(map_index_template="{{ custom_map_index }}")
    def set_hibernation(deployment):
        context = get_current_context()
        context["custom_map_index"] = deployment["name"]
        organization_id, _, astro_api_token = get_organization_workspace_and_token()
        session = requests.session()
        session.headers = {"Authorization": f"Bearer {astro_api_token}"}
        try:
            if deployment["status"] == "HIBERNATING":
                logging.info(
                    "Deployment %s is already hibernating.",
                    deployment["name"],
                )
            elif deployment["name"].lower().endswith(("_dnd", "-dnd")):
                logging.info(
                    "Deployment %s has Do Not Disturb mode enabled. Skipping hibernation.",
                    deployment["name"],
                )
            elif deployment["isDevelopmentMode"]:
                deployment_id = deployment["id"]
                response = session.post(
                    url=f"https://api.astronomer.io/platform/v1beta1/organizations/{organization_id}/deployments/{deployment_id}/hibernation-override",
                    json={
                        "isHibernating": True,
                    },
                )
                response.raise_for_status()
                logging.info("Setting hibernation for %s", deployment["name"])
        except requests.exceptions.HTTPError as e:
            logging.exception(e.response.json()["message"])
            raise e

    deployments = get_deployments()
    delete_non_dev_deployments.expand(deployment=deployments)
    set_hibernation.expand(deployment=deployments)


daily_deployment_hibernation()
