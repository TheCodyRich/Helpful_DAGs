from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator

def get_formatted_message(**context):
    # Retrieve parameters from the DAG conf
    dag_run = coclearntext.get('dag_run')
    message = dag_run.conf.get('message', 'No specific reason provided')
    dag_name = dag_run.conf.get('dagName', 'Unknown DAG')
    # Format the message
    formatted_message = f"Error: {dag_name} failed for this reason: {message}"
    return formatted_message

with DAG(
    dag_id="opsgenie_alert_dag",
    start_date=datetime(2024, 4, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    get_formatted_message_task = PythonOperator(
        task_id='get_formatted_message_task',
        python_callable=get_formatted_message,
        provide_context=True
    )

    create_alert = OpsgenieCreateAlertOperator(
        task_id="create_opsgenie_alert",
        message="{{ task_instance.xcom_pull(task_ids='get_formatted_message_task') }}"
    )

    get_formatted_message_task >> create_alert
