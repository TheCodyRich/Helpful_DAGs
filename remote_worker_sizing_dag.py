from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.operators.python import get_current_context
import pendulum

default_args = {
    'owner': 'Cody',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='remote_worker_sizing_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run raw SQL against MetaDB to compute task duration',
    tags=['metadb', 'raw_sql'],
) as dag:

    @provide_session
    def run_raw_query(start_date_str, end_date_str, session=None, **kwargs):
        start_date = pendulum.parse(start_date_str).in_timezone("UTC")
        end_date = pendulum.parse(end_date_str).in_timezone("UTC")

        raw_sql = """
            SELECT 
                SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
            FROM 
                task_instance
            WHERE 
                start_date >= :start_date
                AND end_date < :end_date
                AND end_date IS NOT NULL
                AND start_date IS NOT NULL;
        """

        result = session.execute(
            raw_sql,
            {
                "start_date": start_date,
                "end_date": end_date,
            },
        ).scalar()

        print(f"Total task duration (minutes) from {start_date_str} to {end_date_str}: {result}")
        return result

    def calculate_average(**kwargs):
        ti = kwargs['ti']
        jan = ti.xcom_pull(task_ids='January')
        feb = ti.xcom_pull(task_ids='February')
        mar = ti.xcom_pull(task_ids='March')

        durations = [val for val in [jan, feb, mar] if val is not None]
        if not durations:
            avg = None
        else:
            avg = sum(durations) / len(durations)

        print(f"Average task duration across January, February, and March: {avg}")
        return avg

    January = PythonOperator(
        task_id='January',
        python_callable=run_raw_query,
        op_kwargs={
            "start_date_str": "2025-01-01",
            "end_date_str": "2025-01-31",
        },
    )

    February = PythonOperator(
        task_id='February',
        python_callable=run_raw_query,
        op_kwargs={
            "start_date_str": "2025-02-01",
            "end_date_str": "2025-02-28",
        },
    )

    March = PythonOperator(
        task_id='March',
        python_callable=run_raw_query,
        op_kwargs={
            "start_date_str": "2025-03-01",
            "end_date_str": "2025-03-31",
        },
    )

    Calculate_Average = PythonOperator(
        task_id='Calculate_Average',
        python_callable=calculate_average,
        provide_context=True,
    )

    [January, February, March] >> Calculate_Average
