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
    task_ids = kwargs['params']['task_ids']

    # Pull values from all specified tasks at once
    durations = ti.xcom_pull(task_ids=task_ids)

    # Filter out None results, just in case
    durations = [d for d in durations if d is not None]

    avg = sum(durations) / len(durations) if durations else None
    print(f"Average task duration: {avg}")
    return avg

with DAG(
    dag_id='metadb_last_3_months_avg',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Dynamically calculate task durations for last 3 complete months',
    tags=['metadb', 'dynamic', 'raw_sql'],
) as dag:

    now = pendulum.now("UTC")
    first_of_this_month = now.start_of("month")

    month_ranges = [
        (
            first_of_this_month.subtract(months=i+1).to_date_string(),
            first_of_this_month.subtract(months=i).to_date_string()
        )
        for i in reversed(range(3))
    ]

    monthly_tasks = []
    task_ids = []

    for start, end in month_ranges:
        label = pendulum.parse(start).format('MMMM')
        task_id = f"task_{label}"
        task_ids.append(task_id)

        task = PythonOperator(
            task_id=task_id,
            python_callable=run_raw_query,
            op_kwargs={
                "start_date_str": start,
                "end_date_str": end,
            },
        )
        monthly_tasks.append(task)

    avg_task = PythonOperator(
        task_id='calculate_average',
        python_callable=calculate_average,
        provide_context=True,
        params={'task_ids': task_ids},
    )

    monthly_tasks >> avg_task
