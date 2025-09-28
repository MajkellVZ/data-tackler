from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {}

with DAG(
    dag_id="dbt_transform_football",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/football_dbt && dbt run --target dev",
    )

    dbt_run