from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
from airflow.models import Variable


API_URL = "https://api.football-data.org/v4/matches"
LOCAL_FOLDER = "/opt/airflow/data/raw"

os.makedirs(LOCAL_FOLDER, exist_ok=True)

def fetch_data(**kwargs):
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("FOOTBALL_API_KEY environment variable not set")

    headers = {"X-Auth-Token": api_key}

    last_run_str = Variable.get("football_last_run", default_var=None)
    if last_run_str:
        date_from = datetime.strptime(last_run_str, "%Y-%m-%d")
    else:
        date_from = datetime.utcnow() - timedelta(days=2)

    date_to = datetime.utcnow()

    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d")
    }

    matches = []
    try:
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        matches = resp.json().get("matches", [])
        print(f"Fetched {len(matches)} matches between {params['dateFrom']} and {params['dateTo']}")
    except Exception as e:
        print(f"Error fetching data: {e}")

    kwargs["ti"].xcom_push(key="matches", value=matches)

    if matches:
        Variable.set("football_last_run", date_to.strftime("%Y-%m-%d"))

    return len(matches)

def store_locally(**kwargs):
    ti = kwargs["ti"]
    matches = ti.xcom_pull(task_ids="fetch_task", key="matches")

    if not matches:
        print("No matches to store")
        return

    df = pd.json_normalize(matches)
    filename = f"matches_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(LOCAL_FOLDER, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} matches to {filepath}")
    return filepath

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="extract_football_local",
    start_date=days_ago(1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["football", "extraction", "local"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_task",
        python_callable=fetch_data,
    )

    store_task = PythonOperator(
        task_id="store_task",
        python_callable=store_locally,
    )

    fetch_task >> store_task
