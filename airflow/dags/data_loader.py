import sqlite3
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable


RAW_DATA_DIR = Path("/opt/airflow/data/raw")
DB_PATH = Path("/opt/airflow/data/football.db")

def load_csv_to_sqlite(**kwargs):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    last_load_str = Variable.get("football_last_csv_load", default_var=None)
    last_load_time = pd.to_datetime(last_load_str) if last_load_str else pd.Timestamp.min

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            area_id INTEGER,
            area_name TEXT,
            competition_id INTEGER,
            competition_name TEXT,
            season_id INTEGER,
            utc_date TEXT,
            status TEXT,
            matchday INTEGER,
            home_team_id INTEGER,
            home_team_name TEXT,
            away_team_id INTEGER,
            away_team_name TEXT,
            home_score_full INTEGER,
            away_score_full INTEGER,
            last_updated TEXT
        )
    """)
    conn.commit()

    for csv_file in sorted(RAW_DATA_DIR.glob("matches_*.csv")):
        if pd.to_datetime(csv_file.stat().st_mtime, unit='s') <= last_load_time:
            continue

        df = pd.read_csv(csv_file)
        df = df.where(pd.notna(df), None)

        for _, row in df.iterrows():
            values = (
                row["id"],
                row["area.id"],
                row["area.name"],
                row["competition.id"],
                row["competition.name"],
                row["season.id"],
                row["utcDate"],
                row["status"],
                row["matchday"],
                row["homeTeam.id"],
                row["homeTeam.name"],
                row["awayTeam.id"],
                row["awayTeam.name"],
                row["score.fullTime.home"] if pd.notna(row["score.fullTime.home"]) else None,
                row["score.fullTime.away"] if pd.notna(row["score.fullTime.away"]) else None,
                row["lastUpdated"]
            )

            cursor.execute("""
                INSERT INTO matches (
                    match_id, area_id, area_name, competition_id, competition_name,
                    season_id, utc_date, status, matchday,
                    home_team_id, home_team_name, away_team_id, away_team_name,
                    home_score_full, away_score_full, last_updated
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                )
                ON CONFLICT(match_id) DO UPDATE SET
                    area_id=excluded.area_id,
                    area_name=excluded.area_name,
                    competition_id=excluded.competition_id,
                    competition_name=excluded.competition_name,
                    season_id=excluded.season_id,
                    utc_date=excluded.utc_date,
                    status=excluded.status,
                    matchday=excluded.matchday,
                    home_team_id=excluded.home_team_id,
                    home_team_name=excluded.home_team_name,
                    away_team_id=excluded.away_team_id,
                    away_team_name=excluded.away_team_name,
                    home_score_full=excluded.home_score_full,
                    away_score_full=excluded.away_score_full,
                    last_updated=excluded.last_updated
            """, values)

    conn.commit()
    conn.close()

    Variable.set("football_last_csv_load", pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    print(f"Loaded all CSV files from {RAW_DATA_DIR} into {DB_PATH}")

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="football_load_csv_sqlite",
    start_date=days_ago(1),
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["football", "sqlite", "load"],
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_to_sqlite",
        python_callable=load_csv_to_sqlite,
    )
