import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import time

DB_PATH = "/opt/airflow/data/football.db"

st.set_page_config(page_title="Football Data Explorer", layout="wide")
st.title("Football Data Explorer")

def load_data(retries=3, delay=3):
    attempt = 0
    while attempt < retries:
        try:
            conn = sqlite3.connect(DB_PATH)
            df = pd.read_sql_query("SELECT * FROM recent_matches", conn)
            conn.close()
            return df
        except (sqlite3.OperationalError, pd.io.sql.DatabaseError) as e:
            st.warning(f"Database not ready yet. Retrying in {delay} seconds... ({attempt+1}/{retries})")
            attempt += 1
            time.sleep(delay)
    st.error("Failed to load data from database. Please make sure DAGs have run successfully.")
    return pd.DataFrame()

df = load_data()

if not df.empty:
    st.subheader("All Matches")
    st.dataframe(df)

    finished_df = df[df["status"] == "FINISHED"]

    if not finished_df.empty:
        st.subheader("Score Comparison (Finished Matches)")
        fig = px.scatter(
            finished_df,
            x="home_score_full",
            y="away_score_full",
            hover_data=["home_team_name", "away_team_name", "utc_date"],
            labels={"home_score_full": "Home Score", "away_score_full": "Away Score"},
            title="Home vs Away Full-Time Scores"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No finished matches available to plot.")
else:
    st.info("No data available yet. Please wait for DAGs to populate the database.")
