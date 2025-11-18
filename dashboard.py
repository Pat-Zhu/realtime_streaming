import time
from typing import Tuple

import pandas as pd
import psycopg2
import streamlit as st


# Basic page config
st.set_page_config(
    page_title="Real-Time Ride-Sharing Dashboard",
    layout="wide",
)


# 1. Create and cache DB connection (so it is reused across reruns)
@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_pass",
        host="localhost",
        port=5432,
    )
    return conn


def load_latest_trips(conn, limit: int = 50) -> pd.DataFrame:
    """Read latest N trips from the database."""
    query = """
        SELECT
            trip_id,
            driver_id,
            passenger_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            distance_km,
            price_usd,
            ts
        FROM trips
        ORDER BY ts DESC
        LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(limit,))
    return df


def load_summary(conn) -> Tuple[int, float, float]:
    """Read summary metrics: total trips, avg price, avg distance."""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM trips;")
    total_trips = cur.fetchone()[0]

    cur.execute("SELECT AVG(price_usd), AVG(distance_km) FROM trips;")
    avg_price, avg_distance = cur.fetchone()

    avg_price = round(avg_price or 0, 2)
    avg_distance = round(avg_distance or 0, 2)

    return total_trips, avg_price, avg_distance


# 2. Main page layout
st.title("🚕 Real-Time Ride-Sharing Trips Dashboard")
st.caption("Data pipeline: Kafka → Postgres → Streamlit")

# Sidebar controls
st.sidebar.header("Controls")
limit = st.sidebar.slider(
    "Number of latest trips to display",
    min_value=10,
    max_value=200,
    value=50,
    step=10,
)
refresh_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds)",
    min_value=2,
    max_value=20,
    value=5,
    step=1,
)

# Connect to DB
try:
    conn = get_connection()
except Exception as e:
    st.error(f"Failed to connect to Postgres: {e}")
    st.stop()

# 3. Load data
df = load_latest_trips(conn, limit=limit)
total_trips, avg_price, avg_distance = load_summary(conn)

# 4. KPI metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total trips", total_trips)
col2.metric("Average price ($)", avg_price)
col3.metric("Average distance (km)", avg_distance)

st.markdown("---")

# 5. Main content: table + charts
left, right = st.columns([2, 1])

with left:
    st.subheader("Latest trips")
    if df.empty:
        st.info("No records found yet. Make sure the producer and consumer are running.")
    else:
        st.dataframe(df, use_container_width=True)

with right:
    st.subheader("Top 10 drivers by revenue")
    if not df.empty:
        by_driver = (
            df.groupby("driver_id")["price_usd"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        st.bar_chart(by_driver)

    st.subheader("Price over recent trips")
    if not df.empty:
        st.line_chart(df[["price_usd"]])

st.markdown("---")
st.caption(f"This page will auto-refresh every {refresh_interval} seconds.")

# 6. Simple auto-refresh: wait a few seconds, then rerun the script
time.sleep(refresh_interval)
st.rerun()

