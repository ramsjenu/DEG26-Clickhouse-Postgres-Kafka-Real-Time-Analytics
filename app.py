import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import time

# Connect to ClickHouse
try:
    conn_str = 'clickhouse://default:@localhost/default'
    engine = create_engine(conn_str)
    session = sessionmaker(bind=engine)()
except Exception as e:
    st.error(f"Error while connecting to ClickHouse: {e}")

st.set_page_config(layout="wide")
st.title("Sales Transactions Real-Time ▁ ▂ ▃ ▄ ▅ ▆ █")

# Last updated timestamp
now = datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")
st.write(f"Last update: {dt_string}")

# Session state defaults
if "sleep_time" not in st.session_state:
    st.session_state.sleep_time = 5

if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True

# Time period mapping
mapping = {
    "1 hour": {"period": "60", "granularity": "minute", "raw": 60},
    "30 minutes": {"period": "30", "granularity": "minute", "raw": 30},
    "10 minutes": {"period": "10", "granularity": "minute", "raw": 10},
    "5 minutes": {"period": "5", "granularity": "minute", "raw": 5}
}

with st.expander("Configure Dashboard", expanded=True):
    left, right = st.columns(2)

    with left:
        auto_refresh = st.checkbox('Auto Refresh?', st.session_state.auto_refresh)
        if auto_refresh:
            number = st.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
            st.session_state.sleep_time = number

    with right:
        time_ago = st.radio("Time period to cover", mapping.keys(), horizontal=True, key="time_ago")

st.header("Live Kafka Sales...") 

minute = mapping[time_ago]["period"]

# Query ClickHouse using the new view and compute sales dynamically
query = f"""
SELECT 
    created_date,
    sales_order_number,
    COUNT(*) AS num_transactions,
    COUNT(DISTINCT sales_order_number) AS num_orders,
    ROUND(SUM(order_quantity * unit_price), 2) AS sales
FROM default.vw_sales
WHERE created_date >= now64() - INTERVAL {minute} MINUTE
GROUP BY created_date, sales_order_number
ORDER BY created_date
"""

df = pd.read_sql(query, engine)

# Display metrics
metric1, metric2, metric3 = st.columns(3)

metric1.metric(
    label="Number of Transactions",
    value=df['num_transactions'].sum(),
)

metric2.metric(
    label="Number of Orders",
    value=df['num_orders'].sum(),
)

metric3.metric(
    label="Sales Amount",
    value=df['sales'].sum(),
)

# Line chart for sales over time
st.header(f"Transactions in the last {minute} minutes") 
st.line_chart(data=df, x="created_date", y="sales")

# Show raw data
st.write(df[['sales_order_number', 'created_date', 'sales']])

# Auto refresh
if auto_refresh:
    st_autorefresh(interval=st.session_state.sleep_time * 1000, key="sales_refresh")
