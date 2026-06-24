# dashboard/pages/01_overview.py
import streamlit as st
from db import fetch_data,init_connection
import pandas as pd

st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")
st.header("Executive Overview")

kpis = pd.read_sql("SELECT * FROM daily_kpis ORDER BY run_date DESC LIMIT 1",init_connection())

if not kpis.empty:
    row = kpis.iloc[0]
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Orders",         int(row["total_orders"]))
    col2.metric("Active Drivers",       int(row["active_drivers"]))
    col3.metric("Avg Route (km)",       f"{row['avg_route_km']:.1f}")
    col4.metric("Total km Driven",      f"{row['total_km_driven']:.0f}")
    col5.metric("Orders per Driver",    f"{row['avg_orders_per_driver']:.1f}")
else:
    st.warning("No KPI data found. Ensure the master pipeline has run.")