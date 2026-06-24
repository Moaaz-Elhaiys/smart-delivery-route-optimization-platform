# dashboard/app.py
import streamlit as st

st.set_page_config(
    page_title="Delivery Route Optimizer",
    page_icon="🗺️",
    layout="wide",
)

st.title("Smart Delivery Route Optimization Platform")
st.caption("Cairo — Powered by Spark · Sedona · OR-Tools · PostGIS")

st.markdown("""
### Welcome to the Operations Command Center
Use the sidebar to navigate through the operational layers:
* **01 Overview:** Executive KPI summary and high-level platform health.
* **02 Routes Map:** Deep dive into OR-Tools generated geometries.
* **03 Hotspots:** Spatial density analysis of incoming orders.
* **04 Drivers:** Individual fleet performance and capacity metrics.
""")