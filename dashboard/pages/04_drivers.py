# dashboard/pages/04_drivers.py
import streamlit as st
import pandas as pd
from db import fetch_data

st.set_page_config(page_title="Driver Performance", page_icon="🚚", layout="wide")
st.header("Fleet & Driver Analytics")

run_date = st.date_input("Select processing date")

# Fetch driver performance metrics using a SQL JOIN
# jsonb_array_length is a native PostGIS/PostgreSQL function to count the stops in your JSON array!
query = """
    SELECT 
        d.driver_id,
        d.home_district,
        d.capacity_kg,
        r.total_distance_m / 1000.0 AS route_km,
        jsonb_array_length(r.stop_sequence) AS total_stops
    FROM drivers d
    LEFT JOIN routes r ON d.driver_id = r.driver_id AND r.run_date = %s
    ORDER BY route_km DESC NULLS LAST
"""

driver_stats = fetch_data(query, params=[str(run_date)])

if not driver_stats.empty:
    # High-level utilization metrics
    active_drivers = driver_stats['route_km'].notna().sum()
    total_fleet = len(driver_stats)
    
    st.subheader("Fleet Utilization")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Fleet Capacity", total_fleet)
    col2.metric("Active on Route", int(active_drivers))
    col3.metric("Idle / Available", int(total_fleet - active_drivers))
    
    st.divider()
    st.subheader("Driver Route Metrics")
    
    # Format the dataframe for a cleaner UI display
    formatted_df = driver_stats.copy()
    formatted_df['route_km'] = formatted_df['route_km'].apply(lambda x: f"{x:.1f}" if pd.notnull(x) else "0.0")
    formatted_df['total_stops'] = formatted_df['total_stops'].fillna(0).astype(int)
    
    # Streamlit dataframe with custom column configs
    st.dataframe(
        formatted_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "driver_id": "Driver ID",
            "home_district": "Home District",
            "capacity_kg": "Capacity (kg)",
            "route_km": "Distance Driven (km)",
            "total_stops": "Total Deliveries"
        }
    )
else:
    st.info(f"No driver data found for {run_date}. Check your Bronze layer ingestion.")