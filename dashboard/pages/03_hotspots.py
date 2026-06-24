# dashboard/pages/03_hotspots.py
import streamlit as st
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
import pandas as pd
from db import init_connection

st.set_page_config(page_title="Hotspots", page_icon="🔥", layout="wide")
st.header("Delivery Density Heatmap")

# Date selector
run_date = st.date_input("Select processing date")

# Establish connection
conn = init_connection()

# --- Cached Data Functions ---
@st.cache_data(ttl=600) # Cache expires after 10 minutes to keep data fresh
def get_map_data(date_str):
    coords_query = """
        SELECT 
            ST_Y(location::geometry) as lat, 
            ST_X(location::geometry) as lon
        FROM orders
        WHERE run_date = %s
    """
    return pd.read_sql(coords_query, conn, params=[date_str])

@st.cache_data(ttl=600)
def get_district_stats(date_str):
    stats_query = """
        SELECT district, COUNT(*) as "Total Orders"
        FROM orders
        WHERE run_date = %s
        GROUP BY district
        ORDER BY "Total Orders" DESC
    """
    return pd.read_sql(stats_query, conn, params=[date_str])

# --- Fetch Data ---
coords_df = get_map_data(str(run_date))

if not coords_df.empty:
    # Fix: Actually enforce the 3-to-1 layout
    col1, col2 = st.columns([3, 1])    
    with col1:
        # Initialize map centered on Cairo
        m = folium.Map(location=[30.0444, 31.2357], zoom_start=11, tiles="CartoDB positron")
        
        # Convert Pandas DataFrame columns to a list of [lat, lon] pairs
        heat_data = coords_df[['lat', 'lon']].values.tolist()
        
        # Add the HeatMap layer
        HeatMap(
            heat_data,
            radius=15,          
            blur=10,            
            min_opacity=0.4,
            gradient={0.4: 'blue', 0.6: 'cyan', 0.7: 'lime', 0.9: 'yellow', 1.0: 'red'}
        ).add_to(m)
        
        # Fix: Use container width for cleaner responsive UI
        st_folium(m, height=600, use_container_width=True)
        
    with col2:
        st.subheader("Top Districts")
        st.caption("Order volume by zone")
        
        # Fix: Complete the data fetch and render the dataframe
        stats_df = get_district_stats(str(run_date))
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
else:
    st.info(f"No delivery data available for {run_date}.")