# dashboard/pages/02_routes_map.py
import streamlit as st
import folium
from streamlit_folium import st_folium
from db import fetch_data

st.set_page_config(page_title="Routes Map", page_icon="🗺️", layout="wide")
st.header("Optimized Routes Map")

# Date selector
run_date = st.date_input("Select processing date")

# Fetch routes for the selected date
query = "SELECT driver_id, stop_sequence, total_distance_m FROM routes WHERE run_date = %s"
routes_df = fetch_data(query, params=[str(run_date)])

if not routes_df.empty:
    # Initialize map centered on Cairo
    m = folium.Map(location=[30.0444, 31.2357], zoom_start=11, tiles="CartoDB positron")

    colors = ["red","blue","green","purple","orange","darkred","lightred","beige",
                "darkblue","darkgreen","cadetblue","darkpurple","white","pink","lightblue",
                "lightgreen","gray","black","lightgray"]

    for idx, row in routes_df.iterrows():
        stops = row["stop_sequence"]
        color = colors[idx % len(colors)]
        coords = [[s["lat"], s["lon"]] for s in stops]
        
        # Draw the route line
        if len(coords) >= 2:
            folium.PolyLine(
                coords, 
                color=color, 
                weight=3, 
                opacity=0.8,
                tooltip=f"Driver: {row['driver_id']} | {row['total_distance_m']/1000:.1f} km"
            ).add_to(m)
            
        # Draw the delivery stops
        for i, s in enumerate(stops):
            folium.CircleMarker(
                [s["lat"], s["lon"]],
                radius=6, 
                color=color, 
                fill=True,
                popup=f"Stop {i+1} — Order {s['order_id'][:8]}"
            ).add_to(m)

    st_folium(m, width=None, height=600)
else:
    st.info(f"No routes generated for {run_date}.")