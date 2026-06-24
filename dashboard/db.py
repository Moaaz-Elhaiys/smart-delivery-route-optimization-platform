# dashboard/db.py
import psycopg2
import pandas as pd
import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

# @st.cache_resource keeps the DB connection open across reruns
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "delivery_platform"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", os.getenv("DB_PASSWORD")),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432)
    )

# @st.cache_data caches the query results for 10 minutes (600 seconds)
@st.cache_data(ttl=600)
def fetch_data(query, params=None):
    conn = init_connection()
    # pandas read_sql is perfect for pulling PostGIS data directly into DataFrames
    return pd.read_sql(query, conn, params=params)