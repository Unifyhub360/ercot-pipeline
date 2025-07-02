# ercot_loader.py

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import socket

# === 🌐 Patch to force IPv4 DNS resolution ===
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(*args, **kwargs):
    return [ai for ai in orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
socket.getaddrinfo = getaddrinfo_ipv4

# === 🔐 Load DB credentials ===
load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")

if not db_url:
    raise ValueError("No Supabase DB URL found. Make sure .env is configured correctly.")

engine = create_engine(db_url)

# === 🧪 Example: Replace with your actual DataFrame loaders ===
from your_existing_fetchers import (
    get_latest_archive_df  # or specific fetchers per report
)

def insert_wind_5min_actuals():
    df = get_latest_archive_df("NP4-733-CD")
    df = df.rename(columns={
        "INTERVAL_ENDING": "interval_ending",
        "SYSTEM_WIDE_GEN": "system_wide_gen",
        "LZ_SOUTH_HOUSTON": "lz_south_houston",
        "LZ_WEST": "lz_west",
        "LZ_NORTH": "lz_north",
        "SYSTEM_WIDE_HSL": "system_wide_hsl",
        "DSTFlag": "dst_flag"
    })
    df["interval_ending"] = pd.to_datetime(df["interval_ending"])
    df.to_sql("wind_5min_actuals", engine, if_exists="append", index=False)
    print("✅ wind_5min_actuals uploaded")

def insert_wind_hourly_forecast():
    df = get_latest_archive_df("NP4-732-CD")
    df = df.rename(columns={
        "DELIVERY_DATE": "delivery_date",
        "HOUR_ENDING": "hour_ending",
        "SYSTEM_WIDE_GEN": "system_wide_gen",
        "WGRPP_LZ_NORTH": "wgrpp_lz_north",
        "STWPF_System_Wide": "stwpf_system_wide",
        "SYSTEM_WIDE_HSL": "system_wide_hsl",
        "DSTFlag": "dst_flag"
    })
    df["delivery_date"] = pd.to_datetime(df["delivery_date"])
    df.to_sql("wind_hourly_forecast", engine, if_exists="append", index=False)
    print("✅ wind_hourly_forecast uploaded")

# === 🚀 Run all inserts
if __name__ == "__main__":
    insert_wind_5min_actuals()
    insert_wind_hourly_forecast()
