from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os
from ercot_api import get_archives_df  # ✅ renamed function supports smart ingestion

# === 📊 CONFIGURATION ===
DATETIME_COLUMNS = {"interval_ending", "delivery_date"}

# === 🔐 Load DB connection ===
load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")
if not db_url:
    raise ValueError("🛑 Missing SUPABASE_DB_URL in .env")

engine = create_engine(db_url)

# ... [unchanged helper functions: get_existing_key_df, process_and_insert, start_pipeline_run, finalize_pipeline_run] ...

# === 🚀 Main entrypoint
def run_pipeline():
    print("🚀 Starting ERCOT forecast data pipeline...\n")
    run_id = start_pipeline_run()

    try:
        # Wind 5-Min Actuals (report_id: NP4-733-CD)
        df1 = get_archives_df(
            report_id="NP4-733-CD",
            report_type="wind_5min_actuals"
            # Optional: max_files=5  # uncomment to limit for testing
        )
        process_and_insert(
            df1,
            {
                "INTERVAL_ENDING": "interval_ending",
                "SYSTEM_WIDE_GEN": "system_wide_gen",
                "LZ_SOUTH_HOUSTON": "lz_south_houston",
                "LZ_WEST": "lz_west",
                "LZ_NORTH": "lz_north",
                "SYSTEM_WIDE_HSL": "system_wide_hsl",
                "DSTFlag": "dst_flag"
            },
            "wind_5min_actuals",
            ["interval_ending"],
            ["interval_ending", "system_wide_gen", "lz_south_houston", "lz_west", "lz_north", "system_wide_hsl", "dst_flag"],
            pk_column="interval_ending",
            run_id=run_id
        )

        # Wind Hourly Forecast (report_id: NP4-732-CD)
        df2 = get_archives_df(
            report_id="NP4-732-CD",
            report_type="wind_hourly_forecast"
            # Optional: max_files=3
        )
        process_and_insert(
            df2,
            {
                "DELIVERY_DATE": "delivery_date",
                "HOUR_ENDING": "hour_ending",
                "SYSTEM_WIDE_GEN": "system_wide_gen",
                "STWPF_SYSTEM_WIDE": "stwpf_system_wide",
                "WGRPP_LZ_NORTH": "wgrpp_lz_north",
                "SYSTEM_WIDE_HSL": "system_wide_hsl",
                "DSTFlag": "dst_flag"
            },
            "wind_hourly_forecast",
            ["delivery_date"],
            ["delivery_date", "hour_ending", "system_wide_gen", "stwpf_system_wide", "wgrpp_lz_north", "system_wide_hsl", "dst_flag"],
            pk_column=("delivery_date", "hour_ending"),
            run_id=run_id
        )

        finalize_pipeline_run(run_id)

    except Exception as e:
        print(f"🔥 Pipeline failed: {e}")
        finalize_pipeline_run(run_id, status="fail", notes=str(e))

    print("\n🎯 Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
