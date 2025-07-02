from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os
from ercot_api import get_recent_archives_df

# === 📊 CONFIGURATION ===
NUM_FILES_5MIN = 5
NUM_FILES_HOURLY = 3
DATETIME_COLUMNS = {"interval_ending", "delivery_date"}

# === 🔐 Load DB connection ===
load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")
if not db_url:
    raise ValueError("🛑 Missing SUPABASE_DB_URL in .env")

engine = create_engine(db_url)

# === 🔍 Fetch existing keys from DB
def get_existing_key_df(table_name, key_column):
    try:
        if isinstance(key_column, tuple):
            query = f"SELECT {key_column[0]}, {key_column[1]} FROM {table_name};"
        else:
            query = f"SELECT {key_column} FROM {table_name};"
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"⚠️ Could not fetch existing keys from {table_name}: {e}")
        return pd.DataFrame()

# === 📦 Deduplicate and insert
def process_and_insert(df, renames, table_name, date_cols, allowed_cols, pk_column=None, run_id=None):
    df = df.rename(columns=renames)

    for col in date_cols:
        if col in DATETIME_COLUMNS:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    df = df[allowed_cols].dropna(subset=date_cols).drop_duplicates()

    if pk_column:
        existing_keys_df = get_existing_key_df(table_name, pk_column)

        if isinstance(pk_column, tuple):
            for col in pk_column:
                if col in DATETIME_COLUMNS and col in existing_keys_df.columns:
                    existing_keys_df[col] = pd.to_datetime(existing_keys_df[col], errors="coerce").dt.tz_localize(None)
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
            merged = df.merge(existing_keys_df, on=list(pk_column), how="left", indicator=True)
            df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        else:
            if pk_column in DATETIME_COLUMNS and pk_column in existing_keys_df.columns:
                existing_keys_df[pk_column] = pd.to_datetime(existing_keys_df[pk_column], errors="coerce").dt.tz_localize(None)
                df[pk_column] = pd.to_datetime(df[pk_column], errors="coerce").dt.tz_localize(None)
            df = df.merge(existing_keys_df, on=pk_column, how="left", indicator=True)
            df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

    print(f"🧾 Rows to insert into {table_name}: {len(df)}")

    if len(df) == 0:
        print(f"ℹ️ No new rows to insert into {table_name}")
    else:
        try:
            df.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"✅ {table_name} uploaded successfully.")
        except Exception as e:
            print(f"❌ Insert failed for {table_name}: {e}")

    if run_id:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO stream_insert_log (run_id, table_name, rows_inserted)
                    VALUES (:run_id, :table_name, :rows)
                """),
                {"run_id": run_id, "table_name": table_name, "rows": len(df)}
            )

# === 📝 Pipeline logging
def start_pipeline_run():
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO pipeline_run_log (status, notes)
            VALUES ('started', 'pipeline launched')
            RETURNING run_id
        """))
        return result.scalar()

def finalize_pipeline_run(run_id, status="success", notes=None):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE pipeline_run_log
            SET status = :status, notes = :notes
            WHERE run_id = :run_id
        """), {"status": status, "notes": notes, "run_id": run_id})

# === 🚀 Main entrypoint
def run_pipeline():
    print("🚀 Starting ERCOT forecast data pipeline...\n")
    run_id = start_pipeline_run()

    try:
        # Wind 5-Min Actuals
        df1 = get_recent_archives_df("NP4-733-CD", num_archives=NUM_FILES_5MIN)
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

        # Wind Hourly Forecast
        df2 = get_recent_archives_df("NP4-732-CD", num_archives=NUM_FILES_HOURLY)
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
