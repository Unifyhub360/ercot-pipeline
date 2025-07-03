import sys
import traceback
from datetime import datetime
from ercot_api import get_archives_df
from sqlalchemy import text
from db import engine

def start_pipeline_run(report_type: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO pipeline_run_log (report_type, status)
                VALUES (:type, 'running')
                RETURNING run_id
            """),
            {"type": report_type}
        )
        return result.scalar()

def end_pipeline_run(run_id: int, status: str, notes: str = None):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE pipeline_run_log
                SET status = :status,
                    notes = :notes,
                    run_timestamp = now()
                WHERE run_id = :id
            """),
            {"id": run_id, "status": status, "notes": notes}
        )

def run_pipeline(report_id: str, report_type: str, table_name: str, column_renames: dict = None):
    print(f"\n🚀 Starting ERCOT forecast data pipeline for {report_type}...\n")
    run_id = start_pipeline_run(report_type)

    try:
        df = get_archives_df(report_id, report_type)

        if df is None or df.empty:
            print("ℹ️ No new data available to ingest. Skipping upload.")
            end_pipeline_run(run_id, "skipped", notes="No new archives processed")
            return

        print(f"🧾 Retrieved {len(df)} new rows")

        if column_renames:
            print(f"🔧 Renaming columns: {column_renames}")
            df = df.rename(columns=column_renames)

        with engine.begin() as conn:
            df.to_sql(table_name, con=conn, if_exists="append", index=False)
        print(f"✅ {table_name} uploaded successfully.")

        end_pipeline_run(run_id, "success")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n🔥 Pipeline failed: {error_msg}\n")
        traceback.print_exc()
        end_pipeline_run(run_id, "fail", notes=error_msg)

    print("\n🎯 Pipeline complete.")

# === Example usage ===
if __name__ == "__main__":
    run_pipeline(
        report_id="NP4-733-CD",
        report_type="wind_5min_actuals",
        table_name="wind_5min_actuals",
        column_renames={"Date": "timestamp", "Wind": "wind_mw"}
    )

    run_pipeline(
        report_id="NP4-732-CD",
        report_type="wind_hourly_forecast",
        table_name="wind_hourly_forecast",
        column_renames={"Date": "timestamp", "Forecast": "forecast_mw"}
    )
