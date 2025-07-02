# ercot_getdata.py
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from ercot_auth import get_ercot_ropc_token

load_dotenv()

def get_solar_hourly_forecast(report_id="NP4-744-CD") -> pd.DataFrame:
    """
    Retrieves the latest solar hourly actual & forecast data via /getData endpoint.

    Args:
        report_id (str): ERCOT report ID for the solar hourly forecast dataset.

    Returns:
        pd.DataFrame: Wide-format DataFrame with generation and forecast metrics.
    """
    token = get_ercot_ropc_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": os.getenv("ERCOT_SUBSCRIPTION_KEY")
    }

    url = f"https://api.ercot.com/api/public-reports/getData/{report_id}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch JSON from {report_id}: {e}")

    rows = data.get("reportData", [])
    if not rows:
        raise ValueError("No data returned from getData endpoint.")

    records = []
    for row in rows:
        entry = {item["name"]: item["value"] for item in row.get("reportColumns", [])}
        records.append(entry)

    df = pd.DataFrame(records)
    return df
