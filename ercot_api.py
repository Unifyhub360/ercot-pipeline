# ercot_api.py
import requests
import io
import zipfile
import pandas as pd
import os
import time
from dotenv import load_dotenv
from ercot_auth import get_ercot_ropc_token

load_dotenv()

def get_recent_archives_df(report_id: str, num_archives: int = 24) -> pd.DataFrame:
    """
    Downloads and loads multiple ERCOT archives with caching and format detection.
    Archives may be CSV or ZIP (auto-handled).
    """
    token = get_ercot_ropc_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": os.getenv("ERCOT_SUBSCRIPTION_KEY")
    }

    meta_url = f"https://api.ercot.com/api/public-reports/archive/{report_id}"

    try:
        r = requests.get(meta_url, headers=headers)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch archive list for {report_id}: {e}")

    archives = data.get("archives", [])
    if not archives:
        raise ValueError(f"No archives found for {report_id}")

    records = []
    for doc in archives[:num_archives]:
        download_url = doc["_links"]["endpoint"]["href"]
        filename_hint = doc.get("friendlyName", f"{report_id}_{doc['postDatetime']}")
        safe_name = filename_hint.replace(":", "-").replace("/", "-") + ".bin"
        cache_dir = f"cache/{report_id}"
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, safe_name)

        # === Load from cache or download ===
        if os.path.exists(cache_path):
            print(f"📂 Loading from cache: {safe_name}")
            with open(cache_path, "rb") as f:
                content = f.read()
        else:
            print(f"🔽 Downloading: {filename_hint} at {doc['postDatetime']}")
            try:
                time.sleep(2)  # ⏱️ avoid 429 errors
                resp = requests.get(download_url, headers=headers)
                resp.raise_for_status()
                content = resp.content
                with open(cache_path, "wb") as f:
                    f.write(content)
                print(f"💾 Cached to {cache_path}")
            except Exception as e:
                print(f"⚠️ Skipping archive due to download error: {e}")
                continue

        # === Parse ZIP or plain CSV ===
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                csv_file = z.namelist()[0]
                with z.open(csv_file) as f:
                    df = pd.read_csv(f)
                    records.append(df)
                    continue
        except zipfile.BadZipFile:
            try:
                df = pd.read_csv(io.BytesIO(content))
                records.append(df)
            except Exception as e:
                print(f"⚠️ Failed to parse archive {safe_name}: {e}")
                continue

    if not records:
        raise RuntimeError(f"❌ No valid archives processed for {report_id}")

    return pd.concat(records, ignore_index=True)
