# ... [unchanged imports] ...

import requests
import io
import zipfile
import os
import time
import pandas as pd  # ✅ Add this line
from sqlalchemy import text
from dotenv import load_dotenv
from ercot_auth import get_ercot_ropc_token
from db import engine

load_dotenv()

def get_archives_df(report_id: str, report_type: str, max_files: int = None) -> pd.DataFrame:
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
    skipped = 0
    for doc in sorted(archives, key=lambda x: x.get("postDatetime", "")):  # safer sort
        archive_id = doc.get("archiveId")
        if not archive_id:
            print(f"⚠️ Skipping archive with missing archiveId: {doc.get('friendlyName', 'unnamed')}")
            continue

        if already_ingested(archive_id, report_type):
            skipped += 1
            continue

        try:
            download_url = doc["_links"]["endpoint"]["href"]
        except KeyError:
            print(f"⚠️ Skipping {archive_id} due to missing download URL")
            log_ingest_status(archive_id, report_type, "error", "missing endpoint href")
            continue

        filename_hint = doc.get("friendlyName", f"{report_id}_{doc.get('postDatetime', archive_id)}")
        safe_name = filename_hint.replace(":", "-").replace("/", "-") + ".bin"
        cache_dir = f"cache/{report_id}"
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, safe_name)

        # === Load from cache or download ===
        try:
            if os.path.exists(cache_path):
                print(f"📂 Loading from cache: {safe_name}")
                with open(cache_path, "rb") as f:
                    content = f.read()
            else:
                print(f"🔽 Downloading: {filename_hint}")
                time.sleep(2)
                resp = requests.get(download_url, headers=headers)
                resp.raise_for_status()
                content = resp.content
                with open(cache_path, "wb") as f:
                    f.write(content)
                print(f"💾 Cached to {cache_path}")
        except Exception as e:
            print(f"⚠️ Skipping archive due to download error: {e}")
            log_ingest_status(archive_id, report_type, "error", str(e))
            continue

        # === Parse ZIP or plain CSV ===
        try:
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    csv_file = z.namelist()[0]
                    with z.open(csv_file) as f:
                        df = pd.read_csv(f)
            except zipfile.BadZipFile:
                df = pd.read_csv(io.BytesIO(content))

            records.append(df)
            log_ingest_status(archive_id, report_type, "success")
        except Exception as e:
            print(f"⚠️ Failed to parse archive {safe_name}: {e}")
            log_ingest_status(archive_id, report_type, "error", str(e))
            continue

        if max_files and len(records) >= max_files:
            break

    print(f"✅ Processed: {len(records)}, Skipped: {skipped}, Total: {len(archives)}")
    if not records:
        raise RuntimeError(f"❌ No valid archives processed for {report_id}")

    return pd.concat(records, ignore_index=True)
