# ercot_auth.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_ercot_ropc_token():
    url = os.getenv("ERCOT_ROPC_TOKEN_URL")
    data = {
        "grant_type": "password",
        "username": os.getenv("ERCOT_USERNAME"),
        "password": os.getenv("ERCOT_PASSWORD"),
        "scope": os.getenv("ERCOT_SCOPE"),
        "client_id": os.getenv("ERCOT_CLIENT_ID"),
        "response_type": "id_token"
    }

    headers = { "Content-Type": "application/x-www-form-urlencoded" }

    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()

    # Depending on ERCOT config, the token may live under different keys
    return response.json().get("access_token") or response.json().get("id_token")
