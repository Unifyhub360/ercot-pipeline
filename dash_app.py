# dash_app.py

import os
import socket
from sqlalchemy import create_engine
import pandas as pd
import dash
from dash import dcc, html
import plotly.express as px

# ⚙️ Force IPv4 DNS resolution (to avoid psycopg2 + IPv6 issue on Railway)
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(*args, **kwargs):
    return [ai for ai in orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
socket.getaddrinfo = getaddrinfo_ipv4

# 🛠️ Database connection
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # Make sure it ends with `?sslmode=require`
engine = create_engine(SUPABASE_DB_URL)

# ✅ Fetch and prepare data (customize as needed)
def load_data():
    query = "SELECT * FROM wind_forecast ORDER BY timestamp DESC LIMIT 500"
    df = pd.read_sql(query, engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

df = load_data()

# 📊 Dash app layout
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("ERCOT Wind Forecast Dashboard"),
    dcc.Graph(
        figure=px.line(df, x="timestamp", y="value", color="zone", title="Forecast vs Actuals")
    )
])

server = app.server  # Expose server for Railway or Render

if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
