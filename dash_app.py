# dash_app.py

import os
import pandas as pd
import dash
from dash import dcc, html
import plotly.express as px
from db import engine

# ✅ Fetch and prepare data
def load_data():
    query = """
        SELECT delivery_date, hour_ending, stwpf_system_wide, wgrpp_lz_north
        FROM wind_hourly_forecast
        ORDER BY delivery_date DESC, hour_ending DESC
        LIMIT 500
    """
    try:
        df = pd.read_sql(query, engine)
        df["timestamp"] = pd.to_datetime(df["delivery_date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
        return df
    except Exception as e:
        print(f"⚠️ Failed to load data: {e}")
        return pd.DataFrame()

# 🚀 Load data
df = load_data()

# 📊 Set up the app
app = dash.Dash(__name__)
app.title = "ERCOT Wind Forecast"

app.layout = html.Div([
    html.H1("ERCOT Wind Hourly Forecast", style={"textAlign": "center"}),

    dcc.Graph(
        figure=px.line(
            df,
            x="timestamp",
            y="stwpf_system_wide",
            color="wgrpp_lz_north",
            title="System-Wide Wind Forecast (STWPF) by North Zone",
            labels={
                "timestamp": "Date & Hour",
                "stwpf_system_wide": "Forecasted Generation (MW)",
                "wgrpp_lz_north": "Zone"
            }
        ) if not df.empty else {
            "data": [],
            "layout": {"title": "⚠️ Data unavailable – please check back soon."}
        }
    )
])

server = app.server

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

