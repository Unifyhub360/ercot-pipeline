# dash_app.py

import pandas as pd
import dash
from dash import dcc, html
import plotly.express as px
from db import engine  # ⬅️ Import from the centralized DB module

# ✅ Fetch and prepare data
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

server = app.server

if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
