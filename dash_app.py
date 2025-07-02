import os
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")
engine = create_engine(db_url)

app = Dash(__name__)
app.title = "ERCOT Wind Forecast Dashboard"

def get_wind_5min():
    query = """
        SELECT *
        FROM wind_5min_actuals
        ORDER BY interval_ending DESC
        LIMIT 288
    """
    df = pd.read_sql(query, engine)
    df["interval_ending"] = pd.to_datetime(df["interval_ending"])
    return df.sort_values("interval_ending")

def get_wind_hourly():
    query = """
        SELECT *
        FROM wind_hourly_forecast
        ORDER BY delivery_date DESC, hour_ending DESC
        LIMIT 168
    """
    df = pd.read_sql(query, engine)
    return df.sort_values(["delivery_date", "hour_ending"])

df5 = get_wind_5min()
dfh = get_wind_hourly()

fig5 = px.line(df5, x="interval_ending", y=["system_wide_gen", "lz_west", "lz_north", "lz_south_houston"],
               title="Wind Generation — 5 Minute Actuals")

figh = px.line(dfh, x="hour_ending", y=["system_wide_gen", "stwpf_system_wide", "wgrpp_lz_north"],
               color=dfh["delivery_date"].dt.strftime("%Y-%m-%d"),
               title="Wind Forecast vs Actual — Hourly")

app.layout = html.Div([
    html.H1("ERCOT Wind Dashboard", style={"textAlign": "center"}),
    dcc.Graph(figure=fig5),
    dcc.Graph(figure=figh),
])

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
