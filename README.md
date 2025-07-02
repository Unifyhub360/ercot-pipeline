📡 ERCOT Data Pipeline
This project is a secure, production-ready Python pipeline that automates the retrieval, cleaning, deduplication, and storage of public ERCOT datasets. It handles both ZIP archive and JSON-based reports, inserts clean time series into a Supabase-hosted PostgreSQL database, and logs every pipeline run and row-level insert for auditability and dashboarding.

🚀 Features
Automated ingestion of wind & solar generation and forecast data

Real-time token authentication via Azure B2C (ROPC Flow)

Modular architecture for fetching archive and live JSON data

Deduplication using primary or composite keys

Logging of every script run (pipeline_run_log)

Logging of insert counts per stream (stream_insert_log)

.env file for secure credential management

Fully extensible for dashboards, forecasting, and scheduling

📁 Project Structure
ercot_pipeline/
│
├── pipeline.py            # Main orchestrator script
├── main.py                # Optional CLI or app entry point
├── ercot_api.py           # Retrieves ZIP archive datasets
├── ercot_auth.py          # Handles ROPC token authentication
├── ercot_getdata.py       # Retrieves JSON datasets via /getData
├── ercot_loader.py        # Optional custom ingestion tools
├── .env                   # ⚠️ Stores secrets (excluded from Git)
└── README.md              # This documentation
🔐 Environment Variables (.env)
Create a .env file in the root of your project:

dotenv
# Azure B2C ROPC Auth (Replace with your actual credentials)
ERCOT_USERNAME=your.email@example.com
ERCOT_PASSWORD=yourStrongPassword
ERCOT_CLIENT_ID=fec253ea-0d06-4272-a5e6-b478baeecd70
ERCOT_SCOPE=openid fec253ea-0d06-4272-a5e6-b478baeecd70 offline_access
ERCOT_ROPC_TOKEN_URL=https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token
ERCOT_SUBSCRIPTION_KEY=your-subscription-key

# Supabase connection
SUPABASE_DB_URL=postgresql://username:password@host:port/dbname
📦 Running the Pipeline
bash
python pipeline.py
✅ Output will include:

Stream activity per dataset

Row insert counts

Errors or warnings

Final “Pipeline complete” summary

🧾 Logging Tables (in Supabase)
pipeline_run_log
Tracks each script run.

Column	Type	Notes
run_id	SERIAL PK	Unique run identifier
run_timestamp	TIMESTAMPTZ	Automatically set per run
status	TEXT	"success", "fail", "partial"
notes	TEXT	Optional error or debug notes
stream_insert_log
Tracks number of rows inserted per table per run.

Column	Type	Notes
log_id	SERIAL PK	Auto-incremented log ID
run_id	INT FK	Links to pipeline_run_log
table_name	TEXT	e.g., wind_5min_actuals
rows_inserted	INT	Number of rows added this run
log_timestamp	TIMESTAMPTZ	Set to now()