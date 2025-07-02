# db.py

import socket
import os
from sqlalchemy import create_engine

# 🌐 Force IPv4 resolution (prevent IPv6 psycopg2 errors on Railway)
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(*args, **kwargs):
    return [ai for ai in orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
socket.getaddrinfo = getaddrinfo_ipv4

# 🔐 Load connection string and validate
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL or "sslmode=require" not in SUPABASE_DB_URL:
    raise ValueError("Invalid or missing SUPABASE_DB_URL. Must include sslmode=require.")

# 🚀 Create engine
engine = create_engine(SUPABASE_DB_URL)
