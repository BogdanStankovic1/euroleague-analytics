# scripts/utils.py

import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "admin",
    "database": "euroleague_db",
    "charset":  "utf8mb4"
}

def get_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"❌ Greška pri konekciji: {e}")
        return None

def safe_int(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return int(value)
    except:
        return None

def safe_float(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except:
        return None

def clean_percentage(value):
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, str):
        try:
            return float(value.replace("%", "").strip()) / 100
        except:
            return None
    return float(value) / 100 if float(value) > 1 else float(value)

def safe_date(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except:
        return None