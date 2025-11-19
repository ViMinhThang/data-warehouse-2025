import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta
import random
from db.base_db import BaseDatabase
from dotenv import load_dotenv
import os

load_dotenv()

dw_db = BaseDatabase(
    host="10.11.1.66",
    dbname=os.getenv("DB_NAME_DW"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=int(os.getenv("DB_PORT", 5432)),
)
# conn = psycopg2.connect(
#     host="10.11.1.66",
#     port=5432,
#     dbname="dw",
#     user="postgres",
#     password="123456"
# )
# cur = conn.cursor()
# cur.execute("SELECT version();")
# print(cur.fetchone())
# conn.close()
result = dw_db.execute_query("SELECT * FROM fact_stock_indicators LIMIT 5")
print(result)
