from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, datetime, timedelta
from psycopg2.extras import RealDictCursor
from db.base_db import BaseDatabase
from dotenv import load_dotenv
import os

load_dotenv()

def get_connected():
    conn = BaseDatabase(
            host="10.11.1.66",
            dbname=os.getenv("DB_NAME_DW"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", 5432)),
    )
    return conn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "FastAPI is running"}

@app.get("/stocks-indicators2")
def get_stock_indicators2(
    # stock_sk: int = Query(..., description="Stock SK (integer)"),
    # start: str = Query(..., description="Start date yyyy-mm-dd"),
    # end: str = Query(..., description="End date yyyy-mm-dd"),
    stock_sk: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """
    Trả dữ liệu stock indicators theo ticker + khoảng date.
    Join 3 bảng: fact_stock_indicators, dim_date, dim_stock
    """
    try:
        # Convert string to date
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, should be yyyy-mm-dd")

    sql = """
        SELECT
            f.record_sk,
            f.stock_sk,
            s.ticker,
            f.date_sk,
            d.full_date,
            f.close,
            f.volume,
            f.diff,
            f.percent_changer_close,
            f.rsi,
            f.roc,
            f.bb_upper,
            f.bb_lower,
            f.created_at
        FROM fact_stock_indicators f
        JOIN dim_date d ON f.date_sk = d.date_sk
        JOIN dim_stock s ON f.stock_sk = s.stock_sk
        WHERE f.stock_sk = %s
          AND d.full_date BETWEEN %s AND %s
        ORDER BY d.full_date
    """

    try:
        conn = get_connected()
        result = conn.execute_query(sql, (stock_sk, start_date, end_date))
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

@app.get("/stocks")
def get_stocks():
    """
    Trả danh sách stock để FE chọn ticker
    """
    sql = "SELECT stock_sk, ticker FROM dim_stock ORDER BY ticker"
    try:
        conn = get_connected()
        result = conn.execute_query(sql)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

@app.get("/stocks-indicators")
def get_stock_indicators(
    stock_sk: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """
    Lấy dữ liệu stock indicators.
    stock_sk: lọc theo cổ phiếu (nếu cần)
    start_date, end_date: dạng 'YYYY-MM-DD'
    """
    sql = """
        SELECT 
            f.record_sk,
            f.stock_sk,
            s.ticker,
            f.date_sk,
            d.full_date,
            f.close,
            f.volume,
            f.diff,
            f.percent_change_close,
            f.rsi,
            f.roc,
            f.bb_upper,
            f.bb_lower
        FROM fact_stock_indicators f
        JOIN dim_date d ON f.date_sk = d.date_sk
        JOIN dim_stock s ON f.stock_sk = s.stock_sk
        WHERE f.close IS NOT NULL AND f.volume IS NOT NULL
    """

    params = []
    if stock_sk:
        sql += " AND f.stock_sk = %s"
        params.append(stock_sk)
    if start:
        sql += " AND d.full_date >= %s"
        params.append(start)
    if end:
        sql += " AND d.full_date <= %s"
        params.append(end)

    sql += " ORDER BY d.full_date"

    try:
        conn = get_connected()
        result = conn.execute_query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"data": result}
