import logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone


def parse_tickers(tickers_str_or_list):
    if isinstance(tickers_str_or_list, str):
        tickers = tickers_str_or_list.split(",")
    else:
        tickers = tickers_str_or_list or []

    tickers = [t.strip().upper() for t in tickers if t.strip()]
    if not tickers:
        raise ValueError("Danh sách tickers rỗng hoặc không hợp lệ.")
    return tickers


def fetch_yfinance_data(ticker: str, period: str, interval: str) -> pd.DataFrame:
    logging.info(f"📥 Fetching data for {ticker} ({period}, {interval}) ...")
    data = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
    )

    if data.empty:
        logging.warning(f"⚠️ Không có dữ liệu cho {ticker}")
        return pd.DataFrame()

    if data.index.tz is None:
        data.index = data.index.tz_localize("UTC")
    else:
        data = data.tz_convert("UTC")

    return data


def compute_stock_indicators(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    result = pd.DataFrame()
    result["Close"] = data["Close"][ticker]
    result["Volume"] = data["Volume"][ticker]
    result["Diff"] = result["Close"].diff()
    result["PercentChangeClose"] = result["Close"].pct_change() * 100
    return result


def build_records_from_df(ticker: str, df: pd.DataFrame):
    records = []
    for dt, row in df.iterrows():
        records.append(
            {
                "ticker": ticker,
                "datetime_utc": dt,
                "close": row["Close"],
                "volume": row["Volume"],
                "diff": row["Diff"],
                "percent_change_close": row["PercentChangeClose"],
                "extracted_at": datetime.now(timezone.utc),
            }
        )
    return records
