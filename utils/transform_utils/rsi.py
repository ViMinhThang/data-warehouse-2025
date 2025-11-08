import pandas as pd

def calculate_rsi(data: pd.DataFrame, window: int, price_col: str = 'close') -> pd.Series:
    """Calculate the Relative Strength Index (RSI)."""
    delta = data[price_col].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
