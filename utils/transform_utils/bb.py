import pandas as pd

def calculate_bb(data: pd.DataFrame, window: int, price_col: str = 'close'):
    """Calculate Bollinger Bands."""
    rolling_mean = data[price_col].rolling(window=window).mean()
    rolling_std = data[price_col].rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * 2)
    lower_band = rolling_mean - (rolling_std * 2)
    return upper_band, lower_band
