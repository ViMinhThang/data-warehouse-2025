import pandas as pd

def calculate_roc(data: pd.DataFrame, window: int, price_col: str = 'close') -> pd.Series:
    """Calculate the Rate of Change (ROC)."""
    roc = (data[price_col].diff(window) / data[price_col].shift(window)) * 100
    return roc
