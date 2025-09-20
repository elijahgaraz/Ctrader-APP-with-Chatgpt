import pandas as pd
import pandas_ta as ta

# Ensure DataFrame has the required OHLC columns, optionally Volume
# For pandas_ta, columns are often expected to be lowercase: 'open', 'high', 'low', 'close', 'volume'

def calculate_ema(ohlc_df: pd.DataFrame, length: int = 20, source_col: str = 'close') -> pd.Series:
    """Calculates Exponential Moving Average (EMA)."""
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length: # Not enough data for EMA
        return pd.Series(dtype='float64')
    return ohlc_df.ta.ema(length=length, append=False)

def calculate_atr(ohlc_df: pd.DataFrame, length: int = 14) -> pd.Series:
    """Calculates Average True Range (ATR). Requires 'high', 'low', 'close' columns."""
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length: # Not enough data for ATR
        return pd.Series(dtype='float64')
    # pandas_ta expects lowercase column names
    temp_df = ohlc_df.rename(columns={'high': 'high', 'low': 'low', 'close': 'close'})
    return temp_df.ta.atr(length=length, append=False)

def calculate_rsi(ohlc_df: pd.DataFrame, length: int = 14, source_col: str = 'close') -> pd.Series:
    """Calculates Relative Strength Index (RSI)."""
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length:
        return pd.Series(dtype='float64')
    return ohlc_df.ta.rsi(length=length, close=ohlc_df[source_col], append=False)

def calculate_stochastic(ohlc_df: pd.DataFrame, k: int = 5, d: int = 3, smooth_k: int = 3) -> pd.DataFrame:
    """
    Calculates Stochastic Oscillator (Fast %K and Fast %D).
    Requires 'high', 'low', 'close'.
    Returns a DataFrame with 'STOCHk_k_d_smooth_k' and 'STOCHd_k_d_smooth_k' columns.
    Example column names from pandas-ta: STOCHk_5_3_3, STOCHd_5_3_3
    """
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        return pd.DataFrame() # Return empty DataFrame
    if len(ohlc_df) < max(k,d,smooth_k): # Basic check for enough data
         return pd.DataFrame()
    # pandas_ta expects lowercase column names
    temp_df = ohlc_df.rename(columns={'high': 'high', 'low': 'low', 'close': 'close'})
    stoch_df = temp_df.ta.stoch(k=k, d=d, smooth_k=smooth_k, append=False)
    if stoch_df is None or stoch_df.empty:
        return pd.DataFrame()
    # Rename columns for easier access if needed, or use pandas_ta default names.
    # Example: stoch_df.rename(columns={f'STOCHk_{k}_{d}_{smooth_k}': 'K', f'STOCHd_{k}_{d}_{smooth_k}': 'D'}, inplace=True)
    return stoch_df # Returns DataFrame with STOCHk and STOCHd columns

def calculate_momentum(ohlc_df: pd.DataFrame, length: int = 12, source_col: str = 'close') -> pd.Series:
    """Calculates Momentum."""
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length:
        return pd.Series(dtype='float64')
    return ohlc_df.ta.mom(length=length, close=ohlc_df[source_col], append=False)

def calculate_donchian(ohlc_df: pd.DataFrame, lower_length: int = 20, upper_length: int = 20) -> pd.DataFrame:
    """
    Calculates Donchian Channels. Requires 'high', 'low'.
    Returns a DataFrame with columns like 'DONCHIANl_20_20', 'DONCHIANm_20_20', 'DONCHIANu_20_20'.
    """
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low']):
        return pd.DataFrame()
    if len(ohlc_df) < max(lower_length, upper_length):
         return pd.DataFrame()
    # pandas_ta expects lowercase column names
    temp_df = ohlc_df.rename(columns={'high': 'high', 'low': 'low'})
    donchian_df = temp_df.ta.donchian(lower_length=lower_length, upper_length=upper_length, append=False)
    if donchian_df is None or donchian_df.empty:
        return pd.DataFrame()
    return donchian_df

def calculate_bollinger_bands(ohlc_df: pd.DataFrame, length: int = 20, std: float = 2, source_col: str = 'close') -> pd.DataFrame:
    """
    Calculates Bollinger Bands.
    Returns a DataFrame with columns like 'BBL_20_2.0' (lower), 'BBM_20_2.0' (middle/SMA), 'BBU_20_2.0' (upper),
    'BBB_20_2.0' (bandwidth), 'BBP_20_2.0' (percent).
    """
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.DataFrame()
    if len(ohlc_df) < length:
        return pd.DataFrame()
    bbands_df = ohlc_df.ta.bbands(length=length, std=std, close=ohlc_df[source_col], append=False)
    if bbands_df is None or bbands_df.empty:
        return pd.DataFrame()
    return bbands_df

if __name__ == '__main__':
    # Example Usage (requires a sample CSV or DataFrame)
    # Create a sample DataFrame for testing
    data = {
        'timestamp': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:01:00', '2023-01-01 10:02:00',
                                     '2023-01-01 10:03:00', '2023-01-01 10:04:00', '2023-01-01 10:05:00',
                                     '2023-01-01 10:06:00', '2023-01-01 10:07:00', '2023-01-01 10:08:00',
                                     '2023-01-01 10:09:00', '2023-01-01 10:10:00']),
        'open':  [1.1000, 1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018],
        'high':  [1.1003, 1.1006, 1.1007, 1.1010, 1.1012, 1.1015, 1.1013, 1.1016, 1.1018, 1.1017, 1.1020],
        'low':   [1.0999, 1.1001, 1.1002, 1.1001, 1.1007, 1.1008, 1.1007, 1.1010, 1.1012, 1.1011, 1.1014],
        'close': [1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018, 1.1016],
        'volume':[10,     12,     15,     13,     18,     20,     19,     22,     25,     23,     28]
    }
    sample_ohlc_df = pd.DataFrame(data)
    sample_ohlc_df.set_index('timestamp', inplace=True) # pandas-ta often works well with DatetimeIndex

    print("Sample OHLC Data:")
    print(sample_ohlc_df)
    print("\nEMA (10) on close:")
    ema_values = calculate_ema(sample_ohlc_df, length=5, source_col='close')
    print(ema_values)

    print("\nATR (5):")
    # Ensure columns are lowercase if pandas-ta expects it, or pass a renamed df
    atr_values = calculate_atr(sample_ohlc_df.rename(columns=str.lower), length=5)
    print(atr_values)

    print("\nRSI (7) on close:")
    rsi_values = calculate_rsi(sample_ohlc_df, length=7, source_col='close')
    print(rsi_values)

    print("\nStochastic (5,3,3):")
    stoch_values = calculate_stochastic(sample_ohlc_df.rename(columns=str.lower), k=5, d=3, smooth_k=3)
    print(stoch_values)

    print("\nMomentum (6) on close:")
    mom_values = calculate_momentum(sample_ohlc_df, length=6, source_col='close')
    print(mom_values)

    print("\nDonchian Channels (5,5):")
    donchian_values = calculate_donchian(sample_ohlc_df.rename(columns=str.lower), lower_length=5, upper_length=5)
    print(donchian_values)

    print("\nBollinger Bands (5,2) on close:")
    bbands_values = calculate_bollinger_bands(sample_ohlc_df, length=5, std=2, source_col='close')
    print(bbands_values)

    # Test with insufficient data
    print("\nEMA (10) on close (insufficient data):")
    ema_insufficient = calculate_ema(sample_ohlc_df.head(3), length=5)
    print(ema_insufficient) # Should be empty or all NaN

    print("\nATR (14) with insufficient data")
    atr_insufficient = calculate_atr(sample_ohlc_df.head(10).rename(columns=str.lower), length=14)
    print(atr_insufficient)

    print("\nTesting with empty DataFrame:")
    empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    print("EMA from empty:", calculate_ema(empty_df, 5))
    print("ATR from empty:", calculate_atr(empty_df, 5))

    # Note: pandas-ta often returns Series/DataFrames with NaNs at the beginning
    # where the indicator cannot be calculated due to insufficient prior data.
    # This is standard behavior.
    # The wrappers include basic checks for empty DFs or insufficient length for the period.
    # pandas-ta also expects column names to be lowercase by default for many indicators,
    # so the wrappers use .rename(columns=str.lower) or pass specific column names.
    # For the real OHLC data from Trader, we will ensure columns are named 'open', 'high', 'low', 'close'.

def calculate_adx(ohlc_df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    """
    Calculates the Average Directional Movement Index (ADX).
    Requires 'high', 'low', 'close'.
    Returns a DataFrame with ADX, DMP, and DMN columns.
    e.g., ADX_14, DMP_14, DMN_14
    """
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        return pd.DataFrame()
    if len(ohlc_df) < length:
        return pd.DataFrame()
    temp_df = ohlc_df.rename(columns={'high': 'high', 'low': 'low', 'close': 'close'})
    adx_df = temp_df.ta.adx(length=length, append=False)
    if adx_df is None or adx_df.empty:
        return pd.DataFrame()
    return adx_df


if __name__ == '__main__':
    # Example Usage (requires a sample CSV or DataFrame)
    # Create a sample DataFrame for testing
    data = {
        'timestamp': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:01:00', '2023-01-01 10:02:00',
                                     '2023-01-01 10:03:00', '2023-01-01 10:04:00', '2023-01-01 10:05:00',
                                     '2023-01-01 10:06:00', '2023-01-01 10:07:00', '2023-01-01 10:08:00',
                                     '2023-01-01 10:09:00', '2023-01-01 10:10:00']),
        'open':  [1.1000, 1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018],
        'high':  [1.1003, 1.1006, 1.1007, 1.1010, 1.1012, 1.1015, 1.1013, 1.1016, 1.1018, 1.1017, 1.1020],
        'low':   [1.0999, 1.1001, 1.1002, 1.1001, 1.1007, 1.1008, 1.1007, 1.1010, 1.1012, 1.1011, 1.1014],
        'close': [1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018, 1.1016],
        'volume':[10,     12,     15,     13,     18,     20,     19,     22,     25,     23,     28]
    }
    sample_ohlc_df = pd.DataFrame(data)
    sample_ohlc_df.set_index('timestamp', inplace=True) # pandas-ta often works well with DatetimeIndex

    print("Sample OHLC Data:")
    print(sample_ohlc_df)
    print("\nEMA (10) on close:")
    ema_values = calculate_ema(sample_ohlc_df, length=5, source_col='close')
    print(ema_values)

    print("\nATR (5):")
    # Ensure columns are lowercase if pandas-ta expects it, or pass a renamed df
    atr_values = calculate_atr(sample_ohlc_df.rename(columns=str.lower), length=5)
    print(atr_values)

    print("\nRSI (7) on close:")
    rsi_values = calculate_rsi(sample_ohlc_df, length=7, source_col='close')
    print(rsi_values)

    print("\nADX (5):")
    adx_values = calculate_adx(sample_ohlc_df.rename(columns=str.lower), length=5)
    print(adx_values)

    print("\nStochastic (5,3,3):")
    stoch_values = calculate_stochastic(sample_ohlc_df.rename(columns=str.lower), k=5, d=3, smooth_k=3)
    print(stoch_values)

    print("\nMomentum (6) on close:")
    mom_values = calculate_momentum(sample_ohlc_df, length=6, source_col='close')
    print(mom_values)

    print("\nDonchian Channels (5,5):")
    donchian_values = calculate_donchian(sample_ohlc_df.rename(columns=str.lower), lower_length=5, upper_length=5)
    print(donchian_values)

    print("\nBollinger Bands (5,2) on close:")
    bbands_values = calculate_bollinger_bands(sample_ohlc_df, length=5, std=2, source_col='close')
    print(bbands_values)

    # Test with insufficient data
    print("\nEMA (10) on close (insufficient data):")
    ema_insufficient = calculate_ema(sample_ohlc_df.head(3), length=5)
    print(ema_insufficient) # Should be empty or all NaN

    print("\nATR (14) with insufficient data")
    atr_insufficient = calculate_atr(sample_ohlc_df.head(10).rename(columns=str.lower), length=14)
    print(atr_insufficient)

    print("\nTesting with empty DataFrame:")
    empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    print("EMA from empty:", calculate_ema(empty_df, 5))
    print("ATR from empty:", calculate_atr(empty_df, 5))

    # Note: pandas-ta often returns Series/DataFrames with NaNs at the beginning
    # where the indicator cannot be calculated due to insufficient prior data.
    # This is standard behavior.
    # The wrappers include basic checks for empty DFs or insufficient length for the period.
    # pandas-ta also expects column names to be lowercase by default for many indicators,
    # so the wrappers use .rename(columns=str.lower) or pass specific column names.
    # For the real OHLC data from Trader, we will ensure columns are named 'open', 'high', 'low', 'close'.

    # The `append=False` argument in pandas_ta calls ensures that the indicator results
    # are returned as new Series/DataFrames rather than appending to the input DataFrame.
    # This is generally cleaner for our wrapper functions.
    pass # End of example usage
