# data_transformer.py
import pandas as pd
import numpy as np
from datetime import datetime # <--- ADD THIS LINE

def transform_fred_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs essential cleaning and standardization on raw FRED data.

    Args:
        df (pd.DataFrame): The raw DataFrame from fred_api_extractor.py
                           expected to have 'date', 'value', and 'series_id' columns.

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame.
    """
    if df.empty:
        print("Warning: Empty DataFrame provided for transformation. Returning empty DataFrame.")
        return pd.DataFrame()

    # Make a copy to avoid SettingWithCopyWarning
    transformed_df = df.copy()

    # 1. Date Parsing: Ensure 'date' column is in datetime format
    #    It should already be in datetime from fredapi, but this is a good safeguard.
    transformed_df['date'] = pd.to_datetime(transformed_df['date'])

    # 2. Numeric Conversion: Ensure 'value' column is numeric
    #    Use errors='coerce' to turn non-numeric values into NaN
    transformed_df['value'] = pd.to_numeric(transformed_df['value'], errors='coerce')

    # 3. Missing Value Handling: Strategically address NaNs
    #    For time series, common strategies are forward-fill, backward-fill, or interpolation.
    #    The best method depends on the nature of the data and its frequency.
    #    Let's use forward-fill (ffill) for simplicity, which propagates last valid observation forward.
    #    You might consider more sophisticated methods (e.g., interpolation for daily data) later.
    original_nan_count = transformed_df['value'].isnull().sum()
    if original_nan_count > 0:
        print(f"  Info: Found {original_nan_count} missing values in '{transformed_df['series_id'].iloc[0]}' before handling.")
        # Group by series_id if processing multiple series at once (though extractor gives one DF per series)
        # This is more robust if you ever combine DFs before transformation.
        transformed_df['value'] = transformed_df.groupby('series_id')['value'].ffill()
        transformed_df['value'] = transformed_df.groupby('series_id')['value'].bfill() # fill any leading NaNs
        
        # If still NaNs after ffill/bfill (e.g., entirely empty series), drop them.
        # Or you could log these and decide based on the data.
        final_nan_count = transformed_df['value'].isnull().sum()
        if final_nan_count > 0:
             print(f"  Warning: {final_nan_count} missing values remain after ffill/bfill for '{transformed_df['series_id'].iloc[0]}'. These rows will be dropped.")
             transformed_df.dropna(subset=['value'], inplace=True)
             
    # 4. Remove duplicates based on (series_id, date) - important for UPSERT later
    #    FRED data should ideally not have duplicates for a given date and series, but good to ensure.
    initial_rows = len(transformed_df)
    transformed_df.drop_duplicates(subset=['series_id', 'date'], inplace=True)
    if len(transformed_df) < initial_rows:
        print(f"  Info: Removed {initial_rows - len(transformed_df)} duplicate rows for '{transformed_df['series_id'].iloc[0]}'.")

    # 5. Optional: Add a timestamp for when the data was processed
    transformed_df['processed_at'] = datetime.now()

    print(f"Transformation complete for series '{transformed_df['series_id'].iloc[0]}'. Total observations: {len(transformed_df)}")
    return transformed_df[['series_id', 'date', 'value', 'processed_at']] # Select/order columns explicitly

# ... (previous code for transform_fred_data function)

if __name__ == "__main__":
    from fred_api_extractor import get_fred_series_data, extract_all_fred_data # Import needed functions

    print("--- Testing data_transformer.py ---")

    # Test with a single series (e.g., GDP)
    print("\nTesting transformation for GDP data:")
    raw_gdp_data = get_fred_series_data("GDP", start_date="2000-01-01")
    if not raw_gdp_data.empty:
        print(f"Raw GDP data head:\n{raw_gdp_data.head()}")
        transformed_gdp_data = transform_fred_data(raw_gdp_data)
        print(f"Transformed GDP data head:\n{transformed_gdp_data.head()}")
        print(f"Transformed GDP data tail:\n{transformed_gdp_data.tail()}")
        print(f"Transformed GDP data info:\n")
        transformed_gdp_data.info()
    else:
        print("Could not get raw GDP data for transformation test.")

    # Test with a series that might have NaNs (though FRED data is usually clean)
    # For demonstration, let's create a dummy DataFrame with NaNs
    print("\nTesting transformation with dummy NaN data:")
    dummy_data = pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01']),
        'value': [100.0, np.nan, 102.0, np.nan],
        'series_id': ['DUMMY_SERIES'] * 4
    })
    print(f"Dummy data before transformation:\n{dummy_data}")
    transformed_dummy_data = transform_fred_data(dummy_data)
    print(f"Dummy data after transformation:\n{transformed_dummy_data}")
    transformed_dummy_data.info()