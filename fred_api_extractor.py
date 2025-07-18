print("Script started!")

import os
from datetime import datetime, date
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv

# Load environment variables from .env file
# This must be called at the very beginning to ensure environment variables are loaded
load_dotenv()

# ... (previous imports and load_dotenv() call)

FRED_API_KEY = os.getenv('FRED_API_KEY')
if not FRED_API_KEY:
    raise ValueError("FRED_API_KEY not found in .env file or environment variables. Please set it.")

fred = Fred(api_key=FRED_API_KEY)

# You can add a print statement here to confirm initialization (optional)
# print("FRED API client initialized successfully.")

# ... (previous code, including fred = Fred(api_key=FRED_API_KEY))

# Define the FRED series we'll integrate with their metadata
# These are the series IDs you identified in your project description
FRED_SERIES_METADATA = {
    "GDP": {
        "name": "Gross Domestic Product (Nominal)",
        "description": "Gross Domestic Product, 1 Decimal",
        "units": "Billions of Dollars",
        "frequency": "Quarterly",
        "seasonal_adjustment": "Seasonally Adjusted Annual Rate",
        "observation_start": "1947-01-01" # Approx. earliest available data
    },
    "GDPC1": {
        "name": "Real Gross Domestic Product",
        "description": "Real Gross Domestic Product, 3 Decimal",
        "units": "Billions of Chained 2017 Dollars",
        "frequency": "Quarterly",
        "seasonal_adjustment": "Seasonally Adjusted Annual Rate",
        "observation_start": "1947-01-01"
    },
    "CPIAUCSL": {
        "name": "Consumer Price Index for All Urban Consumers: All Items",
        "description": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average, Seasonally Adjusted, Index 1982-1984=100",
        "units": "Index 1982-1984=100",
        "frequency": "Monthly",
        "seasonal_adjustment": "Seasonally Adjusted",
        "observation_start": "1947-01-01"
    },
    "UNRATE": {
        "name": "Unemployment Rate",
        "description": "Unemployment Rate, Seasonally Adjusted",
        "units": "Percent",
        "frequency": "Monthly",
        "seasonal_adjustment": "Seasonally Adjusted",
        "observation_start": "1948-01-01"
    },
    "INDPRO": {
        "name": "Industrial Production Index",
        "description": "Industrial Production Index, Seasonally Adjusted",
        "units": "Index 2017=100",
        "frequency": "Monthly",
        "seasonal_adjustment": "Seasonally Adjusted",
        "observation_start": "1919-01-01"
    },
    "DCOILWTICO": {
        "name": "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
        "description": "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma, Dollars per Barrel",
        "units": "Dollars per Barrel",
        "frequency": "Daily",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "observation_start": "1986-01-02"
    },
    "MHHNGSP": {
        "name": "Henry Hub Natural Gas Spot Price",
        "description": "Henry Hub Natural Gas Spot Price, Dollars per Million BTU",
        "units": "Dollars per Million BTU",
        "frequency": "Monthly",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "observation_start": "1997-01-01"
    },
    "FEDFUNDS": {
        "name": "Federal Funds Effective Rate",
        "description": "Federal Funds Effective Rate, Percent",
        "units": "Percent",
        "frequency": "Daily",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "observation_start": "1954-07-01"
    }
}

# ... (previous code, including FRED_SERIES_METADATA)

def get_fred_series_data(series_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Fetches time series data for a given FRED series ID.

    Args:
        series_id (str): The FRED series ID (e.g., 'GDP', 'UNRATE').
        start_date (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to None (earliest available data).
        end_date (str, optional): End date in 'YYYY-MM-DD' format. Defaults to None (latest available data).

    Returns:
        pd.DataFrame: A DataFrame with 'date', 'value', and 'series_id' columns,
                      or an empty DataFrame if no data found or an error occurs.
    """
    if series_id not in FRED_SERIES_METADATA:
        print(f"Warning: Series ID '{series_id}' not found in predefined metadata. Skipping.")
        return pd.DataFrame()

    print(f"Attempting to fetch data for series: {series_id}")
    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
        
        if data is None or data.empty:
            print(f"No data or empty data returned for series: {series_id}")
            return pd.DataFrame()

        # get_series returns a Pandas Series, where index is date, and values are observations.
        # We need to convert this to a DataFrame with explicit 'date' and 'value' columns.
        df = data.reset_index()
        df.columns = ['date', 'value']
        df['series_id'] = series_id # Add series_id column for easier identification later in the pipeline
        
        print(f"Successfully fetched {len(df)} observations for {series_id}.")
        return df
    except Exception as e:
        print(f"Error fetching data for {series_id}: {e}")
        return pd.DataFrame()

def extract_all_fred_data(start_date: str = None, end_date: str = None) -> dict:
    """
    Extracts data for all predefined FRED series.

    Args:
        start_date (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to None.
        end_date (str, optional): End date in 'YYYY-MM-DD' format. Defaults to None.

    Returns:
        dict: A dictionary where keys are series_ids and values are Pandas DataFrames.
    """
    all_data = {}
    for series_id in FRED_SERIES_METADATA.keys():
        df = get_fred_series_data(series_id, start_date, end_date)
        if not df.empty:
            all_data[series_id] = df
    return all_data

def get_series_metadata() -> pd.DataFrame:
    """
    Returns a DataFrame containing metadata for all predefined FRED series.
    This is useful for loading into a database dimension table.
    """
    metadata_list = []
    for series_id, details in FRED_SERIES_METADATA.items():
        metadata_list.append({
            "series_id": series_id,
            "series_name": details["name"],
            "description": details["description"],
            "units": details["units"],
            "frequency": details["frequency"],
            "seasonal_adjustment": details.get("seasonal_adjustment", "N/A"),
            "observation_start": details.get("observation_start", None) # Can be None for older series
        })
    return pd.DataFrame(metadata_list)


if __name__ == "__main__":
    # This block runs only when fred_api_extractor.py is executed directly
    print("--- Testing fred_api_extractor.py ---")

    # Example 1: Fetching historical data for GDP (e.g., from 2000 onwards)
    print("\n--- Testing get_fred_series_data for GDP (from 2000-01-01) ---")
    gdp_data = get_fred_series_data("GDP", start_date="2000-01-01")
    if not gdp_data.empty:
        print("GDP Data (first 5 rows):")
        print(gdp_data.head())
        print(f"Total GDP observations: {len(gdp_data)}")
    else:
        print("Failed to fetch GDP data.")

    # Example 2: Fetching latest CPI data (e.g., last 12 months)
    print("\n--- Testing get_fred_series_data for CPI (last 12 months) ---")
    one_year_ago = (datetime.now() - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
    cpi_data = get_fred_series_data("CPIAUCSL", start_date=one_year_ago)
    if not cpi_data.empty:
        print("CPI Data (last 5 rows):")
        print(cpi_data.tail())
        print(f"Total CPI observations: {len(cpi_data)}")
    else:
        print("Failed to fetch CPI data.")

    # Example 3: Fetching all predefined data for a recent period (e.g., last 2 years)
    print("\n--- Testing extract_all_fred_data (last 2 years) ---")
    two_years_ago = (datetime.now() - pd.DateOffset(years=2)).strftime('%Y-%m-%d')
    all_extracted_data = extract_all_fred_data(start_date=two_years_ago)
    print(f"\nExtracted data for {len(all_extracted_data)} series in total.")
    for series_id, df in all_extracted_data.items():
        print(f"  {series_id}: {len(df)} observations")

    # Example 4: Get and print series metadata
    print("\n--- Testing get_series_metadata ---")
    metadata_df = get_series_metadata()
    print("Series Metadata (first 3 rows):")
    print(metadata_df.head(3))
    print(f"Total series metadata entries: {len(metadata_df)}")
    
    print("Script ended!")