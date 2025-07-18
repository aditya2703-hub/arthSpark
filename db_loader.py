# db_loader.py
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from psycopg2 import sql # For safely constructing SQL queries, specifically the ON CONFLICT clause
from datetime import datetime, date # Ensure both datetime and date are imported

# Load environment variables from .env file
load_dotenv()

# --- Database Configuration (from .env) ---
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --- Check for missing DB credentials ---
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("One or more database credentials (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD) not found in .env file.")

# --- Database Connection String for SQLAlchemy ---
# postgresql+psycopg2 is the dialect + driver combination
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.
    This function will be called whenever a database operation is needed.
    """
    try:
        engine = create_engine(DATABASE_URL)
        # Attempt to connect to test the engine immediately
        with engine.connect() as connection:
            print("Successfully connected to the database!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        print(f"Please check your .env database credentials and ensure PostgreSQL is running.")
        raise

def load_series_metadata(metadata_df: pd.DataFrame):
    """
    Loads or updates series metadata into the series_metadata table.
    Uses ON CONFLICT (series_id) DO UPDATE to handle existing records.
    Parameters are passed as a dictionary for robustness.
    """
    if metadata_df.empty:
        print("Warning: No metadata to load. Skipping load_series_metadata.")
        return

    engine = get_db_engine()
    print(f"Loading {len(metadata_df)} series metadata records...")
    try:
        with engine.connect() as connection:
            for index, row in metadata_df.iterrows():
                # Explicitly handle observation_start: convert to datetime.date or None
                # The .date() call ensures it's a date object, not a full datetime object
                obs_start_date_obj = None
                if pd.notna(row['observation_start']) and isinstance(row['observation_start'], str):
                    try:
                        obs_start_date_obj = datetime.strptime(row['observation_start'], '%Y-%m-%d').date()
                    except ValueError:
                        # Log if parsing fails, but proceed with None
                        print(f"Warning: Could not parse observation_start '{row['observation_start']}' for {row['series_id']}. Setting to NULL.")
                        obs_start_date_obj = None

                # Use current timestamp for both created_at (for new inserts) and updated_at
                current_timestamp = datetime.now()

                # Define the SQL INSERT statement using named parameters
                insert_stmt_sql = """
                    INSERT INTO series_metadata (series_id, series_name, description, units, frequency, seasonal_adjustment, observation_start, created_at, updated_at)
                    VALUES (:series_id, :series_name, :description, :units, :frequency, :seasonal_adjustment, :observation_start, :created_at, :updated_at)
                    ON CONFLICT (series_id) DO UPDATE SET
                        series_name = EXCLUDED.series_name,
                        description = EXCLUDED.description,
                        units = EXCLUDED.units,
                        frequency = EXCLUDED.frequency,
                        seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                        observation_start = EXCLUDED.observation_start,
                        updated_at = EXCLUDED.updated_at;
                """
                # Execute using text() and pass parameters as a dictionary
                connection.execute(
                    text(insert_stmt_sql),
                    {
                        'series_id': row['series_id'],
                        'series_name': row['series_name'],
                        'description': row['description'],
                        'units': row['units'],
                        'frequency': row['frequency'],
                        'seasonal_adjustment': row['seasonal_adjustment'],
                        'observation_start': obs_start_date_obj, # Pass the date object or None
                        'created_at': current_timestamp,
                        'updated_at': current_timestamp
                    }
                )
            connection.commit() # Commit the transaction after all rows are processed
        print("Successfully loaded/updated series metadata.")
    except Exception as e:
        print(f"Error loading series metadata: {e}")
        # Re-raise the exception to provide full traceback for debugging the root cause
        raise

def load_economic_time_series_data(data_df: pd.DataFrame):
    """
    Loads or updates economic time series data into the economic_time_series_data table.
    Uses raw SQL INSERT ... ON CONFLICT DO UPDATE for efficient UPSERT.
    Parameters are passed as a dictionary for robustness.
    """
    if data_df.empty:
        print("Warning: No time series data to load. Skipping load_economic_time_series_data.")
        return

    engine = get_db_engine()
    print(f"Loading {len(data_df)} time series records for series_id: {data_df['series_id'].iloc[0]}...")
    try:
        with engine.connect() as connection:
            for index, row in data_df.iterrows():
                # Use current timestamp for updated_at
                current_timestamp = datetime.now()

                # Define the SQL INSERT statement using named parameters
                insert_stmt_sql = """
                    INSERT INTO economic_time_series_data (series_id, date, value, processed_at, updated_at)
                    VALUES (:series_id, :date, :value, :processed_at, :updated_at)
                    ON CONFLICT (series_id, date) DO UPDATE SET
                        value = EXCLUDED.value,
                        processed_at = EXCLUDED.processed_at,
                        updated_at = EXCLUDED.updated_at;
                """
                # Execute using text() and pass parameters as a dictionary
                connection.execute(
                    text(insert_stmt_sql),
                    {
                        'series_id': row['series_id'],
                        'date': row['date'].date(), # Ensure it's a date object, not a full datetime
                        'value': row['value'],
                        'processed_at': row['processed_at'],
                        'updated_at': current_timestamp
                    }
                )
            connection.commit() # Commit the transaction after all rows are processed
        print(f"Successfully loaded/updated time series data for {data_df['series_id'].iloc[0]}.")

    except Exception as e:
        print(f"Error loading economic time series data: {e}")
        raise

# --- Example Usage (for testing this module directly) ---
if __name__ == "__main__":
    # Import necessary modules for testing
    from fred_api_extractor import get_series_metadata, get_fred_series_data
    from data_transformer import transform_fred_data

    print("\n--- Testing db_loader.py data loading functions ---")

    # --- Test 1: Load Metadata ---
    print("\n--- Testing Metadata Load ---")
    metadata_to_load = get_series_metadata()
    load_series_metadata(metadata_to_load)

    # --- Test 2: Load Economic Time Series Data (e.g., GDP) ---
    print("\n--- Testing Time Series Data Load (GDP) ---")
    # Get a smaller subset of data for testing the loader efficiently
    raw_gdp_data = get_fred_series_data("GDP", start_date="2020-01-01")
    if not raw_gdp_data.empty:
        transformed_gdp_data = transform_fred_data(raw_gdp_data)
        load_economic_time_series_data(transformed_gdp_data)
    else:
        print("Skipping GDP time series load test: No raw data obtained.")

    # --- Test 3: Load Economic Time Series Data (e.g., CPI) ---
    print("\n--- Testing Time Series Data Load (CPI) ---")
    raw_cpi_data = get_fred_series_data("CPIAUCSL", start_date="2023-01-01")
    if not raw_cpi_data.empty:
        transformed_cpi_data = transform_fred_data(raw_cpi_data)
        load_economic_time_series_data(transformed_cpi_data)
    else:
        print("Skipping CPI time series load test: No raw data obtained.")

    print("\nAll db_loader.py tests concluded. Please verify data in PostgreSQL.")
    # You can manually verify the data in your PostgreSQL database using a tool like pgAdmin
    # Example queries:
    # SELECT * FROM series_metadata;
    # SELECT * FROM economic_time_series_data WHERE series_id = 'GDP' ORDER BY date DESC LIMIT 5;
    # SELECT COUNT(*) FROM economic_time_series_data;