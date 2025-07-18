# main.py
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

# Import our custom modules
from fred_api_extractor import extract_all_fred_data, get_series_metadata
from data_transformer import transform_fred_data
from db_loader import load_series_metadata, load_economic_time_series_data, get_db_engine # get_db_engine for last_load_date

def get_last_load_date(engine, series_id):
    """
    Retrieves the latest 'date' for a given series_id from the database.
    Returns None if no data exists for the series.
    """
    query = text(f"SELECT MAX(date) FROM economic_time_series_data WHERE series_id = :series_id")
    with engine.connect() as connection:
        result = connection.execute(query, {'series_id': series_id}).scalar_one_or_none()
        return result

def run_etl_pipeline():
    """
    Executes the end-to-end ETL pipeline for ArthSpark.
    Extracts data, transforms it, and loads it into the PostgreSQL database.
    Includes logic for incremental loading.
    """
    print("\n--- ArthSpark ETL Pipeline Started ---")
    start_time = datetime.now()
    engine = get_db_engine() # Get engine once for the entire pipeline run

    # 1. Load Series Metadata (Always run to ensure metadata is up-to-date)
    print("\n[STEP 1/3] Loading Series Metadata...")
    metadata_df = get_series_metadata()
    load_series_metadata(metadata_df)
    print("Series Metadata Loading Complete.")

    # 2. Extract and Load Economic Time Series Data (Incremental Logic)
    print("\n[STEP 2/3] Extracting and Loading Economic Time Series Data...")
    all_series_ids = metadata_df['series_id'].tolist() # Get all series IDs from loaded metadata

    for series_id in all_series_ids:
        print(f"\nProcessing series: {series_id}")
        
        # Determine start date for incremental load
        last_load_date = get_last_load_date(engine, series_id)
        
        # FRED data often has revisions, so it's good practice to fetch a little overlap.
        # Fetch data starting from (last_load_date - say, 6 months) or from earliest if no previous load.
        if last_load_date:
            # Fetch from slightly before the last load date to capture any revisions
            fetch_start_date = (last_load_date - timedelta(days=90)).strftime('%Y-%m-%d') # e.g., 3 months back
            print(f"  Last loaded date for {series_id}: {last_load_date}. Fetching data from: {fetch_start_date}")
        else:
            # If no data exists, fetch all available historical data (from FRED_SERIES_METADATA's start date, or let API decide)
            fetch_start_date = None # Let get_fred_series_data default to earliest if None
            print(f"  No previous data found for {series_id}. Fetching all available historical data.")

        # Extract
        raw_data_df = extract_all_fred_data(start_date=fetch_start_date)
        series_data_df = raw_data_df.get(series_id)

        if series_data_df is None or series_data_df.empty:
            print(f"  No new or historical data to process for {series_id}.")
            continue

        # Transform
        transformed_data_df = transform_fred_data(series_data_df)

        if transformed_data_df.empty:
            print(f"  Transformed data for {series_id} is empty. Skipping load.")
            continue

        # Load
        load_economic_time_series_data(transformed_data_df)
        print(f"  Finished processing {series_id}.")

    print("\nEconomic Time Series Data Loading Complete.")

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n--- ArthSpark ETL Pipeline Finished in {duration} ---")

if __name__ == "__main__":
    run_etl_pipeline()