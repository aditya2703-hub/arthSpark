-- create_tables.sql

-- Table to store metadata about each FRED economic series
CREATE TABLE IF NOT EXISTS series_metadata (
    series_id VARCHAR(50) PRIMARY KEY, -- FRED Series ID (e.g., 'GDP', 'CPIAUCSL')
    series_name VARCHAR(255) NOT NULL,
    description TEXT,
    units VARCHAR(100),
    frequency VARCHAR(50),
    seasonal_adjustment VARCHAR(100), -- e.g., 'Seasonally Adjusted', 'Not Seasonally Adjusted'
    observation_start DATE,           -- Earliest observation date available from FRED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store the actual economic time series data
CREATE TABLE IF NOT EXISTS economic_time_series_data (
    id SERIAL PRIMARY KEY, -- Auto-incrementing unique ID for each record
    series_id VARCHAR(50) NOT NULL, -- Foreign key referencing series_metadata
    date DATE NOT NULL,             -- The date of the observation
    value NUMERIC(20, 10) NOT NULL, -- The economic value, NUMERIC(precision, scale) for accuracy
    processed_at TIMESTAMP NOT NULL, -- When this data point was processed by our pipeline
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Define a unique constraint to prevent duplicate entries for the same series on the same date.
    -- This is crucial for our UPSERT logic later.
    UNIQUE (series_id, date),

    -- Define the foreign key relationship
    CONSTRAINT fk_series_id
        FOREIGN KEY (series_id)
        REFERENCES series_metadata (series_id)
        ON UPDATE CASCADE  -- If a series_id changes in metadata, update here
        ON DELETE RESTRICT -- Prevent deleting metadata if data exists for it
);

-- Optional: Create indexes for faster data retrieval
CREATE INDEX IF NOT EXISTS idx_economic_time_series_data_series_id
ON economic_time_series_data (series_id);

CREATE INDEX IF NOT EXISTS idx_economic_time_series_data_date
ON economic_time_series_data (date);

-- Optional: Index on both series_id and date for common queries
CREATE INDEX IF NOT EXISTS idx_economic_time_series_data_series_id_date
ON economic_time_series_data (series_id, date DESC); -- DESC for common time-series queries