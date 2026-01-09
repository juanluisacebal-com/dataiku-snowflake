USE DATABASE DataWareHouse;
USE SCHEMA ELECTRICITY_MARKET;

-- Recreating the table to fix the GEO_NAME data type (was DATE, needs to be VARCHAR)
-- Since the previous ingestion failed, the table should be empty or contain invalid data (though insert is atomic usually).
-- We can safely replace it.

CREATE OR REPLACE TABLE PVPC_PRICES (
    DATE_ID TIMESTAMP_NTZ COMMENT 'Date and hour of the price',
    PRICE_VALUE FLOAT COMMENT 'Price in EUR/MWh usually, or EUR/kWh depending on source',
    GEO_ID INT COMMENT 'Geographic ID from REE',
    GEO_NAME VARCHAR(50) COMMENT 'Geographic Name (Peninsula, Canarias, Baleares, Ceuta, Melilla)',
    INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when the row was inserted'
);

COMMENT ON TABLE PVPC_PRICES IS 'Table storing Spanish PVPC electricity prices fetched from REE API';
