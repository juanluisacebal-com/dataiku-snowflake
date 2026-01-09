USE DATABASE DataWareHouse;
USE SCHEMA ELECTRICITY_MARKET;

-- Create unified table for electricity prices (PVPC and OMIE)
-- We use REPLACE to drop the old PVPC_PRICES if we wanted, but to keep history clean/simple for this task
-- we will just create the new table. The old one can be dropped or ignored.
-- Actually, let's just create the new table.

CREATE OR REPLACE TABLE ELECTRICITY_PRICES (
    DATE_ID TIMESTAMP_NTZ COMMENT 'Date and hour of the price',
    PRICE_VALUE FLOAT COMMENT 'Price in EUR/kWh',
    PRICE_SOURCE VARCHAR(20) COMMENT 'Source of the price: PVPC or OMIE',
    GEO_ID INT COMMENT 'Geographic ID from REE',
    GEO_NAME VARCHAR(50) COMMENT 'Geographic Name (Peninsula, Canarias, Baleares, Ceuta, Melilla)',
    INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when the row was inserted'
);

COMMENT ON TABLE ELECTRICITY_PRICES IS 'Table storing Spanish electricity prices (PVPC Retail and OMIE Wholesale) in EUR/kWh';
