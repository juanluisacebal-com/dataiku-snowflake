-- Create DataWareHouse database if it doesn't exist
CREATE OR REPLACE DATABASE DataWareHouse;

-- Set the context to the new database
USE DATABASE DataWareHouse;

-- Create ELECTRICITY_MARKET schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ELECTRICITY_MARKET;

-- Set the context to the new schema
USE SCHEMA ELECTRICITY_MARKET;

-- Create PVPC_PRICES table to store electricity prices
CREATE TABLE IF NOT EXISTS PVPC_PRICES (
    DATE_ID TIMESTAMP_NTZ COMMENT 'Date and hour of the price',
    PRICE_VALUE FLOAT COMMENT 'Price in EUR/MWh usually, or EUR/kWh depending on source',
    GEO_ID INT COMMENT 'Geographic ID from REE',
    GEO_NAME DATE COMMENT 'Geographic Name (Peninsula, Canarias, Baleares, Ceuta, Melilla)',
    INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when the row was inserted'
);

COMMENT ON TABLE PVPC_PRICES IS 'Table storing Spanish PVPC electricity prices fetched from REE API';
