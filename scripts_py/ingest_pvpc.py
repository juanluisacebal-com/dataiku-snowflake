import os
import requests
import snowflake.connector
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def get_snowflake_connection():
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    private_key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH')
    use_key_pair = os.environ.get('USE_KEY_PAIR')
    
    connect_args = {
        "account": account,
        "user": user,
        "warehouse": "COMPUTE_WH",
        "database": "DataWareHouse",
        "schema": "ELECTRICITY_MARKET"
    }

    if use_key_pair == 'true' and private_key_path:
        with open(private_key_path, "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        connect_args["private_key"] = pkb
    else:
        connect_args["password"] = password

    return snowflake.connector.connect(**connect_args)

def fetch_indicator(indicator_id, source_name):
    ree_token = os.environ.get('REE_TOKEN')
    if not ree_token:
        raise ValueError("REE_TOKEN environment variable is not set")

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Host': 'api.esios.ree.es',
        'Authorization': f'Token token="{ree_token}"'
    }
    
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    # Fetch data for current day
    url = f"https://api.esios.ree.es/indicators/{indicator_id}?start_date={date_str}T00:00&end_date={date_str}T23:59"
    
    print(f"Fetching {source_name} (Ind: {indicator_id}) from: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to fetch {source_name}: {response.status_code}")
        return []

    data = response.json()
    values = data.get('indicator', {}).get('values', [])
    
    results = []
    for item in values:
        ts_str = item.get('datetime')
        dt_val = datetime.fromisoformat(ts_str)
        
        # Convert €/MWh to €/kWh
        raw_price = item.get('value')
        price_kwh = raw_price / 1000.0 if raw_price is not None else None
        
        geo_id = item.get('geo_id')
        geo_name = item.get('geo_name')
        
        results.append((dt_val, price_kwh, source_name, geo_id, geo_name))
        
    print(f"Fetched {len(results)} records for {source_name}")
    return results

def ingest_data(conn, all_rows):
    if not all_rows:
        print("No data to ingest.")
        return

    cursor = conn.cursor()
    print(f"Ingesting {len(all_rows)} total records...")
    
    insert_query = """
    INSERT INTO ELECTRICITY_PRICES (DATE_ID, PRICE_VALUE, PRICE_SOURCE, GEO_ID, GEO_NAME)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    try:
        cursor.executemany(insert_query, all_rows)
        print("Ingestion completed successfully")
    except Exception as e:
        print(f"Error executing insert: {e}")
        raise e
    finally:
        cursor.close()

def main():
    print("Starting Electricity Data ingestion...")
    try:
        conn = get_snowflake_connection()
        print("Connected to Snowflake.")
        
        # Indicator 1001: PVPC 2.0TD (Retail)
        pvpc_rows = fetch_indicator(1001, 'PVPC')
        
        # Indicator 600: Precio mercado SPOT diario (Wholesale)
        omie_rows = fetch_indicator(600, 'OMIE')
        
        all_rows = pvpc_rows + omie_rows
        
        ingest_data(conn, all_rows)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
