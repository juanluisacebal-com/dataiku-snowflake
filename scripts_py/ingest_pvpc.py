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

def fetch_pvpc_prices():
    ree_token = os.environ.get('REE_TOKEN')
    if not ree_token:
        raise ValueError("REE_TOKEN environment variable is not set")

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Host': 'api.esios.ree.es',
        'Authorization': f'Token token="{ree_token}"'
    }
    
    # Indicator 1001 is "Término de facturación de energía activa del PVPC 2.0TD"
    # We fetch for the current day. ESIOS API by default returns latest data if date not specified, 
    # or strict date range. Let's start with default call which usually gives today/tomorrow.
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    # Using 23:59 to get the full day
    url = f"https://api.esios.ree.es/indicators/1001?start_date={date_str}T00:00&end_date={date_str}T23:59"
    
    print(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    data = response.json()
    return data

def ingest_data(conn, data):
    cursor = conn.cursor()
    
    # ESIOS Indicator 1001 response structure
    # data['indicator']['values'] is a list of dicts
    values = data.get('indicator', {}).get('values', [])
    
    if not values:
        print("No values found in API response")
        return

    print(f"Found {len(values)} records to ingest")
    
    insert_query = """
    INSERT INTO PVPC_PRICES (DATE_ID, PRICE_VALUE, GEO_ID, GEO_NAME)
    VALUES (%s, %s, %s, %s)
    """
    
    rows_to_insert = []
    
    for item in values:
        # item['datetime'] is like '2023-10-27T00:00:00.000+02:00'
        # item['value'] is the price
        # item['geo_id'] 
        # item['geo_name']
        
        # We need to handle timezone or just store as is. Snowflake TIMESTAMP_NTZ assumes wall time.
        # ESIOS returns ISO format.
        ts_str = item.get('datetime')
        # Remove timezone offset for NTZ or parse strictly. 
        # Simpler for this demo: keep string and let Snowflake parse or python parse.
        # Python isoformat parsing:
        dt_val = datetime.fromisoformat(ts_str)
        
        price = item.get('value')
        geo_id = item.get('geo_id')
        geo_name = item.get('geo_name')
        
        rows_to_insert.append((dt_val, price, geo_id, geo_name))

    try:
        cursor.executemany(insert_query, rows_to_insert)
        print("Ingestion completed successfully")
    except Exception as e:
        print(f"Error executing insert: {e}")
        raise e
    finally:
        cursor.close()

def main():
    print("Starting PVPC ingestion...")
    try:
        conn = get_snowflake_connection()
        print("Connected to Snowflake.")
        
        data = fetch_pvpc_prices()
        print("Fetched data from REE.")
        
        ingest_data(conn, data)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
