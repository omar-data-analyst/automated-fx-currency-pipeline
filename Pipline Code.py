import os
import pandas as pd
import requests
from prefect import flow, task
from google.cloud import bigquery
from google.oauth2 import service_account

# ==========================================
# 1. Telegram Configuration
# ==========================================
TELEGRAM_BOT_TOKEN = "Add Your Telegram Token Here"
TELEGRAM_CHAT_ID = "Add Chat_ID Here"

def send_telegram_alert(message):
    """Send alert via Telegram on pipeline events"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# ==========================================
# 2. ETL Tasks
# ==========================================
@task(name='Get Data', retries=3, retry_delay_seconds=3)
def get_data(url):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

@task(name='Transform & Deduplicate DataFrame')
def transform_to_df(raw_data):
    date_str = raw_data.get('date')
    rates = raw_data.get('usd', {})

    df = pd.DataFrame(list(rates.items()), columns=['currency', 'rate_to_usd'])
    
    df['fetch_date'] = pd.to_datetime(date_str).date()
    
    df = df[['fetch_date', 'currency', 'rate_to_usd']]
    df = df.drop_duplicates(subset=['fetch_date', 'currency'], keep='first')
    
    print(f"Data transformed successfully. Shape: {df.shape}")
    return df

@task(name='Check of Data Quality')
def check_quality(df):
    nulls_count = df[['currency', 'rate_to_usd']].isnull().sum().sum()
    if nulls_count > 0:
        raise RuntimeError(f'Check Quality Failed: There are {nulls_count} Nulls in Essential Columns')
        
    invalid_rates = df[df['rate_to_usd'] <= 0]
    if not invalid_rates.empty:
        raise ValueError(f"Quality Check Failed: Found {len(invalid_rates)} non-positive exchange rates.")
        
    min_expected_records = 300
    if len(df) < min_expected_records:
        raise ValueError(f"Quality Check Failed: Data size ({len(df)}) is lower than expected minimum ({min_expected_records}).")
        
    print('All Data Quality Checks Passed Successfully!')

@task(name="Load to BigQuery", retries=3, retry_delay_seconds=10)
def load_to_bigquery(df, table_id):
    project_id = "qualified-sun-508411-h8"
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    key_path = os.path.join(base_dir, "pigquery key.json")
    
    if not os.path.exists(key_path):
        key_path = r"F:\Data Analystics Projects\Currency ETL Pipline\pigquery key.json"
    
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  
    print(f"Data loaded successfully to {table_id}")

# ==========================================
# 3. Main Prefect Flow
# ==========================================
@flow(name='Data_Pipeline')
def pipeline():
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
    table_id = "qualified-sun-508411-h8.currency_db.daily_rates"

    try:
        raw_data = get_data(url)
        df = transform_to_df(raw_data)
        check_quality(df)
        load_to_bigquery(df, table_id)
        
        success_msg = f"🟢 **ETL Job Success**\n\n📅 **Date:** `{df['fetch_date'].iloc[0]}`\n📊 **Records Loaded:** `{len(df)}`\n🎯 **Destination:** `{table_id}`"
        send_telegram_alert(success_msg)

    except Exception as e:
        error_msg = f"🚨 **ETL Job Failed!**\n\n❌ **Error Details:** `{str(e)}`"
        send_telegram_alert(error_msg)
        raise e

if __name__ == "__main__":
    pipeline()