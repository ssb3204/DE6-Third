from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import io
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable

def fetch_and_insert(ds, **kwargs):
    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    tm2 = exec_date.strftime("%Y%m%d")
    tm1 = (exec_date - timedelta(days=2)).strftime("%Y%m%d")

    print(f"checking date: {tm1}-{tm2}")
    auth_key = Variable.get("temperature_api_key")

    url = "https://apihub.kma.go.kr/api/typ01/url/sts_ta.php"
    params = {
        "tm1": tm1,
        "tm2": tm2,
        "stn_id": 0,
        "disp": "0",
        "authKey": auth_key
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    if not response.text.strip():
        raise Exception("API 응답이 비어 있습니다.")

    df = pd.read_csv(io.StringIO(response.text), comment='#', encoding='utf-8-sig', header=None)

    df.columns = [
        "YMD", "STN_ID", "LAT", "LON", "ALTD",
        "TA_DAVG", "TMX_DD", "TMX_OCUR_TMA",
        "TMN_DD", "TMN_OCUR_TMA",
        "MRNG_TMN", "MRNG_TMN_OCUR_TMA",
        "DYTM_TMX", "DYTM_TMX_OCUR_TMA",
        "NGHT_TMN", "NGHT_TMN_OCUR_TMA",
        "EXTRA_COL"
    ]
    df.drop(columns=["EXTRA_COL"], inplace=True)

    df["YMD"] = pd.to_datetime(df["YMD"], format="%Y%m%d", errors="coerce")
    df["YMD"] = df["YMD"].dt.strftime('%Y-%m-%d')
    df["STN_ID"] = pd.to_numeric(df["STN_ID"], errors='coerce')
    df["LAT"] = pd.to_numeric(df["LAT"], errors='coerce').round(5)
    df["LON"] = pd.to_numeric(df["LON"], errors='coerce').round(5)
    df["ALTD"] = pd.to_numeric(df["ALTD"], errors='coerce').round(1)
    df["TA_DAVG"] = pd.to_numeric(df["TA_DAVG"], errors='coerce').round(1)
    df["TMX_DD"] = pd.to_numeric(df["TMX_DD"], errors='coerce').round(1)
    df["TMN_DD"] = pd.to_numeric(df["TMN_DD"], errors='coerce').round(1)
    df["MRNG_TMN"] = pd.to_numeric(df["MRNG_TMN"], errors='coerce').round(1)
    df["DYTM_TMX"] = pd.to_numeric(df["DYTM_TMX"], errors='coerce').round(1)
    df["NGHT_TMN"] = pd.to_numeric(df["NGHT_TMN"], errors='coerce').round(1)

    for col in ["TMX_OCUR_TMA", "TMN_OCUR_TMA", "MRNG_TMN_OCUR_TMA", "DYTM_TMX_OCUR_TMA", "NGHT_TMN_OCUR_TMA"]:
        df[col] = df[col].astype(str).str.zfill(4)

    # Snowflake 연결 정보 불러오기
    conn = BaseHook.get_connection("snowflake_conn")
    extra = conn.extra_dejson

    snowflake_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=extra["account"],
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema=conn.schema,
        role=extra.get("role"),
        region=extra.get("region"),
        insecure_mode=extra.get("insecure_mode", False)
    )

    write_pandas(
        conn=snowflake_conn,
        df=df,
        table_name='DAILY_DAG_TEST',
        schema=conn.schema
    )

    snowflake_conn.close()

with DAG(
    dag_id='daily_temp_update_solo',
    schedule_interval='0 7 * * *',
    start_date=datetime(2025, 6, 9),
    catchup=True,
    tags=['weather', 'daily', 'snowflake'],
    default_args={
        'owner': 'ryan',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    max_active_runs=1,
) as dag:

    fetch_and_insert_weather_data = PythonOperator(
        task_id='fetch_and_insert_weather_data',
        python_callable=fetch_and_insert,
        provide_context=True,
    )
