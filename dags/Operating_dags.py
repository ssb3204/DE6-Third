from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import io
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable

# 기온 데이터를 가져와 raw_data 테이블에 적재하는 함수
def fetch_and_insert(ds, **kwargs):
    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    tm2 = exec_date.strftime("%Y%m%d")
    tm1 = (exec_date - timedelta(days=2)).strftime("%Y%m%d")

    print(f"[INFO] API 요청 기간: {tm1} ~ {tm2}")

    url = "https://apihub.kma.go.kr/api/typ01/url/sts_ta.php"
    api_key = Variable.get("temperature_api_key")

    params = {
        "tm1": tm1,
        "tm2": tm2,
        "stn_id": 0,
        "disp": "0",
        "authKey": api_key
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

    df["YMD"] = pd.to_datetime(df["YMD"], format="%Y%m%d", errors="coerce").dt.strftime('%Y-%m-%d')
    for col in ["STN_ID", "LAT", "LON", "ALTD", "TA_DAVG", "TMX_DD", "TMN_DD", "MRNG_TMN", "DYTM_TMX", "NGHT_TMN"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    for col in ["TMX_OCUR_TMA", "TMN_OCUR_TMA", "MRNG_TMN_OCUR_TMA", "DYTM_TMX_OCUR_TMA", "NGHT_TMN_OCUR_TMA"]:
        df[col] = df[col].astype(str).str.zfill(4)

    conn = BaseHook.get_connection("snowflake_conn")
    extra = conn.extra_dejson
    sf_conn = snowflake.connector.connect(
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
        conn=sf_conn,
        df=df,
        table_name='DAILY_DAG_TEST',
        schema=conn.schema
    )
    sf_conn.close()

# FACT 테이블에 없는 데이터를 적재하는 함수
def insert_new_fact_data(ds, **kwargs):
    today = datetime.strptime(ds, "%Y-%m-%d")
    start_date = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    print(f"[INFO] FACT 비교 기간: {start_date} ~ {end_date}")

    conn = BaseHook.get_connection("snowflake_conn")
    extra = conn.extra_dejson
    sf_conn = snowflake.connector.connect(
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

    insert_query = f"""
        INSERT INTO WEATHER.ANALYTICS_DATA.FACT_TEST (
            YMD, STN_ID, LAT, LON, ALTD,
            TA_DAVG, TMX_DD, TMX_OCUR_TMA,
            TMN_DD, TMN_OCUR_TMA
        )
        SELECT 
            YMD, STN_ID, LAT, LON, ALTD,
            TA_DAVG, TMX_DD, TMX_OCUR_TMA,
            TMN_DD, TMN_OCUR_TMA
        FROM WEATHER.RAW_DATA.DAILY_DAG_TEST raw
        WHERE YMD BETWEEN '{start_date}' AND '{end_date}'
          AND NOT EXISTS (
            SELECT 1 FROM WEATHER.ANALYTICS_DATA.FACT_TEST fact
            WHERE raw.YMD = fact.YMD AND raw.STN_ID = fact.STN_ID
        )
    """

    with sf_conn.cursor() as cur:
        cur.execute(insert_query)
        cur.execute(f"""
            SELECT DISTINCT YMD FROM WEATHER.ANALYTICS_DATA.FACT_TEST
            WHERE YMD BETWEEN '{start_date}' AND '{end_date}' ORDER BY YMD
        """)
        for row in cur.fetchall():
            print(f"[INFO] FACT_TEST 적재 날짜: {row[0]}")

    sf_conn.close()

# DAG 정의
default_args = {
    'owner': 'ryan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_weather_fact_pipeline',
    start_date=datetime(2025, 6, 9),
    schedule_interval='0 7 * * *',
    catchup=True,
    tags=['weather', 'raw_to_fact'],
    default_args=default_args,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_insert_weather_data',
        python_callable=fetch_and_insert,
        provide_context=True,
    )

    fact_task = PythonOperator(
        task_id='insert_new_fact_data',
        python_callable=insert_new_fact_data,
        provide_context=True,
    )

    fetch_task >> fact_task
