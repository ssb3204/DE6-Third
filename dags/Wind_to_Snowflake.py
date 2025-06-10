from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
from urllib.request import urlopen
import io
import snowflake.connector
from airflow.models import Variable
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def fetch_and_upload_wind_data(execution_date_str: str):
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()

    # API URL 구성
    domain = Variable.get("wind_api_domain")
    stn_id = Variable.get("wind_api_stn_id")
    option = Variable.get("wind_api_option")
    auth = Variable.get("wind_api_key")
    tm = f"tm1={execution_date.strftime('%Y%m%d')}&tm2={execution_date.strftime('%Y%m%d')}&"
    url = domain + tm + stn_id + option + auth

    column_names = [
        "YMD", "STN_ID", "LAT", "LON", "ALTD",
        "WS_DAVG", "WS_INS_MAX", "WS_INS_MAX_OCUR_TMA", "WD_INS_MAX",
        "WS_MAX", "WS_MAX_OCUR_TMA", "WD_MAX", "WD_FRQ", "WS_MIX", "WD_MIX"
    ]

    try:
        with urlopen(url) as response:
            bytes_data = response.read()
            text_data = bytes_data.decode('euc-kr')
            df = pd.read_fwf(io.StringIO(text_data), skiprows=1, header=None, names=column_names)
            df["YMD"] = pd.to_datetime(df["YMD"].astype(str), format="%Y%m%d").dt.date
            df.replace("=", "", regex=True, inplace=True)

        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
        conn = hook.get_conn()

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name='DAILY_WIND_DAG_TEST',
            #table_name='RAW_DAILY_WIND',
            schema=hook.schema,
            database=hook.database
        )
        conn.close()
        print(f"✅ {execution_date} 적재 성공: {nrows} rows")
    except Exception as e:
        print(f"❌ {execution_date} 적재 실패: {e}")
        raise

with DAG(
    dag_id='daily_wind_to_snowflake',
    default_args=default_args,
    #start_date=datetime(2024, 6, 5),
    start_date=datetime(2025, 6, 5),
    schedule_interval='30 1 * * *',
    catchup=True,
    tags=['weather', 'snowflake', 'wind'],
) as dag:

    daily_task = PythonOperator(
        task_id='upload_daily_wind',
        python_callable=fetch_and_upload_wind_data,
        op_args=["{{ ds }}"],  # ds = execution_date (YYYY-MM-DD)
    )
    
    insert_fact_task = SnowflakeOperator(
        task_id='insert_fact_filtered',
        sql="""
            BEGIN;
            
            INSERT INTO WEATHER.ANALYTICS_DATA.FACT_DAILY_WIND_DAG_TEST(
                    YMD, STN_ID, WS_DAVG, WS_MAX, WS_MAX_OCUR_TMA)
            SELECT YMD, STN_ID, WS_DAVG, WS_MAX, WS_MAX_OCUR_TMA
            FROM WEATHER.RAW_DATA.DAILY_WIND_DAG_TEST
            WHERE YMD=TO_DATE('{{ ds }}')
            AND WS_DAVG != 99.9
            AND WS_MAX != -99.9
            AND WS_INS_MAX != -99.9;
            
            COMMIT;
        """,
        snowflake_conn_id='snowflake_conn_id'
    )

    daily_task >> insert_fact_task