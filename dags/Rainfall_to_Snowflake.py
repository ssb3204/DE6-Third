from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import pandas as pd
import requests
import io



def extract_data(execution_date):

    date_str = execution_date.strftime('%Y%m%d')

    url = "https://apihub.kma.go.kr/api/typ01/url/sts_rn.php"
    params = {
        "tm1": date_str,
        "tm2": date_str,
        "stn_id": "0",
        "disp": "0",
        "authKey": Variable.get("KMA_API_KEY")
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text), comment='#', encoding='utf-8-sig', header=None)
    df.columns = [
        "YMD", "STN_ID", "LAT", "LON", "ALTD",
        "RN_DSUM", "RN_MAX_1HR", "RN_MAX_1HR_OCUR_TMA",
        "RN_MAX_6HR", "RN_MAX_6HR_OCUR_TMA",
        "RN_MAX_10M", "RN_MAX_10M_OCUR_TMA",
        "EXTRA_COL"
    ]
    df.drop(columns=["EXTRA_COL"], inplace=True)
    df["YMD"] = pd.to_datetime(df["YMD"], format="%Y%m%d").dt.strftime("%Y-%m-%d")

    return df


def transform_data(df):

    fact_df = df[["YMD", "STN_ID", "RN_DSUM"]].copy()
    fact_df["RN_DSUM"] = fact_df["RN_DSUM"].apply(lambda x: 0 if x == -99.9 else x)
    return fact_df


def load_to_snowflake(df, fact_df):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    

    try:
        ymd = df["YMD"].iloc[0]
        cursor.execute(f"DELETE FROM RAW_DAILY_RAINFALL WHERE YMD = '{ymd}'")
        conn.commit()
        success, _, _, _ = write_pandas(conn, df, 'RAW_DAILY_RAINFALL')
        if not success:
            raise Exception("write_pandas to RAW_DAILY_RAINFALL failed")


        cursor.execute(f"DELETE FROM ANALYTICS_DATA.FACT_DAILY_RAINFALL WHERE YMD = '{ymd}'")
        conn.commit()
        success_fact, _, _, _ = write_pandas(
            conn,
            fact_df,
            'FACT_DAILY_RAINFALL',
            database='WEATHER',
            schema='ANALYTICS_DATA'
        )
        if not success_fact:
            raise Exception("write_pandas to FACT_DAILY_RAINFALL failed")

    finally:
        cursor.close()
        conn.close()




def etl_pipeline(execution_date, **context):
    df = extract_data(execution_date)
    fact_df = transform_data(df)
    load_to_snowflake(df, fact_df)




default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Rainfall_to_Snowflake',
    default_args=default_args,
    start_date=datetime(2025, 6, 5),
    schedule_interval='0 0 * * *',  
    catchup=True,
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=etl_pipeline,
        provide_context=True
    )
