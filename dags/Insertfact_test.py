from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import snowflake.connector


def insert_new_fact_data(**kwargs):
    today = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    start_date = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    print(f"[INFO] 비교 기간: {start_date} ~ {end_date}")

    # Snowflake 연결 정보 가져오기
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

    # 중복 없는 데이터만 삽입
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
            SELECT 1 
            FROM WEATHER.ANALYTICS_DATA.FACT_TEST fact
            WHERE raw.YMD = fact.YMD AND raw.STN_ID = fact.STN_ID
        )
        """

    with snowflake_conn.cursor() as cur:
        cur.execute(insert_query)

        # DISTINCT 날짜 로그 출력
        log_query = f"""
            SELECT DISTINCT YMD 
            FROM WEATHER.ANALYTICS_DATA.FACT_TEST
            WHERE YMD BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY YMD
            """
        cur.execute(log_query)
        inserted_dates = cur.fetchall()

        print("[INFO] FACT_TEST에 적재된 날짜 목록:")
        for row in inserted_dates:
            print(f" - {row[0]}")

    snowflake_conn.close()


# DAG 정의
with DAG(
    dag_id='daily_fact_update_solo',
    start_date=datetime(2025, 6, 10),
    schedule_interval='0 9 * * *',  # 매일 오전 09:00
    catchup=True,
    default_args={
        'owner': 'ryan',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=["weather", "fact", "snowflake"],
    max_active_runs=1,
) as dag:

    insert_fact_task = PythonOperator(
        task_id='insert_new_fact_data',
        python_callable=insert_new_fact_data,
        provide_context=True
    )
