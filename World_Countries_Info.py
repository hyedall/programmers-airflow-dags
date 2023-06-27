from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp


import pandas as pd
import logging
import requests
import json
import pandas as pd


def get_Redshift_connection(autocommit=True):
    '''
    레드시프트 연결 
    '''
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    # postgresql://ID:PW@learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_world_countries_info():
    '''
    API로 나라 정보 가져오기 : 250개
    리스트 형태로 return
    '''
    url = 'https://restcountries.com/v3/all'
    res = requests.get(url)
    json_data = res.json()
    # len(json_data) # 250
    # type(json_data) # list
    # type(json_data[0]) # dict

    trans_data = []

    for data in json_data:
        countries_info = {
            'country': data['name']['official'].replace("'", "''"),
            'population': data['population'],
            'area': data['area']
        }

        trans_data.append(countries_info)

    # pandas DataFrame 생성
    df = pd.DataFrame(trans_data)
    logging.info(df)
    
    return trans_data


def _create_table(cur, schema, table, drop_first):
    '''
    레드시프트에 테이블 만들기 
    drop_first : True - 테이블이 있으면 삭제 후 
    '''
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
    country VARCHAR(100),
    population INTEGER,
    area INTEGER
);""")


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection() # 레드시프트 연결 
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, True) # 있으면 지우고 다시 만들기 : Full Refresh
        # 임시 테이블로 원본 테이블을 복사
        # cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r['country']}', {r['population']}, {r['area']});"
            print(sql)
            cur.execute(sql)

        # # 원본 테이블 생성
        # _create_table(cur, schema, table, True)
        # # 임시 테이블 내용을 원본 테이블로 복사
        # cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")


with DAG(
    dag_id = 'WorldCountriesInfo',
    start_date = datetime(2023,6,24),
    catchup=True,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:

    results = get_world_countries_info()
    load("dahye991228", "world_countries_info", results)