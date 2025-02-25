import re
import os
import json
import numpy as np
import pandas as pd
import psycopg2
import requests
from datetime import datetime, timedelta

FILE_PATH = os.path.abspath(__file__)
PROJECT_PATH = os.path.dirname(FILE_PATH)

def get_no_dates(db_config, cursor, start_date, end_date):
    """
    start_date 부터 end_date까지 db에 기록되지 않은 날짜 반환
    """
    
    query = f"""    
        SELECT DISTINCT TO_CHAR(datetime, 'yyyymmdd')
        FROM {db_config['table_name']}
        WHERE datetime >= '{start_date}' AND datetime <= '{end_date}'
    """

    cursor.execute(query)
    dates_in_db = {row[0] for row in cursor}
            
    no_dates = []
    for date in pd.date_range(start=start_date, end=end_date, freq='D'):
        str_date = date.strftime('%Y%m%d')
        if str_date not in dates_in_db:
            no_dates.append(str_date)
    
    return no_dates

def get_data(url: str, params:dict) -> pd.DataFrame:
    # API GET    
    response = requests.get(url=url, params=params)
    rows = response.text.split('\n')

    if len(rows) < 5:
        print('Empty or invalid response text.')
        return None

    columns = re.sub(' +', ' ', rows[1]).split(' ')[1:]
    columns_sub = re.sub(' +', ' ', rows[2]).split(' ')[1:]

    raw_data = []
    for line in rows[3:-2]:
        line = line.strip()
        if not line:
            continue
        values = line.split(',')[:-1]
        raw_data.append(values)

    # DataFrame 생성
    weather = pd.DataFrame(raw_data, columns=columns)

    # 필요한 컬럼만 사용 & 이름 매핑
    use_cols = ['YYMMDDHHMI', 'TA', 'RN-DAY', 'WD1', 'WS1', 'PA', 'PS', 'HM']
    rename_dict = {
        'YYMMDDHHMI': 'datetime',
        'TA': 'ta',
        'RN-DAY': 'rn',
        'WD1': 'wd',
        'WS1': 'ws',
        'PA': 'pa',
        'PS': 'ps',
        'HM': 'hm'
    }

    # 열 확인
    missing_cols = [c for c in use_cols if c not in weather.columns]
    assert not missing_cols, f'API 열 정보 변경 (누락): {missing_cols}'

    weather = weather[use_cols].rename(columns=rename_dict)
    
    # datetime 형 변환
    weather['datetime'] = (
        pd.to_datetime(weather['datetime'], format='%Y%m%d%H%M')
        .dt.strftime('%Y-%m-%d %H:%M:%S')
    )

    # 수치 컬럼 형 변환
    numeric_cols = weather.columns.difference(['datetime'])
    weather[numeric_cols] = weather[numeric_cols].astype('Float64')

    # 이상치 (-50 이하 값) NaN 처리
    weather[numeric_cols] = weather[numeric_cols].where(weather[numeric_cols] > -50, np.nan)

    return weather

def fetch_weather_data(url: str, params: dict, date: str) -> pd.DataFrame:
    """
    특정 날짜(YYYYMMDD)에 대해 오전(00~11시59분), 오후(12~23시59분) 데이터를 조회 후 합쳐서 반환
    """
    try:
        # 오전 데이터
        am_params = params.copy()
        am_params['tm1'] = f'{date}0000'
        am_params['tm2'] = f'{date}1159'
        df_am = get_data(url=url, params=am_params)
        if df_am is None:
            raise ValueError("No AM data returned")

        # 오후 데이터
        pm_params = params.copy()
        pm_params['tm1'] = f'{date}1200'
        pm_params['tm2'] = f'{date}2359'
        df_pm = get_data(url=url, params=pm_params)
        if df_pm is None:
            raise ValueError("No PM data returned")

        # 오전, 오후 데이터 합치기
        df_day = pd.concat([df_am, df_pm], axis=0, ignore_index=True).dropna()

        # 하루 1440분 중 1000분 이상 데이터 있으면 유효하다고 판단
        if len(df_day) < 1000:
            print(f"[WARN] {date} 데이터 건수 부족")
            return None
        
        return df_day

    except AssertionError as e:
        print(f"Assertion error for date {date}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching data for date {date}: {e}")
        return None
    
def insert_data(db_config, connect, cursor, df: pd.DataFrame):
    def insert_query(connect, cursor, table: str, columns_str: str, values_str: str):
        if not rows_list:
            return
        
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES {values_str}
        """

        cursor.execute(query)
        connect.commit()
    
    columns_str = str.lower(','.join(df.columns.to_list()))
    
    batch_size = 1000 # 100개씩 insert
    
    rows_list = []
    for i, row in enumerate(df.itertuples(index=False)):
        values_list = []
        for v in row:
            if v is None or str(v).lower() in ('nat', 'nan') or str(v).upper() == 'NULL':
                values_list.append("NULL")
            elif isinstance(v, (int, float)):
                values_list.append(str(v))
            else:
                values_list.append(f"'{v}'")
        row_str = f"({','.join(values_list)})"
        rows_list.append(row_str)

        if (i + 1) % batch_size == 0:
            values_str = ",".join(rows_list)
            insert_query(connect, cursor, f"{db_config['table_name']}", columns_str, values_str)
            rows_list.clear()
            
    if rows_list:
        values_str = ",".join(rows_list)
        insert_query(connect, cursor, f"{db_config['table_name']}", columns_str, values_str)

    
def main():
    api_config_path = os.path.join(PROJECT_PATH, 'data', 'api_config.json')
    with open(api_config_path, 'r') as file:
        api_config = json.load(file)
    authKey = api_config['authKey'] 

    db_config_path = os.path.join(PROJECT_PATH, 'data', 'db_config.json')
    with open(db_config_path, 'r') as file:
        db_config = json.load(file)

    url = api_config['url']  # 방재기상관측(AWS) - 매분자료
    params = {
        'tm1': None,
        'tm2': None,        
        'stn': '133',       # 대전 지점번호
        'disp': '1',        # 표출형태(0=탭, 1=쉼표)
        'help': '0',        # 도움말(0=OFF, 1=ON)
        'authKey': authKey,
    }
    
    check_start_date = '2025-02-01 00:00:00'
    check_end_date_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    check_end_date = check_end_date_dt.strftime("%Y-%m-%d %H:%M:%S")

    with psycopg2.connect(
        host=db_config['host'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    ) as connect:
        with connect.cursor() as cursor:
            # DB에 기록되지 않은 날짜 목록 조회
            no_dates = get_no_dates(db_config, cursor, check_start_date, check_end_date)
            print("2025년 2월 1일 부터 현재까지 누락된 시간들 : ")            
            for date in no_dates:
                print(f'\t{date}')

            # 누락된 날짜들에 대한 데이터 수집
            collected_dfs = []
            for date in no_dates:
                print(f"[INFO] {date} data processing ...")
                daily_df = fetch_weather_data(url, params, date)

                if daily_df is not None and not daily_df.empty:
                    if not daily_df.empty:
                        daily_df['datetime'] = pd.to_datetime(daily_df['datetime'])
                        daily_df = (
                            daily_df
                            .set_index('datetime')
                            .sort_index()
                            .reindex(pd.date_range(start=date, end=pd.to_datetime(date)+pd.Timedelta(hours=23, minutes=59), freq='1min'))
                            .reset_index().rename(columns={'index': 'datetime'})
                        )

                    # DB INSERT
                    insert_data(connect, cursor, df=daily_df)
                        
                    collected_dfs.append(daily_df)
                    print(f"[INFO] {date} data processed, complete !! (rows: {len(daily_df)})")
                else:
                    print(f"[INFO] {date} skipped, no valid data retrieved.")

            weather_df = pd.concat(collected_dfs, axis=0, ignore_index=True)
            print("Processed data : \n")
            print(weather_df)
            

if __name__ == '__main__':
    main()