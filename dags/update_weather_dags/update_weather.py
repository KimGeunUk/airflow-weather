import re
import os
import json
import numpy as np
import pandas as pd
import psycopg2
import requests
from datetime import datetime, timedelta
from typing import List, Optional

FILE_PATH = os.path.abspath(__file__)
PROJECT_PATH = os.path.dirname(FILE_PATH)

def get_missing_dates(db_config, cursor, start_date: str, end_date: str) -> List[str]:
    """
    시작일부터 종료일까지 DB에 기록되지 않은 날짜 반환
    """
    
    query = f"""    
        SELECT DISTINCT TO_CHAR(datetime, 'yyyymmdd')
        FROM {db_config['table_name']}
        WHERE datetime >= '{start_date}' AND datetime <= '{end_date}'
    """

    cursor.execute(query)
    dates_in_db = {row[0] for row in cursor}
            
    missing_dates = []
    for date in pd.date_range(start=start_date, end=end_date, freq='D'):
        str_date = date.strftime('%Y%m%d')
        if str_date not in dates_in_db:
            missing_dates.append(str_date)
    
    return missing_dates

def _get_data(url: str, params:dict) -> Optional[pd.DataFrame]:
    """
    API에서 기상 데이터 가져오기
    """
    try:
        response = requests.get(url=url, params=params)
        rows = response.text.split('\n')

        if len(rows) < 5:
            print('Empty or invalid response text.')
            return None

        columns = re.sub(' +', ' ', rows[1]).split(' ')[1:]
        raw_data = [line.split(',')[:-1] for line in rows[3:-2] if line.strip()]

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
        if missing_cols:
            raise ValueError(f'API 열 정보 변경 (누락): {missing_cols}')

        # 필요한 컬럼만 선택하고 이름 변경
        weather = weather[use_cols].rename(columns=rename_dict)
                
        # datetime 형 변환
        weather['datetime'] = (
            pd.to_datetime(weather['datetime'], format='%Y%m%d%H%M')
            .dt.strftime('%Y-%m-%d %H:%M:%S')
        )

        # 수치 컬럼 형 변환 & 이상치 (-50 이하 값) NaN 처리
        numeric_cols = weather.columns.difference(['datetime'])
        weather[numeric_cols] = weather[numeric_cols].astype('Float64')
        weather[numeric_cols] = weather[numeric_cols].where(weather[numeric_cols] > -50, np.nan)
        
        return weather
    except Exception as e:
        print('Empty or invalid response text.')
        return None


def fetch_data(url: str, params: dict, date: str) -> Optional[pd.DataFrame]:
    """
    특정 날짜(YYYYMMDD)에 대해 오전(00~11시59분), 오후(12~23시59분) 기상 데이터를 조회 후 합쳐서 반환
    """
    try:
        # 오전 데이터
        am_params = params.copy()
        am_params['tm1'] = f'{date}0000'
        am_params['tm2'] = f'{date}1159'
        df_am = _get_data(url=url, params=am_params)
        if df_am is None:
            raise ValueError(f"No AM data returned for {date}")

        # 오후 데이터
        pm_params = params.copy()
        pm_params['tm1'] = f'{date}1200'
        pm_params['tm2'] = f'{date}2359'
        df_pm = _get_data(url=url, params=pm_params)
        if df_pm is None:
            raise ValueError(f"No PM data returned for {date}")

        # 오전, 오후 데이터 합치기
        df_day = pd.concat([df_am, df_pm], axis=0, ignore_index=True).dropna()

        # 하루 1440분 중 1000분 이상 데이터 있으면 유효하다고 판단
        if len(df_day) < 1000:
            print(f"{date} 데이터 건수 부족")
            return None
        
        # 데이터 인덱싱 및 정렬
        df_day['datetime'] = pd.to_datetime(df_day['datetime'])
        df_day = (
            df_day
            .set_index('datetime')
            .sort_index()
            .reindex(pd.date_range(
                start=date, 
                end=pd.to_datetime(date) + pd.Timedelta(hours=23, minutes=59), 
                freq='1min'
            ))
            .reset_index()
            .rename(columns={'index': 'datetime'})
        )        
        return df_day
    except Exception as e:
        print(f"Error fetching data for date {date}: {e}")
        return None
    
def insert_data(db_config, connect, cursor, df: pd.DataFrame) -> bool:
    """
    기상 데이터를 DB에 삽입
    """
    def insert_query(connect, cursor, table: str, columns_str: str, rows_list: list):        
        if not rows_list:
            return
        
        values_str = ",".join(rows_list)        
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES {values_str}
            ON CONFLICT (datetime)
            DO UPDATE SET {','.join([f"{col}=EXCLUDED.{col}" for col in columns_str.split(',') if col not in ['datetime']])}
        """
        cursor.execute(query)
        connect.commit()
        return
    
    if df is None or df.empty:
        print("삽입할 데이터가 없습니다")
        return False
    
    try:
        columns_str = ','.join(df.columns.to_list()).lower()
        
        batch_size = 1000 # 1000개씩 insert        
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
                insert_query(connect, cursor, f"{db_config['table_name']}", columns_str, rows_list)
                rows_list.clear()
                
        if rows_list:
            insert_query(connect, cursor, f"{db_config['table_name']}", columns_str, rows_list)

        return True 
    except Exception as e:
        print(f"Error inserting data: {e}")
        return False
    
    
def main():
    # API 설정 로드
    api_config_path = os.path.join(PROJECT_PATH, 'data', 'api_config.json')
    with open(api_config_path, 'r') as file:
        api_config = json.load(file)

    # DB 설정 로드
    db_config_path = os.path.join(PROJECT_PATH, 'data', 'db_config.json')
    with open(db_config_path, 'r') as file:
        db_config = json.load(file)

    # API 설정 구성
    url = api_config['url']  # 방재기상관측(AWS) - 매분자료
    params = {
        'tm1': None,
        'tm2': None,        
        'stn': '133',       # 대전 지점번호
        'disp': '1',        # 표출형태(0=탭, 1=쉼표)
        'help': '0',        # 도움말(0=OFF, 1=ON)
        'authKey': api_config['authKey'],
    }
    
    check_start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    check_end_date= (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    with psycopg2.connect(db_config) as connect:
        with connect.cursor() as cursor:
            # DB에 기록되지 않은 날짜 목록 조회
            missing_dates = get_missing_dates(db_config, cursor, check_start_date, check_end_date)
            
            if not missing_dates:
                print("모든 날짜의 데이터가 이미 존재합니다.")
                return
            
            print("일주일 전부터 현재까지 누락된 날짜들 : ")            
            for date in missing_dates:
                print(f'\t{date}')

            # 누락된 날짜들에 대한 데이터 수집
            collected_dfs = []
            for date in missing_dates:
                print(f"Processing date: {date}")
                daily_df = fetch_data(url, params, date)

                if daily_df is not None and not daily_df.empty:
                    # DB INSERT
                    success = insert_data(connect, cursor, df=daily_df)
                    if success:
                        print(f"{date} 데이터 처리 완료 (rows: {len(daily_df)})")
                        collected_dfs.append(daily_df)
                    else:
                        print(f"{date} 데이터 처리 실패")                        
                else:
                    print(f"{date} 건너뜀, 유효한 데이터 없음")

            weather_df = pd.concat(collected_dfs, axis=0, ignore_index=True)
            print("Processed data : \n")
            print(weather_df)
            

if __name__ == '__main__':
    main()