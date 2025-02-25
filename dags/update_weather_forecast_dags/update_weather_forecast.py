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
        SELECT DISTINCT TO_CHAR(tmfc, 'yyyy-mm-dd HH24:MI:SS')
        FROM {db_config['table_name']}
        WHERE tmfc >= '{start_date}' AND tmfc <= '{end_date}'
    """

    cursor.execute(query)
    dates_in_db = {row[0] for row in cursor}

    no_dates = []
    for date in pd.date_range(start=start_date, end=end_date, freq='1h'):
        if date.strftime("%Y-%m-%d %H:%M:%S") not in dates_in_db:
            if int(date.hour) % 3 == 2: # 3시간 간격
                no_dates.append(date)
    
    return no_dates

def process_1(df, tag_dict):
    df_list = []
    for v in tag_dict.values():
        sub_df = df[df['category']==v].copy()
        sub_df['tmfc'] = pd.to_datetime(sub_df['baseDate'] + sub_df['baseTime'])
        sub_df['value'] = sub_df['fcstValue']
        sub_df['forecast'] = 24 * (sub_df['fcstDate'].astype(int) - sub_df['baseDate'].astype(int)) + (sub_df['fcstTime'].astype(int) - sub_df['baseTime'].astype(int)) // 100
        sub_df = sub_df[sub_df['forecast'].between(6, 58)]
        sub_df = sub_df[['tmfc', 'forecast', 'value']]

        sub_df = sub_df.pivot_table(index='tmfc', columns='forecast', values='value')
        sub_df = sub_df.add_prefix('fcst_').add_suffix('h')

        sub_df.insert(0, 'info', v)

        df_list.append(sub_df)
    df_ = pd.concat(df_list, axis=0)
    return df_


def process_2(df, start_date):
    start_date_dt = (datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d %H:%M:%S")
    end_date_dt = (datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") + timedelta(hours=2, minutes=59, seconds=59)).strftime("%Y-%m-%d %H:%M:%S")

    df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
    date_range_1hour = pd.date_range(start=start_date_dt, end=end_date_dt, freq='1h')

    df_list = []
    for _, group_df in df.groupby('info'):
        group_df = group_df.reindex(date_range_1hour)

        for _ in range(2):
            mask = group_df['info'].isnull()

            group_df['info'] = group_df['info'].ffill(limit=1)

            for hour in range(6, 59):
                from_col = f'fcst_{hour+1}h'
                to_col   = f'fcst_{hour}h'

                if from_col in group_df.columns and to_col in group_df.columns:
                    group_df[to_col] = np.where(mask, group_df[from_col].shift(1), group_df[to_col])

        df_list.append(group_df)

    df_ = pd.concat(df_list, axis=0)
    
    df_ = df_.drop(columns=['fcst_55h', 'fcst_56h', 'fcst_57h', 'fcst_58h'], axis=1)
    df_ = df_.reset_index().rename(columns={'index': 'tmfc'}).dropna()
    
    return df_


def process_3(df, connect, cursor, db_config):
    def insert_db(connect, cursor, db_config, columns, rows_list):
        if not rows_list:
            return
        
        values_str = ",".join(rows_list)
        
        query = f"""
        INSERT INTO {db_config['table_name']} ({columns})
        VALUES {values_str}
        """

        cursor.execute(query)
        connect.commit()   

    columns_str =  ",".join(col.lower() for col in df.columns)
    batch_size = 20

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
            insert_db(connect, cursor, db_config, columns_str, rows_list)
            rows_list.clear()
            
    if rows_list:
        insert_db(connect, cursor, db_config, columns_str, rows_list)
        
        

def main(**kwargs):
    # API Config Load
    api_config_path = os.path.join(PROJECT_PATH, 'data', 'api_config.json')
    with open(api_config_path, 'r') as file:
        api_config = json.load(file)
    authKey = api_config['authKey'] 
    
    # DB Config Load
    db_config_path = os.path.join(PROJECT_PATH, 'data', 'db_config.json')
    with open(db_config_path, 'r') as file:
        db_config = json.load(file)

    # API Params Set
    url = api_config['url']         # 단기예보 조회서비스
    params = {
        'serviceKey': authKey,      # 인증키
        'numOfRows': '10000',       # 한 페이지 결과 수
        'pageNo': '1',              # 페이지 번호
        'dataType': 'JSON',         # 요청자료형식(XML/JSON)
        'base_date': None,          # 발표일자
        'base_time': None,          # 발표시각(3시간 단위, 매 시각 10분 이후 호출)
        'nx': '68',                 # 예보지점 X 좌표(대전 대덕구 회덕동)
        'ny': '101',                # 예보지점 Y 좌표(대전 대덕구 회덕동)
    }
    
    # Using Tag Set
    tag_dict = {
        'TMP': 'ta',        # 기온
        'POP': 'rn_st',     # 강수확률
        'PTY': 'rn_yn',     # 강수형태 : 없음(0), 비(1), 비/눈(2), 눈(3), 소나기(4) 
        'SKY': 'wf_cd',     # 하늘 상태 : 맑음(1), 구름많음(3), 흐림(4)
        'REH': 'hm',        # 습도
        'WSD': 'ws',        # 풍속
    }
    
    # kwargs : airflow dag info
    if kwargs:
        run_time_utc = kwargs['execution_date']
        run_time_ktc = run_time_utc + timedelta(hours=9)
        run_time = run_time_ktc.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    else:
        run_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    
    check_start_date = (run_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    check_end_date = run_time.strftime("%Y-%m-%d %H:%M:%S")
    
    with psycopg2.connect(
        host=db_config['host'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    ) as connect:
        with connect.cursor() as cursor:
            no_dates = get_no_dates(db_config, cursor, check_start_date, check_end_date)
            
            print("어제부터 현재까지 누락된 시간들 : ")            
            for date in no_dates:
                print(f'\t{date.strftime("%Y-%m-%d %H:%M:%S")}')
        
            for date in no_dates:
                date_str = date.strftime("%Y-%m-%d %H:%M:%S")
                yyyymmdd = date.strftime("%Y%m%d")
                hhmm = date.strftime("%H%M")
                print(f"[INFO] Processing data ({date_str}) ...")
                
                params_ = params.copy()
                params_['base_date'] = yyyymmdd
                params_['base_time'] = hhmm
                
                response = requests.get(url=url, params=params_)                
                js = response.json()
                
                try:
                    js_body = js['response']['body']    
                    js_item = js_body['items']['item']
                except:
                    js_except = js['response']['header']['resultMsg']
                    print(f"[ERROR] {date_str} get error : {js_except}")
                    
                df = pd.DataFrame(js_item)

                # prcoess_0
                #   - tag 명 변경, 형 변환, 이상치 처리
                df = df[df['category'].isin(tag_dict.keys())].copy()
                df['category'] = df['category'].map(tag_dict)
                
                df['fcstValue'] = df['fcstValue'].astype(float)
                df = df.where((df['fcstValue'] < 900) & (df['fcstValue'] > -900), np.nan) # 900이상 -900 이하 값은 Missing 값

                # process_1
                #   - 행 : 발표시각, 열 : 태그 및 n시간 후 예보, 값 : 예보 값
                df1 = process_1(df, tag_dict)    

                # process_2
                #   - 3시간 간격 예보 -> 1시간 간격 예보
                df2 = process_2(df1, date.strftime("%Y-%m-%d %H:%M:%S"))                
                print(f"[INFO] Processed data ({date_str}):\n{df2.head()}")
                
                # process_3
                #   - insert_db
                process_3(df2, connect, cursor)
                print(f"[INFO] Processed insert data to database ({date_str})")
                
if __name__ == '__main__':
    main()