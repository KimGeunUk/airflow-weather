import os
import json
import numpy as np
import pandas as pd
import psycopg2
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from airflow.exceptions import AirflowException


FILE_PATH = os.path.abspath(__file__)
PROJECT_PATH = os.path.dirname(FILE_PATH)
BATCH_SIZE = 20  # DB 삽입 배치 크기


def get_missing_times(
    conn,
    cursor,
    db_config: Dict[str, Any],
    start_date: str,
    end_date: str
) -> List[datetime]:
    """
    시작일부터 종료일까지 DB에 기록되지 않은 시간 목록을 반환
    """
    query = f"""
        SELECT DISTINCT TO_CHAR(tmfc, 'yyyy-mm-dd HH24:MI:SS')
        FROM {db_config['table_name']}
        WHERE tmfc >= '{start_date}' AND tmfc <= '{end_date}'
    """
    
    cursor.execute(query)
    times_in_db: Set[str] = {row[0] for row in cursor}
    
    missing_times = set()
    for time in pd.date_range(start=start_date, end=end_date, freq='1h'):
        # DB에 없는 시간
        if time.strftime("%Y-%m-%d %H:%M:%S") not in times_in_db:
            # 예보 기준 3시간 간격 보정
            if int(time.hour) % 3 == 2:
                missing_times.add(time)
            elif int(time.hour) % 3 == 1:
                missing_times.add(time - timedelta(hours=2))
            elif int(time.hour) % 3 == 0:
                missing_times.add(time - timedelta(hours=1))
    
    return list(missing_times)

def fetch_forecast_data(
    timestamp: datetime,
    api_url: str,
    api_params: Dict[str, Any],
    tag_dict: Dict[str, str]
) -> Optional[pd.DataFrame]:
    """
    특정 시간의 기상 예보 데이터 수집
    """
    try:
        date_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        yyyymmdd = timestamp.strftime("%Y%m%d")
        hhmm = timestamp.strftime("%H%M")
        print(f"데이터 처리 중 ({date_str}) ...")

        # API 파라미터 설정
        params = api_params.copy()
        params['base_date'] = yyyymmdd
        params['base_time'] = hhmm

        # API 요청
        response = requests.get(url=api_url, params=params)
        js = response.json()
        
        # 응답 파싱
        try:
            js_body = js['response']['body']
            js_item = js_body['items']['item']
        except KeyError:
            js_except = js['response']['header']['resultMsg']
            print(f"{date_str} 데이터 조회 오류: {js_except}")
            return None

        # 초기 DataFrame 생성
        df = pd.DataFrame(js_item)
        
        # 필요한 태그만 남기고 이름 변경
        df = df[df['category'].isin(tag_dict.keys())].copy()
        df['category'] = df['category'].map(tag_dict)

        # 수치 변환 및 이상치 처리
        df['fcstValue'] = df['fcstValue'].astype(float)
        df = df.where((df['fcstValue'] < 900) & (df['fcstValue'] > -900), np.nan)  # ±900 범위 밖은 결측 처리

        # 데이터 처리 1단계
        df1 = process_forecast_data(df)

        # 데이터 처리 2단계: 3시간 예보를 1시간 간격으로 보간
        df2 = convert_3h_to_1h_forecast(df1, date_str)
        print(f"데이터 처리 완료 ({date_str}):\n{df2.head()}")

        return df2

    except Exception as e:
        print(f"예보 데이터 수집 중 오류 발생: {e}")
        raise AirflowException(f"예보 데이터 수집 중 오류 발생: {e}")

def process_forecast_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    기상 예보 데이터 처리 1단계
    - 행: 발표시각(tmfc), 열: 태그(info) 및 예보시간(fcst_?h), 값: 예보 값
    """
    # tag_dict에서 사용된 category 값들을 한 번에 처리
    df_list = []
    for info_tag in df['category'].unique():
        sub_df = df[df['category'] == info_tag].copy()

        # tmfc(발표시각), tmef(예보시각) 생성
        sub_df['tmfc'] = pd.to_datetime(sub_df['baseDate'] + sub_df['baseTime'])
        sub_df['tmef'] = pd.to_datetime(sub_df['fcstDate'] + sub_df['fcstTime'])
        sub_df['value'] = sub_df['fcstValue']

        # 발표시각으로부터 몇 시간 후 예보인지
        sub_df['forecast'] = (sub_df['tmef'] - sub_df['tmfc'])
        sub_df['forecast'] = sub_df['forecast'].apply(lambda x: int(x.total_seconds() // 3600))

        # 6~58시간 범위만
        sub_df = sub_df[sub_df['forecast'].between(6, 58)]
        sub_df = sub_df[['tmfc', 'forecast', 'value']]

        # 피벗 테이블
        sub_df = sub_df.pivot_table(index='tmfc', columns='forecast', values='value')
        sub_df = sub_df.add_prefix('fcst_').add_suffix('h')

        # 태그 정보 열 삽입
        sub_df.insert(0, 'info', info_tag)
        df_list.append(sub_df)

    # 모든 태그 데이터 합치기
    return pd.concat(df_list, axis=0)

def convert_3h_to_1h_forecast(df: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """
    기상 예보 데이터 처리 2단계
    - 3시간 간격 예보를 1시간 간격 예보로 보간
    """
    # 시간 범위 설정
    start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_dt = start_dt + timedelta(hours=2, minutes=59, seconds=59)
    
    date_range_1hour = pd.date_range(start=start_dt, end=end_dt, freq='1H')
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    
    df_list = []
    for _, group_df in df.groupby('info'):
        # 1시간 간격 인덱스 재구성
        group_df = group_df.reindex(date_range_1hour)

        # 2회 보간 시도
        for _ in range(2):
            mask = group_df['info'].isnull()
            
            # info 컬럼 먼저 채워넣기
            group_df['info'] = group_df['info'].ffill(limit=1)
            
            # 예보 컬럼 보간 (fcst_xh -> fcst_(x-1)h)
            for hour in range(6, 59):
                from_col = f'fcst_{hour+1}h'
                to_col = f'fcst_{hour}h'
                
                if from_col in group_df.columns and to_col in group_df.columns:
                    group_df[to_col] = np.where(
                        mask,
                        group_df[from_col].shift(1),
                        group_df[to_col]
                    )

        df_list.append(group_df)
    
    # 모든 태그 보간 결과 합치기
    df_ = pd.concat(df_list, axis=0)

    # 55~58시간 예보는 불필요하다고 가정하고 제거
    drop_cols = [f'fcst_{h}h' for h in range(55, 59)]
    df_ = df_.drop(columns=[c for c in drop_cols if c in df_.columns], axis=1, errors='ignore')

    # 인덱스 초기화
    df_ = df_.reset_index().rename(columns={'index': 'tmfc'}).dropna()
    return df_

def insert_data(
    conn,
    cursor,
    df: pd.DataFrame,
    db_config: Dict[str, Any]
) -> bool:
    """
    예보 데이터를 DB에 삽입
    """
    if df is None or df.empty:
        print("삽입할 데이터가 없습니다")
        return False

    try:
        columns_str = ",".join(col.lower() for col in df.columns)
        rows_list = []
        
        for i, row in enumerate(df.itertuples(index=False)):
            values_list = []
            for val in row:
                if val is None or str(val).lower() in ('nat', 'nan') or str(val).upper() == 'NULL':
                    values_list.append("NULL")
                elif isinstance(val, (int, float)):
                    values_list.append(str(val))
                else:
                    values_list.append(f"'{val}'")
            
            row_str = f"({','.join(values_list)})"
            rows_list.append(row_str)

            # 배치 단위로 삽입
            if (i + 1) % BATCH_SIZE == 0:
                _execute_insert(conn, cursor, rows_list, columns_str, db_config)
                rows_list.clear()

        # 남은 행이 있다면 처리
        if rows_list:
            _execute_insert(conn, cursor, rows_list, columns_str, db_config)

        return True

    except Exception as e:
        print(f"데이터 삽입 중 오류 발생: {e}")
        return False

def _execute_insert(
    conn,
    cursor,
    rows_list: List[str],
    columns_str: str,
    db_config: Dict[str, Any]
) -> None:
    """
    실제 INSERT 쿼리 실행
    """
    if not rows_list:
        return

    values_str = ",".join(rows_list)
    query = f"""
        INSERT INTO {db_config['table_name']} ({columns_str})
        VALUES {values_str}
        ON CONFLICT (tmfc,info)
        DO UPDATE SET {','.join([f"{col}=EXCLUDED.{col}" for col in columns_str.split(',') if col not in ['tmfc', 'info']])}
    """

    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f"INSERT ERROR: {e}")
        conn.rollback()

def load_config() -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, str]]:
    """
    설정 파일 로드
    """
    # API 인증키 로드
    authKey_path = os.path.join(PROJECT_PATH, 'data', 'api_authKey.json')
    with open(authKey_path, 'r') as file:
        authKey_file = json.load(file)

    # DB 설정 로드
    db_config_path = os.path.join(PROJECT_PATH, 'data', 'db_config.json')
    with open(db_config_path, 'r') as fp:
        db_config = json.load(fp)

    # API 설정
    api_config = {
        'url': 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst',  # 단기예보 조회서비스
        'params': {
            'serviceKey': authKey_file['authKey'],  # 인증키
            'numOfRows': '10000',                   # 한 페이지 결과 수
            'pageNo': '1',                          # 페이지 번호
            'dataType': 'JSON',                     # 요청자료형식(XML/JSON)
            'base_date': None,                      # 발표일자
            'base_time': None,                      # 발표시각(3시간 단위)
            'nx': '68',                             # 예보지점 X 좌표
            'ny': '101',                            # 예보지점 Y 좌표
        }
    }

    # 태그 매핑 사전
    tag_dict = {
        'TMP': 'ta',        # 기온
        'POP': 'rn_st',     # 강수확률
        'PTY': 'rn_yn',     # 강수형태 : 없음(0), 비(1), 비/눈(2), 눈(3), 소나기(4)
        'SKY': 'wf_cd',     # 하늘 상태 : 맑음(1), 구름많음(3), 흐림(4)
        'REH': 'hm',        # 습도
        'WSD': 'ws',        # 풍속
    }

    return db_config, api_config, tag_dict

def run(**kwargs):
    """
    메인 함수
    """
    # 설정 로드
    db_config, api_config, tag_dict = load_config()

    # 실행 시간 설정
    if kwargs:
        run_time_utc = kwargs['data_interval_end']
        run_time_ktc = run_time_utc + timedelta(hours=9)
        run_time = run_time_ktc.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    else:
        run_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    print(f"시작 시각 : {run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 체크 기간 설정 (어제부터 현재까지)
    check_start_date = (run_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    check_end_date = run_time.strftime("%Y-%m-%d %H:%M:%S")

    # DB 연결 후 작업 수행
    with psycopg2.connect(db_config) as connect:
        with connect.cursor() as cursor:
            # 1. 누락된 시간 조회
            missing_times = get_missing_times(connect, cursor, db_config, check_start_date, check_end_date)
            if not missing_times:
                print("모든 시간의 데이터가 이미 존재합니다.")
                return

            print("어제부터 현재까지 누락된 시간들:")
            for mt in missing_times:
                print(f"\t{mt.strftime('%Y-%m-%d %H:%M:%S')}")

            # 2. 누락 시간에 대한 예보 데이터 조회 → DB 삽입
            for mt in missing_times:
                time_str = mt.strftime("%Y-%m-%d %H:%M:%S")
                forecast_df = fetch_forecast_data(mt, api_config['url'], api_config['params'], tag_dict)

                if forecast_df is not None and not forecast_df.empty:
                    success = insert_data(connect, cursor, forecast_df, db_config)
                    if success:
                        print(f"{time_str} 데이터 처리 및 DB 삽입 완료")
                    else:
                        print(f"{time_str} 데이터 DB 삽입 실패")
                else:
                    print(f"{time_str} 건너뜀, 유효한 데이터 없음")

if __name__ == "__main__":
    run()
