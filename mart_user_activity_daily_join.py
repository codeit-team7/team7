"""
==============================================================================
[Python / Pandas] mart_user_activity_daily 구축 및 조인 스크립트
(반영 사항: Hackle 이벤트 유지 + accounts_friendrequest 순수 발송(send_user_id)만 집계)
==============================================================================
"""

import os
import re
import json
import pandas as pd
import numpy as np

DATA_DIR = r'c:\Users\user\Desktop\고급프로젝트\csv_export'
OUT_DIR  = r'c:\Users\user\Desktop\고급프로젝트'

def load_csv(name, usecols=None):
    return pd.read_csv(os.path.join(DATA_DIR, name), usecols=usecols, encoding='utf-8-sig', low_memory=False)

def build_mart_user_activity_daily():
    print("=== mart_user_activity_daily (Pandas Join) 시작 ===")

    # 1. accounts_user (가입일)
    print("[1/7] accounts_user 로드...")
    user = load_csv('accounts_user.csv', usecols=['id', 'created_at'])
    user['user_id'] = pd.to_numeric(user['id'], errors='coerce').astype('Int64')
    user['user_signup_date'] = pd.to_datetime(user['created_at']).dt.date
    user_signup_map = user.dropna(subset=['user_id']).set_index('user_id')['user_signup_date']

    # 2. accounts_attendance (출석체크 언패킹)
    print("[2/7] accounts_attendance 파싱...")
    att = load_csv('accounts_attendance.csv')
    att_rows = []
    for _, r in att.iterrows():
        uid = r['user_id']
        dates_raw = r['attendance_date_list']
        if pd.notna(dates_raw):
            try:
                for d in json.loads(dates_raw):
                    att_rows.append({'user_id': uid, 'activity_date': d[:10]})
            except:
                pass
    df_att = pd.DataFrame(att_rows).drop_duplicates()
    df_att['attendance_flag'] = True

    # 3. hackle 세션 매핑 & 이벤트 집계
    print("[3/7] hackle_events 집계...")
    hp = load_csv('hackle_properties.csv', usecols=['session_id', 'user_id'])
    hp['is_num'] = hp['user_id'].apply(lambda x: bool(re.match(r'^\d+$', str(x).strip())) if pd.notna(x) else False)
    hp_num = hp[hp['is_num']].copy()
    hp_num['user_id'] = hp_num['user_id'].astype(int)
    valid_sess = hp_num.groupby('session_id')['user_id'].nunique()
    session_map = hp_num[hp_num['session_id'].isin(valid_sess[valid_sess == 1].index)].drop_duplicates('session_id').set_index('session_id')['user_id'].to_dict()

    target_events = {'$session_start', 'click_question_start', 'complete_question', 'skip_question', 'click_question_open'}
    he_chunks = []
    for chunk in pd.read_csv(os.path.join(DATA_DIR, 'hackle_events.csv'), chunksize=500000, low_memory=False):
        sub = chunk[chunk['event_key'].isin(target_events)].copy()
        sub['user_id'] = sub['session_id'].map(session_map)
        sub = sub.dropna(subset=['user_id'])
        sub['user_id'] = sub['user_id'].astype(int)
        sub['activity_date'] = sub['event_datetime'].str[:10]
        he_chunks.append(sub)

    he_all = pd.concat(he_chunks, ignore_index=True) if he_chunks else pd.DataFrame()
    df_hackle = he_all.groupby(['user_id', 'activity_date']).agg(
        launch_app_count=('event_key', lambda x: (x == '$session_start').sum()),
        session_count=('session_id', 'nunique'),
        question_start_count=('event_key', lambda x: (x == 'click_question_start').sum()),
        question_complete_count=('event_key', lambda x: (x == 'complete_question').sum()),
        skip_count=('event_key', lambda x: (x == 'skip_question').sum()),
        ping_open_count=('event_key', lambda x: (x == 'click_question_open').sum())
    ).reset_index()

    # 4. polls_questionset 집계
    print("[4/7] polls_questionset 집계...")
    qs = load_csv('polls_questionset.csv', usecols=['user_id', 'created_at', 'status'])
    qs['activity_date'] = qs['created_at'].str[:10]
    qs['user_id'] = pd.to_numeric(qs['user_id'], errors='coerce').astype('Int64')
    df_qs = qs.groupby(['user_id', 'activity_date']).agg(
        qs_start_count=('status', 'count'),
        qs_complete_count=('status', lambda x: (x == 'F').sum())
    ).reset_index()

    # 5. accounts_friendrequest (※ 핵심: send_user_id만 집계!)
    print("[5/7] accounts_friendrequest (발송 기준만) 집계...")
    fr = load_csv('accounts_friendrequest.csv', usecols=['send_user_id', 'created_at'])
    fr['user_id'] = pd.to_numeric(fr['send_user_id'], errors='coerce').astype('Int64')
    fr['activity_date'] = fr['created_at'].str[:10]
    df_friend = fr.dropna(subset=['user_id']).groupby(['user_id', 'activity_date']).size().reset_index(name='friend_action_count')

    # 6. accounts_pointhistory & paymenthistory 집계
    print("[6/7] 포인트 및 결제 집계...")
    ph = load_csv('accounts_pointhistory.csv', usecols=['user_id', 'delta_point', 'created_at'])
    ph['user_id'] = pd.to_numeric(ph['user_id'], errors='coerce').astype('Int64')
    ph['activity_date'] = ph['created_at'].str[:10]
    ph['delta_point'] = pd.to_numeric(ph['delta_point'], errors='coerce').fillna(0)
    df_point = ph.groupby(['user_id', 'activity_date']).agg(
        point_earn=('delta_point', lambda x: x[x > 0].sum()),
        point_spend=('delta_point', lambda x: x[x < 0].abs().sum())
    ).reset_index()

    pay = load_csv('accounts_paymenthistory.csv', usecols=['user_id', 'created_at'])
    pay['user_id'] = pd.to_numeric(pay['user_id'], errors='coerce').astype('Int64')
    pay['activity_date'] = pay['created_at'].str[:10]
    df_pay = pay.dropna(subset=['user_id']).groupby(['user_id', 'activity_date']).size().reset_index(name='payment_count')

    # 7. 전체 테이블 FULL OUTER MERGE
    print("[7/7] 마트 병합 및 시계열 윈도우 함수 계산...")
    dfs = [df_att, df_hackle, df_qs, df_friend, df_point, df_pay]
    mart = dfs[0]
    for d in dfs[1:]:
        mart = pd.merge(mart, d, on=['user_id', 'activity_date'], how='outer')

    mart['attendance_flag'] = mart['attendance_flag'].fillna(False)
    num_cols = ['launch_app_count', 'session_count', 'question_start_count', 'question_complete_count',
                'skip_count', 'ping_open_count', 'friend_action_count', 'point_earn', 'point_spend', 'payment_count']
    for col in num_cols:
        mart[col] = mart[col].fillna(0)

    if 'qs_start_count' in mart.columns:
        mart['question_start_count'] = np.maximum(mart['question_start_count'], mart['qs_start_count'].fillna(0))
        mart['question_complete_count'] = np.maximum(mart['question_complete_count'], mart['qs_complete_count'].fillna(0))
        mart = mart.drop(columns=['qs_start_count', 'qs_complete_count'])

    # 시계열 윈도우 피처 계산
    mart['activity_dt'] = pd.to_datetime(mart['activity_date'])
    mart['signup_dt'] = pd.to_datetime(mart['user_id'].map(user_signup_map))
    mart['days_since_first_active'] = (mart['activity_dt'] - mart['signup_dt']).dt.days

    mart = mart.sort_values(['user_id', 'activity_date']).reset_index(drop=True)
    mart['prev_date'] = mart.groupby('user_id')['activity_dt'].shift(1)
    mart['next_date'] = mart.groupby('user_id')['activity_dt'].shift(-1)

    mart['days_since_previous_active'] = (mart['activity_dt'] - mart['prev_date']).dt.days
    mart['next_active_date'] = mart['next_date'].dt.strftime('%Y-%m-%d').fillna('')
    mart['gap_to_next_activity'] = (mart['next_date'] - mart['activity_dt']).dt.days

    mart['is_active'] = (
        (mart['attendance_flag']) |
        (mart['session_count'] > 0) |
        (mart['question_start_count'] > 0) |
        (mart['skip_count'] > 0) |
        (mart['ping_open_count'] > 0) |
        (mart['friend_action_count'] > 0) |
        (mart['point_spend'] > 0) |
        (mart['payment_count'] > 0)
    )

    final_cols = [
        'user_id', 'activity_date', 'attendance_flag', 'launch_app_count', 'session_count',
        'question_start_count', 'question_complete_count', 'skip_count', 'ping_open_count',
        'friend_action_count', 'point_earn', 'point_spend', 'payment_count',
        'days_since_first_active', 'days_since_previous_active',
        'next_active_date', 'gap_to_next_activity', 'is_active'
    ]
    res = mart[final_cols]
    print(f"✅ 완료: {len(res):,} 행")
    return res

if __name__ == '__main__':
    df = build_mart_user_activity_daily()
