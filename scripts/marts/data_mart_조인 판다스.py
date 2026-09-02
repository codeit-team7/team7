"""
==============================================================================
[PANDAS VERSION] 익명 SNS 데이터 마트 구축 및 테이블 조인 코드
==============================================================================
각 데이터 마트별 테이블 로드, 병합(Merge/Join/Concat), 파생변수 생성 및 정제 전 과정.
"""

import os
import re
import pandas as pd
import numpy as np

# 데이터 경로 설정
DATA_DIR = r'c:\Users\user\Desktop\고급프로젝트\csv_export'
OUT_DIR  = r'c:\Users\user\Desktop\고급프로젝트'

def load_csv(filename, usecols=None, dtype=None):
    """CSV 로드 헬퍼 함수"""
    path = os.path.join(DATA_DIR, filename)
    return pd.read_csv(path, usecols=usecols, dtype=dtype, encoding='utf-8-sig', low_memory=False)


# ==============================================================================
# 1. 마트 1: 포인트 및 결제 통합 마트 (mart_point_payment_event)
# 조인 테이블: accounts_pointhistory, accounts_paymenthistory, accounts_failpaymenthistory,
#              hackle_events, hackle_properties, accounts_user, accounts_userquestionrecord
# ==============================================================================
def build_mart_point_payment():
    print("--- [1] mart_point_payment_event 생성 시작 ---")

    # (1) accounts_user 로드 (유저 제재 상태 조인용)
    user = load_csv('accounts_user.csv', usecols=['id', 'ban_status'])
    user['id'] = pd.to_numeric(user['id'], errors='coerce')
    user = user.dropna(subset=['id'])
    user['id'] = user['id'].astype('int64')
    user_dict = user.set_index('id')['ban_status'].to_dict()

    # (2) accounts_userquestionrecord 로드 (Ping 컨텍스트 조인용)
    uqr = load_csv('accounts_userquestionrecord.csv', 
                   usecols=['id', 'question_id', 'has_read', 'answer_status'])
    uqr['id'] = pd.to_numeric(uqr['id'], errors='coerce')
    uqr = uqr.dropna(subset=['id'])
    uqr['id'] = uqr['id'].astype('int64')
    uqr = uqr.rename(columns={
        'id': 'user_question_record_id',
        'question_id': 'ping_question_id',
        'has_read': 'ping_has_read',
        'answer_status': 'ping_answer_status'
    })

    # (3) accounts_pointhistory 처리 (POINT_EARN / POINT_SPEND)
    ph = load_csv('accounts_pointhistory.csv')
    ph['user_id'] = pd.to_numeric(ph['user_id'], errors='coerce').astype('Int64')
    ph['delta_point'] = pd.to_numeric(ph['delta_point'], errors='coerce')
    ph['user_question_record_id'] = pd.to_numeric(ph['user_question_record_id'], errors='coerce').astype('Int64')

    ph['value_event_id'] = 'PH_' + ph['id'].astype(str)
    ph['event_type']     = np.where(ph['delta_point'] > 0, 'POINT_EARN', 'POINT_SPEND')
    ph['event_at']       = ph['created_at']
    ph['point_delta']    = ph['delta_point']
    ph['point_amount_abs']= ph['delta_point'].abs()
    ph['product_id']     = None
    ph['phone_type']     = None
    ph['is_success']     = None
    ph['session_id']     = None
    ph['source_table']   = 'accounts_pointhistory'
    ph['is_outlier']     = False

    # LEFT JOIN: accounts_user & accounts_userquestionrecord
    ph['identity_status'] = ph['user_id'].map(user_dict)
    ph = ph.merge(uqr, on='user_question_record_id', how='left')

    df_point = ph[['value_event_id','event_type','event_at','user_id','session_id',
                   'identity_status','point_delta','point_amount_abs','product_id',
                   'phone_type','is_success','user_question_record_id',
                   'ping_question_id','ping_has_read','ping_answer_status',
                   'source_table','is_outlier']].copy()

    # (4) accounts_paymenthistory (PAYMENT_SUCCESS)
    pay = load_csv('accounts_paymenthistory.csv')
    pay['user_id'] = pd.to_numeric(pay['user_id'], errors='coerce').astype('Int64')
    pay['value_event_id'] = 'PAY_' + pay['id'].astype(str)
    pay['event_type']     = 'PAYMENT_SUCCESS'
    pay['event_at']       = pay['created_at']
    pay['session_id']     = None
    pay['identity_status']= pay['user_id'].map(user_dict)
    pay['point_delta']    = None
    pay['point_amount_abs']= None
    pay['product_id']     = pay['productId']
    pay['is_success']     = True
    pay['user_question_record_id'] = None
    pay['ping_question_id']= None
    pay['ping_has_read']  = None
    pay['ping_answer_status']= None
    pay['source_table']   = 'accounts_paymenthistory'
    pay['is_outlier']     = False

    df_pay = pay[['value_event_id','event_type','event_at','user_id','session_id',
                  'identity_status','point_delta','point_amount_abs','product_id',
                  'phone_type','is_success','user_question_record_id',
                  'ping_question_id','ping_has_read','ping_answer_status',
                  'source_table','is_outlier']].copy()

    # (5) accounts_failpaymenthistory (PAYMENT_FAIL)
    fail = load_csv('accounts_failpaymenthistory.csv')
    fail['user_id'] = pd.to_numeric(fail['user_id'], errors='coerce').astype('Int64')
    fail['value_event_id'] = 'FAIL_' + fail['id'].astype(str)
    fail['event_type']     = 'PAYMENT_FAIL'
    fail['event_at']       = fail['created_at']
    fail['session_id']     = None
    fail['identity_status']= fail['user_id'].map(user_dict)
    fail['point_delta']    = None
    fail['point_amount_abs']= None
    fail['product_id']     = fail['productId']
    fail['is_success']     = False
    fail['user_question_record_id'] = None
    fail['ping_question_id']= None
    fail['ping_has_read']  = None
    fail['ping_answer_status']= None
    fail['source_table']   = 'accounts_failpaymenthistory'
    fail['is_outlier']     = False

    df_fail = fail[['value_event_id','event_type','event_at','user_id','session_id',
                    'identity_status','point_delta','point_amount_abs','product_id',
                    'phone_type','is_success','user_question_record_id',
                    'ping_question_id','ping_has_read','ping_answer_status',
                    'source_table','is_outlier']].copy()

    # (6) hackle_properties: 세션-유저 1:1 매핑 테이블 생성 (M:N 폭발 방지)
    hp = load_csv('hackle_properties.csv', usecols=['session_id', 'user_id'])
    hp['is_numeric'] = hp['user_id'].apply(
        lambda x: bool(re.match(r'^\d+$', str(x).strip())) if pd.notna(x) else False)
    hp_num = hp[hp['is_numeric']].copy()
    hp_num['user_id_int'] = hp_num['user_id'].astype('int64')
    
    # 세션당 유저가 1명인 고유 세션만 필터링
    valid_sessions = hp_num.groupby('session_id')['user_id_int'].nunique()
    valid_sessions = valid_sessions[valid_sessions == 1].index
    session_map = hp_num[hp_num['session_id'].isin(valid_sessions)][['session_id', 'user_id_int']].drop_duplicates('session_id')
    session_map.columns = ['session_id', 'mapped_uid']

    # (7) hackle_events 청크 처리 및 병합 (상점/결제 퍼널 관련 3대 이벤트만 필터링)
    target_keys = {'view_shop': 'SHOP_VIEW', 
                   'click_purchase': 'PRODUCT_CLICK', 
                   'complete_purchase': 'PURCHASE_COMPLETE_EVENT'}
    hackle_chunks = []

    for chunk in pd.read_csv(os.path.join(DATA_DIR, 'hackle_events.csv'), chunksize=500000, low_memory=False):
        sub = chunk[chunk['event_key'].isin(target_keys.keys())].copy()
        if sub.empty:
            continue
        
        # LEFT JOIN: session_map (세션 -> 유저 ID 매핑)
        sub = sub.merge(session_map, on='session_id', how='left')
        sub['value_event_id'] = 'HE_' + sub['event_id'].astype(str)
        sub['event_type']     = sub['event_key'].map(target_keys)
        sub['event_at']       = sub['event_datetime']
        sub['user_id']        = sub['mapped_uid']
        sub['identity_status']= sub['mapped_uid'].map(user_dict)
        sub['point_delta']    = None
        sub['point_amount_abs']= None
        sub['product_id']     = None
        sub['phone_type']     = None
        sub['is_success']     = None
        sub['user_question_record_id']= None
        sub['ping_question_id']= None
        sub['ping_has_read']  = None
        sub['ping_answer_status']= None
        sub['source_table']   = 'hackle_events'
        sub['is_outlier']     = False

        hackle_chunks.append(sub[['value_event_id','event_type','event_at','user_id',
                                  'session_id','identity_status','point_delta',
                                  'point_amount_abs','product_id','phone_type',
                                  'is_success','user_question_record_id',
                                  'ping_question_id','ping_has_read','ping_answer_status',
                                  'source_table','is_outlier']])

    df_hackle = pd.concat(hackle_chunks, ignore_index=True) if hackle_chunks else pd.DataFrame()

    # (8) 전체 이벤트 통합 (UNION ALL) 및 시간순 정렬
    mart_point_payment = pd.concat([df_point, df_pay, df_fail, df_hackle], ignore_index=True)
    mart_point_payment = mart_point_payment.sort_values('event_at').reset_index(drop=True)
    
    print(f"✅ mart_point_payment_event 완료: {len(mart_point_payment):,} 행")
    return mart_point_payment


# ==============================================================================
# 2. 마트 2: 안전 및 신고 통합 마트 (mart_safety_event)
# 조인 테이블: polls_questionreport, accounts_blockrecord, accounts_timelinereport,
#              accounts_user, accounts_group, accounts_school, polls_question
# ==============================================================================
def build_mart_safety():
    print("\n--- [2] mart_safety_event 생성 시작 ---")

    # (1) 유저 - 학교 - 지역(시·도) 사전 매핑 테이블 빌드
    school = load_csv('accounts_school.csv', usecols=['id', 'address'])
    school['id'] = pd.to_numeric(school['id'], errors='coerce').astype('Int64')
    school_dict = school.set_index('id')['address'].to_dict()

    grp = load_csv('accounts_group.csv', usecols=['id', 'school_id'])
    grp['id'] = pd.to_numeric(grp['id'], errors='coerce').astype('Int64')
    grp['school_id'] = pd.to_numeric(grp['school_id'], errors='coerce').astype('Int64')

    user = load_csv('accounts_user.csv', usecols=['id', 'group_id'])
    user['id'] = pd.to_numeric(user['id'], errors='coerce').astype('Int64')
    user['group_id'] = pd.to_numeric(user['group_id'], errors='coerce').astype('Int64')

    # user -> group -> school 조인
    user_school = user.merge(grp.rename(columns={'id': 'group_id'}), on='group_id', how='left')
    uid_to_school = user_school.set_index('id')['school_id'].to_dict()

    def parse_region(addr):
        if not isinstance(addr, str) or not addr.strip() or addr.strip() == '-':
            return None
        return addr.strip().split()[0]  # 첫 단어 (서울, 경기 등)

    sid_to_region = {k: parse_region(v) for k, v in school_dict.items()}

    # (2) 사유 텍스트 자연어 규칙 기반 6대 카테고리화 함수
    def categorize_reason(series):
        s = series.fillna('').str.lower()
        result = pd.Series('기타', index=s.index)
        result = result.where(~s.str.contains('욕설|비방|협박|위험|폭력|죽|살', na=False), '안전·유해')
        result = result.where(~((result == '기타') & s.str.contains('음란|성적|신체|외모|혐오|민감|야한', na=False)), '불쾌·민감')
        result = result.where(~((result == '기타') & s.str.contains('이상|의미없|스팸|광고|반복|관련없', na=False)), '질문 품질')
        result = result.where(~((result == '기타') & s.str.contains('모르는|친하지|불쾌|싫어|관심없', na=False)), '사용자 선호 불일치')
        result = result.where(~((result == '기타') & s.str.contains('좋아|재밌|재미있|좋음', na=False)), '긍정 피드백')
        return result

    # (3) polls_questionreport (질문 신고)
    q_df = load_csv('polls_question.csv', usecols=['id', 'question'])
    q_dict = q_df.set_index('id')['question'].to_dict()

    qr = load_csv('polls_questionreport.csv')
    qr['safety_event_id']   = 'QR_' + qr['id'].astype(str)
    qr['safety_event_type'] = 'QUESTION_REPORT'
    qr['event_at']          = qr['created_at']
    qr['actor_user_id']     = pd.to_numeric(qr['user_id'], errors='coerce').astype('Int64')
    qr['target_user_id']    = None
    qr['question_id']       = pd.to_numeric(qr['question_id'], errors='coerce').astype('Int64')
    qr['question_text']     = qr['question_id'].map(q_dict)
    qr['user_question_record_id'] = None
    qr['reason_raw']        = qr['reason']
    qr['reason_category']   = categorize_reason(qr['reason'])
    qr['actor_school_id']   = qr['actor_user_id'].map(uid_to_school)
    qr['actor_region']      = qr['actor_school_id'].map(sid_to_region)
    qr['target_school_id']  = None
    qr['target_region']     = None
    qr['source_table']      = 'polls_questionreport'

    # (4) accounts_blockrecord (유저 차단)
    bl = load_csv('accounts_blockrecord.csv')
    bl['safety_event_id']   = 'BL_' + bl['id'].astype(str)
    bl['safety_event_type'] = 'USER_BLOCK'
    bl['event_at']          = bl['created_at']
    bl['actor_user_id']     = pd.to_numeric(bl['user_id'], errors='coerce').astype('Int64')
    bl['target_user_id']    = pd.to_numeric(bl['block_user_id'], errors='coerce').astype('Int64')
    bl['question_id']       = None
    bl['question_text']     = None
    bl['user_question_record_id'] = None
    bl['reason_raw']        = bl['reason']
    bl['reason_category']   = categorize_reason(bl['reason'])
    bl['actor_school_id']   = bl['actor_user_id'].map(uid_to_school)
    bl['actor_region']      = bl['actor_school_id'].map(sid_to_region)
    bl['target_school_id']  = bl['target_user_id'].map(uid_to_school)
    bl['target_region']     = bl['target_school_id'].map(sid_to_region)
    bl['source_table']      = 'accounts_blockrecord'

    # (5) accounts_timelinereport (핑 신고)
    tr = load_csv('accounts_timelinereport.csv')
    tr['safety_event_id']   = 'TR_' + tr['id'].astype(str)
    tr['safety_event_type'] = 'TIMELINE_REPORT'
    tr['event_at']          = tr['created_at']
    tr['actor_user_id']     = pd.to_numeric(tr['user_id'], errors='coerce').astype('Int64')
    tr['target_user_id']    = pd.to_numeric(tr['reported_user_id'], errors='coerce').astype('Int64')
    tr['question_id']       = None
    tr['question_text']     = None
    tr['user_question_record_id'] = pd.to_numeric(tr['user_question_record_id'], errors='coerce').astype('Int64')
    tr['reason_raw']        = None
    tr['reason_category']   = '불쾌·민감'
    tr['actor_school_id']   = tr['actor_user_id'].map(uid_to_school)
    tr['actor_region']      = tr['actor_school_id'].map(sid_to_region)
    tr['target_school_id']  = tr['target_user_id'].map(uid_to_school)
    tr['target_region']     = tr['target_school_id'].map(sid_to_region)
    tr['source_table']      = 'accounts_timelinereport'

    cols = ['safety_event_id', 'safety_event_type', 'event_at', 'actor_user_id',
            'target_user_id', 'question_id', 'question_text', 'user_question_record_id',
            'reason_raw', 'reason_category', 'actor_school_id', 'actor_region',
            'target_school_id', 'target_region', 'source_table']

    mart_safety = pd.concat([qr[cols], bl[cols], tr[cols]], ignore_index=True)
    mart_safety = mart_safety.sort_values('event_at').reset_index(drop=True)

    print(f"✅ mart_safety_event 완료: {len(mart_safety):,} 행")
    return mart_safety


# ==============================================================================
# 3. 마트 3: 질문 성과 및 완주율 집계 마트 (mart_question_performance)
# 조인 테이블: polls_question, polls_questionpiece, polls_usercandidate,
#              accounts_userquestionrecord, polls_questionreport
# ==============================================================================
def build_mart_question_performance():
    print("\n--- [3] mart_question_performance 생성 시작 ---")

    # (1) polls_question 마스터 로드
    q = load_csv('polls_question.csv', usecols=['id', 'question'])
    q = q.rename(columns={'id': 'question_id', 'question': 'question_text'})

    # (2) polls_questionpiece: 질문별 노출수, 투표수, 스킵수 집계
    qp = load_csv('polls_questionpiece.csv', usecols=['question_id', 'is_voted', 'is_skipped'])
    qp_agg = qp.groupby('question_id').agg(
        expose_count=('is_voted', 'count'),
        voted_count=('is_voted', lambda x: (x == 1).sum()),
        skipped_count=('is_skipped', lambda x: (x == 1).sum())
    ).reset_index()
    qp_agg['vote_rate'] = qp_agg['voted_count'] / qp_agg['expose_count']
    qp_agg['skip_rate'] = qp_agg['skipped_count'] / qp_agg['expose_count']

    # (3) accounts_userquestionrecord: 핑 전송수, 열람수, 답장수 집계
    uqr = load_csv('accounts_userquestionrecord.csv', 
                   usecols=['question_id', 'has_read', 'answer_status'])
    uqr_agg = uqr.groupby('question_id').agg(
        ping_sent_count=('has_read', 'count'),
        ping_read_count=('has_read', lambda x: (x == 1).sum()),
        answer_count=('answer_status', lambda x: (x != 'N').sum()),
        public_answer_count=('answer_status', lambda x: (x == 'P').sum()),
        secret_answer_count=('answer_status', lambda x: (x == 'S').sum())
    ).reset_index()
    uqr_agg['ping_read_rate'] = uqr_agg['ping_read_count'] / uqr_agg['ping_sent_count']

    # (4) polls_questionreport: 질문별 신고 건수 집계
    qr = load_csv('polls_questionreport.csv', usecols=['question_id'])
    qr_agg = qr.groupby('question_id').size().reset_index(name='report_count')

    # (5) 전체 테이블 LEFT JOIN 결합
    perf = q.merge(qp_agg, on='question_id', how='left')
    perf = perf.merge(uqr_agg, on='question_id', how='left')
    perf = perf.merge(qr_agg, on='question_id', how='left')

    # 결측치 0 대체
    num_cols = ['expose_count', 'voted_count', 'skipped_count', 'vote_rate', 'skip_rate',
                'ping_sent_count', 'ping_read_count', 'ping_read_rate', 'answer_count',
                'public_answer_count', 'secret_answer_count', 'report_count']
    perf[num_cols] = perf[num_cols].fillna(0)
    perf['report_rate'] = np.where(perf['expose_count'] > 0, perf['report_count'] / perf['expose_count'], 0.0)

    print(f"✅ mart_question_performance 완료: {len(perf):,} 행")
    return perf


if __name__ == '__main__':
    print("=== 데이터 마트 생성 (Pandas 스크립트 실행 모드) ===")
    # df1 = build_mart_point_payment()
    # df2 = build_mart_safety()
    # df3 = build_mart_question_performance()
    print("함수를 개별 호출하여 필요한 마트를 생성하거나 검증할 수 있습니다.")
