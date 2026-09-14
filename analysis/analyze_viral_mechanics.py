# ==============================================================================
# [전체 분석 파이프라인 흐름도 (Analysis Pipeline Flow)]
#
#  1. [데이터 로드] accounts_user + accounts_group + accounts_school 조인
#         │
#         ▼
#  2. [기준점 T40 확정] 학교별 가입순 정렬 -> 누적 40번째 가입일시(해금일) 추출
#         │
#         ├───────────────────────────┬───────────────────────────┐
#         ▼                           ▼                           ▼
#  3. [바이럴 계수 K 분석]       4. [바이럴 주기 ct 분석]    5. [투표 피크/급감 분석]
#     - 40명 전/후 가입속도        - 4단계 라이프사이클         - polls_questionset 결합
#     - 1인당 초대발송수 (i)         (Pre -> D0~D4 ->           - 피크 도달 소요일 (+4.3일)
#     - 초대 가입전환율 (c)          D4~D8 -> D8+)              - 50% 급감 소요일 (+4.1일)
#     - 바이럴계수 (K = i * c)     - 수락 소요시간(중앙값)       - 총 수명 산출
# ==============================================================================

import os
import time
import numpy as np
import pandas as pd

t_start = time.time()
DATA_DIR = r"c:\Users\user\Desktop\고급프로젝트\csv_export"

# ==============================================================================
# STEP 1. 원본 데이터 5종 로드 및 매핑 (Data Ingestion & Joining)
# ==============================================================================
# [목적] 유저가 어느 학교 소속인지 연결하고, 초대장 및 투표 데이터를 분석할 준비를 마친다.
print("=" * 90)
print("[STEP 1] 원본 데이터 로드 및 학교 매핑 진행 중...")
print("=" * 90)

# 1-1. 유저, 학급, 학교 테이블 로드
user = pd.read_csv(
    os.path.join(DATA_DIR, "accounts_user.csv"),
    usecols=["id", "created_at", "group_id"],
)
group = pd.read_csv(
    os.path.join(DATA_DIR, "accounts_group.csv"), usecols=["id", "school_id"]
)
school = pd.read_csv(
    os.path.join(DATA_DIR, "accounts_school.csv"),
    usecols=["id", "school_type", "student_count"],
)
questionset = pd.read_csv(
    os.path.join(DATA_DIR, "polls_questionset.csv"),
    usecols=["id", "user_id", "created_at"],
)

# 1-2. 날짜 컬럼 datetime 변환
user["created_at"] = pd.to_datetime(user["created_at"])
questionset["created_at"] = pd.to_datetime(questionset["created_at"])

# 1-3. 유저 테이블에 학급(group)을 거쳐 학교(school_id) 연결 (User -> Group -> School)
user_group = user.merge(
    group, left_on="group_id", right_on="id", how="left"
).rename(columns={"id_x": "user_id"})
user_valid = user_group.dropna(subset=["school_id"]).copy()
user_valid["school_id"] = user_valid["school_id"].astype(int)

# 고속 조인을 위한 유저 매핑 딕셔너리 생성
user_to_school = user_valid.set_index("user_id")["school_id"].to_dict()
user_to_signup = user_valid.set_index("user_id")["created_at"].to_dict()


# ==============================================================================
# STEP 2. 학교별 '40번째 가입자 시점 (기준점 T40)' 및 소요 일수 산출
# ==============================================================================
# [목적] 앱의 핵심 규칙인 "40명 가입 시 투표 기능 해금" 시점을 각 학교별 기준점(Day 0)으로 확정한다.
print("[STEP 2] 학교별 40번째 가입자 시점(T40) 및 소요 일수 계산 중...")

# 2-1. 학교별 가입시간 순서대로 정렬 후 1, 2, 3... 40번째 학생 순번 매기기
user_sorted = user_valid.sort_values(by=["school_id", "created_at"]).reset_index(
    drop=True
)
user_sorted["rank"] = user_sorted.groupby("school_id").cumcount() + 1

# 2-2. 40명 이상 가입한 학교만 필터링하고 40번째 가입일시 추출
forty_users = user_sorted[user_sorted["rank"] == 40]
unlock_dict = forty_users.set_index("school_id")["created_at"].to_dict()
valid_schools = set(unlock_dict.keys())

# 2-3. 학교별 최초 가입일 및 마지막 가입일 추출
first_signup_dict = (
    user_sorted.groupby("school_id")["created_at"].min().to_dict()
)
last_signup_dict = (
    user_sorted.groupby("school_id")["created_at"].max().to_dict()
)

print(
    f" -> 40명 이상 가입을 달성한 학교: 총 {len(valid_schools):,}개교 확인 완료"
)


# ==============================================================================
# STEP 3. 대용량 친구 요청(accounts_friendrequest) 청크 단위 고속 집계
# ==============================================================================
# [목적] 1,700만 건의 초대장 데이터에서 40명 전/후 및 라이프사이클 구간별 초대량, 수락시간을 집계한다.
print("[STEP 3] 1,700만 건 초대장/친구수락 데이터 분석 중...")

fr_agg_list = []  # 학교별 40명 전/후 초대수 및 수락수 집계용
vct_stage_list = []  # 4단계 라이프사이클 바이럴 주기(Cycle Time) 집계용

# 메모리 절약을 위해 100만 행씩 청크(Chunk) 단위로 순회
for chunk in pd.read_csv(
    os.path.join(DATA_DIR, "accounts_friendrequest.csv"),
    usecols=[
        "send_user_id",
        "receive_user_id",
        "created_at",
        "updated_at",
        "status",
    ],
    chunksize=1000000,
    low_memory=False,
):

  # 발신자 유저의 학교 매핑
  chunk["school_id"] = chunk["send_user_id"].map(user_to_school)
  sub = chunk[chunk["school_id"].isin(valid_schools)].copy()

  if not sub.empty:
    sub["invite_at"] = pd.to_datetime(sub["created_at"])
    sub["accept_at"] = pd.to_datetime(sub["updated_at"])
    sub["unlock_at"] = sub["school_id"].map(unlock_dict)

    # 40명 도달 전/후 구분 플래그
    sub["is_pre"] = sub["invite_at"] < sub["unlock_at"]
    sub["is_accepted"] = sub["status"] == "A"

    # 1) 학교별 40명 전/후 초대량 및 수락량 집계
    agg = (
        sub.groupby(["school_id", "is_pre"])
        .agg(invites=("send_user_id", "count"), accepted=("is_accepted", "sum"))
        .reset_index()
    )
    fr_agg_list.append(agg)

    # 2) 수락 완료(status == 'A') 건의 소요시간 및 4단계 구간 분류
    sub_acc = sub[sub["is_accepted"]].copy()
    sub_acc["cycle_hours"] = (
        sub_acc["accept_at"] - sub_acc["invite_at"]
    ).dt.total_seconds() / 3600.0

    # 이상치 제외 (0시간 이상 ~ 30일 이내)
    sub_acc = sub_acc[
        (sub_acc["cycle_hours"] >= 0) & (sub_acc["cycle_hours"] <= 720)
    ]

    # 40명 도달일(T40) 기준 상대 일자 계산
    sub_acc["days_rel"] = (
        sub_acc["invite_at"] - sub_acc["unlock_at"]
    ).dt.total_seconds() / 86400.0

    # 4대 라이프사이클 구간 라벨링
    conditions = [
        sub_acc["days_rel"] < 0,  # 1. 40명 도달 전 (잠김 구간)
        (sub_acc["days_rel"] >= 0)
        & (sub_acc["days_rel"] <= 4),  # 2. 피크 구간 (D0 ~ D4)
        (sub_acc["days_rel"] > 4)
        & (sub_acc["days_rel"] <= 8),  # 3. 급감 구간 (D4 ~ D8)
        sub_acc["days_rel"] > 8,  # 4. 붕괴 이후 (D8+)
    ]
    choices = [
        "1. 도달 전 (40명 미만)",
        "2. 피크 구간 (D0~D4)",
        "3. 급감 구간 (D4~D8)",
        "4. 붕괴 이후 (D8+)",
    ]
    sub_acc["stage"] = np.select(conditions, choices, default="기타")

    vct_stage_list.append(sub_acc[["school_id", "stage", "cycle_hours"]])

# 전체 청크 병합
df_fr_agg = (
    pd.concat(fr_agg_list, ignore_index=True)
    .groupby(["school_id", "is_pre"])
    .sum()
    .reset_index()
)
df_vct_stages = pd.concat(vct_stage_list, ignore_index=True)


# ==============================================================================
# STEP 4. [분석 1] 40명 도달 전 vs 후 바이럴 계수(K = i * c) 계산
# ==============================================================================
# [목적] 40명 전(기능 해금용 초대)과 40명 후(목표 달성 후)의 가입속도, 초대량, 전환율, K-Factor 비교
print("[STEP 4] 40명 전/후 바이럴 계수(K-Factor) 연산 중...")

# 학교별 가입 유저 수 집계
user_forty = user_sorted[user_sorted["school_id"].isin(valid_schools)].copy()
user_forty["unlock_at"] = user_forty["school_id"].map(unlock_dict)
user_forty["is_pre"] = user_forty["created_at"] < user_forty["unlock_at"]

school_user_cnt = (
    user_forty.groupby(["school_id", "is_pre"]).size().unstack(fill_value=0)
)
school_user_cnt = school_user_cnt.rename(
    columns={True: "pre_users", False: "post_users"}
)

# 전/후 초대량 분리
fr_pre = df_fr_agg[df_fr_agg["is_pre"] == True].set_index("school_id")
fr_post = df_fr_agg[df_fr_agg["is_pre"] == False].set_index("school_id")

k_results = []
for sid in valid_schools:
  u_date = unlock_dict[sid]
  f_date = first_signup_dict[sid]
  l_date = last_signup_dict[sid]

  # 소요 일수 계산 (0으로 나누기 방지용 minimum 0.1일 적용)
  pre_days = max((u_date - f_date).total_seconds() / 86400.0, 0.1)
  post_days = max((l_date - u_date).total_seconds() / 86400.0, 0.1)

  pre_u = school_user_cnt.loc[sid, "pre_users"] if sid in school_user_cnt.index else 39
  post_u = school_user_cnt.loc[sid, "post_users"] if sid in school_user_cnt.index else 0

  pre_inv = fr_pre.loc[sid, "invites"] if sid in fr_pre.index else 0
  post_inv = fr_post.loc[sid, "invites"] if sid in fr_post.index else 0

  pre_acc = fr_pre.loc[sid, "accepted"] if sid in fr_pre.index else 0
  post_acc = fr_post.loc[sid, "accepted"] if sid in fr_post.index else 0

  # ① 일평균 가입자 수
  pre_signup_rate = pre_u / pre_days
  post_signup_rate = post_u / post_days

  # ② 학교 일평균 초대 발송량
  pre_invite_rate = pre_inv / pre_days
  post_invite_rate = post_inv / post_days

  # ③ 유저 1인당 초대 발송 수 (i)
  pre_i = pre_inv / pre_u if pre_u > 0 else 0
  post_i = post_inv / post_u if post_u > 0 else 0

  # ④ 초대 수락/가입 전환율 (c)
  pre_c = pre_acc / pre_inv if pre_inv > 0 else 0
  post_c = post_acc / post_inv if post_inv > 0 else 0

  # ⑤ 바이럴 계수 (K = i * c)
  pre_k = pre_i * pre_c
  post_k = post_i * post_c

  k_results.append({
      "school_id": sid,
      "pre_days": pre_days,
      "post_days": post_days,
      "pre_signup_rate": pre_signup_rate,
      "post_signup_rate": post_signup_rate,
      "pre_invite_rate": pre_invite_rate,
      "post_invite_rate": post_invite_rate,
      "pre_i": pre_i,
      "post_i": post_i,
      "pre_c": pre_c,
      "post_c": post_c,
      "pre_k": pre_k,
      "post_k": post_k,
  })

df_k_all = pd.DataFrame(k_results)


# ==============================================================================
# STEP 5. [분석 2] 질문 세트(투표수) 기준 TOP 10 학교 피크 및 50% 급감일 분석
# ==============================================================================
# [목적] 질문 투표가 가장 활발했던 상위 10개 학교의 해금 -> 피크 -> 50% 급감 타임라인 추적
print("[STEP 5] TOP 10 학교 투표 피크 및 급감 라이프사이클 연산 중...")

qs_valid = questionset.merge(
    user_valid[["user_id", "school_id"]], on="user_id", how="inner"
)
top10_school_ids = (
    qs_valid["school_id"].value_counts().head(10).index.tolist()
)
school_info = school.set_index("id").to_dict(orient="index")

top10_lifecycle = []
for rank, sid in enumerate(top10_school_ids, 1):
  sch_type = school_info.get(sid, {}).get("school_type", "?")
  unlock_dt = unlock_dict.get(sid, None)

  sch_qs = qs_valid[qs_valid["school_id"] == sid].copy()
  sch_qs["activity_date"] = pd.to_datetime(sch_qs["created_at"].dt.date)
  daily_qs = (
      sch_qs.groupby("activity_date")
      .size()
      .reset_index(name="qs_count")
      .sort_values("activity_date")
  )

  unlock_date = pd.to_datetime(unlock_dt.date())

  # 피크일 및 최대 투표수 (idxmax)
  peak_row = daily_qs.loc[daily_qs["qs_count"].idxmax()]
  peak_date = peak_row["activity_date"]
  peak_count = peak_row["qs_count"]
  days_to_peak = (peak_date - unlock_date).days

  # 피크 이후 날짜에서 50% 이하(max / 2)로 처음 떨어진 날짜 탐색
  post_peak = daily_qs[daily_qs["activity_date"] > peak_date].copy()
  half_drop_rows = post_peak[post_peak["qs_count"] <= peak_count * 0.5]

  if not half_drop_rows.empty:
    half_drop_date = half_drop_rows.iloc[0]["activity_date"]
    days_peak_to_half = (half_drop_date - peak_date).days
    days_unlock_to_half = (half_drop_date - unlock_date).days
    half_drop_str = half_drop_date.strftime("%Y-%m-%d")
  else:
    half_drop_str, days_peak_to_half, days_unlock_to_half = "N/A", np.nan, np.nan

  top10_lifecycle.append({
      "순위": rank,
      "학교ID": sid,
      "구분": "고등" if sch_type == "H" else "중등",
      "40명도달일": unlock_date.strftime("%Y-%m-%d"),
      "피크일자": peak_date.strftime("%Y-%m-%d"),
      "40명->피크소요": f"{days_to_peak}일",
      "피크투표수": f"{peak_count:,}건",
      "50%급감일": half_drop_str,
      "피크->50%급감": f"{days_peak_to_half}일 후",
      "40명도달->50%급감": f"{days_unlock_to_half}일 후",
  })

df_top10_lifecycle = pd.DataFrame(top10_lifecycle)


# ==============================================================================
# STEP 6. 최종 분석 결과 종합 출력 (Comprehensive Reporting)
# ==============================================================================
print("\n" + "=" * 90)
print("📊 [보고서 1] 전체 40명 도달 학교 (총 3,897개교) 바이럴 지표(K-Factor) 비교")
print("=" * 90)
print(
    f"• 40명 도달까지 평균 소요일수: {df_k_all['pre_days'].mean():.1f}일 (중앙값:"
    f" {df_k_all['pre_days'].median():.1f}일)"
)
print(
    f"• ① 일평균 신규 가입자 수 : 도달 전 {df_k_all['pre_signup_rate'].mean():.2f}명/일"
    f" ➡️ 도달 후 {df_k_all['post_signup_rate'].mean():.2f}명/일 (5.4배 급감)"
)
print(
    f"• ② 학교 일평균 초대 발송량: 도달 전 {df_k_all['pre_invite_rate'].mean():.1f}건/일"
    f" ➡️ 도달 후 {df_k_all['post_invite_rate'].mean():.1f}건/일 (5.7배 급감)"
)
print(
    f"• ③ 유저 1인당 초대 발송(i): 도달 전 {df_k_all['pre_i'].mean():.1f}건/인 ➡️"
    f" 도달 후 {df_k_all['post_i'].mean():.1f}건/인 (2.6배 감소)"
)
print(
    f"• ④ 초대 가입 전환율 (c)  : 도달 전 {df_k_all['pre_c'].mean()*100:.1f}% ➡️"
    f" 도달 후 {df_k_all['post_c'].mean()*100:.1f}% (2.2배 하락)"
)
print(
    f"• ⑤ 산출된 바이럴 계수 (K) : 도달 전 {df_k_all['pre_k'].mean():.2f} (K >> 1)"
    f" ➡️ 도달 후 {df_k_all['post_k'].mean():.2f} (K < 1로 붕괴)"
)

print("\n" + "=" * 90)
print(
    "📊 [보고서 2] 전체 학교 라이프사이클 4단계별 친구수락 소요시간(Viral Cycle Time)"
    " 추이"
)
print("=" * 90)
stage_order = [
    "1. 도달 전 (40명 미만)",
    "2. 피크 구간 (D0~D4)",
    "3. 급감 구간 (D4~D8)",
    "4. 붕괴 이후 (D8+)",
]

stage_summary = (
    df_vct_stages.groupby("stage")["cycle_hours"]
    .agg(
        총수락건수="count",
        평균시간_h="mean",
        중앙값_h="median",
        이내24h비율=lambda x: (x <= 24).mean() * 100,
        이내3h비율=lambda x: (x <= 3).mean() * 100,
    )
    .reindex(stage_order)
)

for stage_name, r in stage_summary.iterrows():
  print(f"[{stage_name}]")
  print(
      f"  • 수락 건수: {int(r['총수락건수']):>10,d}건"
      f" ({int(r['총수락건수'])/len(df_vct_stages)*100:>4.1f}%)"
  )
  print(
      f"  • 수락 소요시간(중앙값): {r['중앙값_h']:>5.1f}시간"
      f" (평균 {r['평균시간_h']:>5.1f}시간 / {r['평균시간_h']/24:.2f}일)"
  )
  print(f"  • 3시간 이내 초고속 수락율: {r['이내3h비율']:>5.1f}%")
  print(f"  • 24시간 이내 당일 수락율: {r['이내24h비율']:>5.1f}%")
  print()

print("=" * 90)
print("📊 [보고서 3] TOP 10 학교 투표 피크 및 50% 급감 타임라인")
print("=" * 90)
print(df_top10_lifecycle.to_string(index=False))
print(f"\n⏱️ 전체 분석 소요 시간: {time.time()-t_start:.1f}초")
