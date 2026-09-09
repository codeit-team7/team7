from pathlib import Path

import nbformat as nbf


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "viral_school_6marts_preprocessing.ipynb"


def md(text: str):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text: str):
    return nbf.v4.new_code_cell(text.strip())


cells = [
    md(
        r"""
# 바이럴·학교 확산 6개 마트 전처리

이 노트북은 바이럴·학교 확산 분석축에 속한 6개 마트를 같은 기준으로 전처리한다.
원본 행은 삭제하지 않고 별도 파일로 보존하며, 분석에서 제외하거나 주의해야 할 행은
플래그로 표시한다.

## 처리 대상

| 마트 | 한 행의 의미 | 기본 키 |
|---|---|---|
| `mart_user_acquisition_profile_v2` | 사용자 1명 | `user_id` |
| `mart_friend_request_event_v2` | 친구요청 원행 1건 | `request_id` |
| `mart_friend_request_pair_summary_v2` | 발신자→수신자 방향쌍 1개 | `send_user_id`, `receive_user_id` |
| `mart_user_viral_profile_v2` | 사용자 1명 | `user_id` |
| `mart_school_viral_daily_v2` | 학교 1개×날짜 1일 | `school_id`, `activity_date` |
| `mart_user_contact_record_v2` | 연락처 스냅샷 원행 1건 | `contact_record_id` |

## 전처리 원칙

1. `\N`만 결측값으로 읽고 실제 0과 구분한다.
2. 원본 행을 삭제하거나 결측을 임의로 채우지 않는다.
3. 직원·슈퍼유저는 삭제하지 않고 `analysis_eligible_nonstaff_flag`로 구분한다.
4. 전체 기간을 보존하고 2023년 5월 여부를 별도 플래그로 만든다.
5. 현재 학교·친구·학년·반은 과거 시점 정보로 바꾸지 않는다.
6. 친구요청 `A/P/R`은 최종 상태로만 해석한다.
7. 친구요청 수락률을 초대 후 가입 전환율로 해석하지 않는다.
8. 모든 파생값은 같은 기준으로 재계산하고 마트 간 합계를 대사한다.
"""
    ),
    code(
        r"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import display, Markdown

pd.set_option("display.max_columns", 120)
pd.set_option("display.max_rows", 120)
pd.set_option("display.width", 220)

REPO_ROOT = Path(r"C:\Users\lucy5\Desktop\team7")
SOURCE_DIR = REPO_ROOT / "data" / "marts" / "23_marts_csv"
OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "viral_school"
REPORT_DIR = REPO_ROOT / "analysis" / "preprocessing" / "viral_school" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 250_000
NA_VALUES = [r"\N"]
SOURCE_NAMES = [
    "mart_user_acquisition_profile_v2",
    "mart_friend_request_event_v2",
    "mart_friend_request_pair_summary_v2",
    "mart_user_viral_profile_v2",
    "mart_school_viral_daily_v2",
    "mart_user_contact_record_v2",
]

manifest = pd.read_csv(SOURCE_DIR / "export_manifest.csv", dtype="string")
source_manifest = manifest.loc[manifest["object_name"].isin(SOURCE_NAMES)].copy()
source_manifest["exported_rows"] = pd.to_numeric(source_manifest["exported_rows"], errors="coerce").astype("Int64")
source_manifest["column_count"] = pd.to_numeric(source_manifest["column_count"], errors="coerce").astype("Int64")
expected_rows = dict(zip(source_manifest["object_name"], source_manifest["exported_rows"].astype(int)))

print(f"원본 폴더: {SOURCE_DIR}")
print(f"전처리 결과 폴더: {OUTPUT_DIR}")
display(source_manifest[["object_name", "exported_rows", "column_count", "status", "sha256"]].reset_index(drop=True))
"""
    ),
    md(
        r"""
## 공통 함수와 검증 규칙

대용량 파일은 한 번에 메모리에 올리지 않고 25만 행씩 처리한다. 출력 파일은
Python에서 압축 해제 없이 읽을 수 있는 UTF-8 `CSV.gz` 형식이다.
"""
    ),
    code(
        r"""
qa_rows = []
dictionary_rows = []
processing_log = []


def add_qa(mart, test_name, actual, expected, status, severity, explanation):
    qa_rows.append({
        "mart": mart,
        "test_name": test_name,
        "actual": actual,
        "expected": expected,
        "status": status,
        "severity": severity,
        "explanation": explanation,
    })


def nullable_int(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def nullable_float(series):
    return pd.to_numeric(series, errors="coerce").astype("Float64")


def nullable_flag(series):
    return pd.to_numeric(series, errors="coerce").astype("Int8")


def datetime_series(series):
    # MySQL 추출값에는 초까지만 있는 값과 마이크로초까지 있는 값이 함께 존재한다.
    # pandas의 단일 추론 포맷에 맡기면 정상 값 한 건이 NaT가 되므로 혼합 포맷을 명시한다.
    return pd.to_datetime(series, format="mixed", errors="coerce")


def date_series(series):
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def make_nullable_flag(condition, valid_mask=None):
    result = pd.Series(pd.NA, index=condition.index, dtype="Int8")
    if valid_mask is None:
        valid_mask = pd.Series(True, index=condition.index)
    result.loc[valid_mask] = condition.loc[valid_mask].astype("int8")
    return result


def source_chunks(name, usecols=None):
    return pd.read_csv(
        SOURCE_DIR / f"{name}.csv.gz",
        compression="gzip",
        encoding="utf-8-sig",
        na_values=NA_VALUES,
        keep_default_na=False,
        dtype="string",
        usecols=usecols,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )


def write_processed(name, transform):
    source_path = SOURCE_DIR / f"{name}.csv.gz"
    output_path = OUTPUT_DIR / f"{name}_clean.csv.gz"
    row_count = 0
    output_columns = None
    with gzip.open(output_path, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        for chunk_no, raw in enumerate(source_chunks(name), start=1):
            clean = transform(raw.copy())
            if output_columns is None:
                output_columns = clean.columns.tolist()
            elif output_columns != clean.columns.tolist():
                raise ValueError(f"{name}: 청크별 컬럼 구성이 달라졌습니다.")
            clean.to_csv(
                handle,
                index=False,
                header=(chunk_no == 1),
                na_rep=r"\N",
                date_format="%Y-%m-%d %H:%M:%S.%f",
            )
            row_count += len(clean)
            if chunk_no == 1 or chunk_no % 20 == 0:
                print(f"  {name}: {row_count:,}행 처리", flush=True)
    processing_log.append({
        "mart": name,
        "source_path": str(source_path),
        "output_path": str(output_path),
        "source_rows": expected_rows[name],
        "output_rows": row_count,
        "row_preserved": row_count == expected_rows[name],
        "output_columns": len(output_columns or []),
    })
    add_qa(
        name,
        "row_count_preserved",
        row_count,
        expected_rows[name],
        "PASS" if row_count == expected_rows[name] else "FAIL",
        "CRITICAL",
        "전처리 전후 행 수가 같아야 합니다.",
    )
    print(f"완료: {name} → {row_count:,}행, {len(output_columns or []):,}열")
    return output_path


def add_dictionary(mart, columns, derived_descriptions):
    for col in columns:
        dictionary_rows.append({
            "mart": mart,
            "column_name": col,
            "column_origin": "DERIVED" if col in derived_descriptions else "SOURCE_PRESERVED",
            "description": derived_descriptions.get(col, "원본 v2 마트 컬럼. 원래 이름과 의미를 보존함."),
        })


def safe_rate(numerator, denominator):
    result = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    valid = denominator.notna() & denominator.gt(0)
    result.loc[valid] = numerator.loc[valid].astype("Float64") / denominator.loc[valid].astype("Float64")
    return result


def flag_equal(left, right, tolerance=0.0):
    valid = left.notna() & right.notna()
    if tolerance:
        condition = (left.astype("Float64") - right.astype("Float64")).abs().le(tolerance)
    else:
        condition = left.eq(right)
    return make_nullable_flag(condition, valid)


print("공통 전처리 함수 준비 완료")
"""
    ),
    md(
        r"""
## 1. 사용자 가입·학교 기준정보

사용자 원행은 모두 보존한다. 직원과 슈퍼유저는 삭제하지 않고 분석 대상 여부를
표시한다. 학교의 40번째 가입 시각은 데이터에서 관측된 기준점이며 실제 기능 해금
시각이라고 단정하지 않는다.
"""
    ),
    code(
        r"""
USER_MART = "mart_user_acquisition_profile_v2"
user_ids_seen = []
user_lookup_parts = []
user_invalid_datetime = defaultdict(int)

user_id_cols = ["user_id", "current_group_id", "current_school_id", "contact_record_id"]
user_integer_cols = [
    "current_point_snapshot", "current_friend_list_length", "current_block_list_length",
    "current_hide_list_length", "current_roster_school_signup_rank", "current_roster_account_count",
    "seconds_first_to_40th_current_roster", "contact_source_row_count", "contacts_count_source",
    "invite_user_id_list_length", "nearby_relation_row_count", "nearby_school_count",
    "nearby_self_relation_count", "nearby_zero_distance_count",
]
user_float_cols = ["nearby_distance_min_raw", "nearby_distance_avg_raw", "nearby_distance_max_raw"]
user_flag_cols = [
    "is_superuser", "is_staff", "is_push_on", "current_friend_json_valid",
    "current_block_json_valid", "current_hide_json_valid", "current_roster_reached_40_flag",
    "contacts_observed_flag", "invite_list_json_valid",
]
user_datetime_cols = [
    "signup_at", "current_roster_first_signup_at", "current_roster_40th_signup_at",
    "current_roster_last_signup_at",
]


def transform_user_acquisition(df):
    original_datetime = {c: df[c].copy() for c in user_datetime_cols}
    for c in user_id_cols + user_integer_cols:
        df[c] = nullable_int(df[c])
    for c in user_float_cols:
        df[c] = nullable_float(df[c])
    for c in user_flag_cols:
        df[c] = nullable_flag(df[c])
    for c in user_datetime_cols:
        df[c] = datetime_series(df[c])
        user_invalid_datetime[c] += int(original_datetime[c].notna().sum() - df[c].notna().sum())

    df["analysis_eligible_nonstaff_flag"] = (
        df["is_staff"].fillna(0).eq(0) & df["is_superuser"].fillna(0).eq(0)
    ).astype("Int8")
    df["has_current_school_flag"] = df["current_school_id"].notna().astype("Int8")
    df["signup_date"] = df["signup_at"].dt.normalize()
    df["signup_year_month"] = df["signup_at"].dt.strftime("%Y-%m").astype("string")
    df["is_2023_may_signup_flag"] = (
        df["signup_at"].dt.to_period("M").eq(pd.Period("2023-05"))
    ).astype("Int8")

    valid_first = df["signup_at"].notna() & df["current_roster_first_signup_at"].notna()
    valid_40 = df["signup_at"].notna() & df["current_roster_40th_signup_at"].notna()
    df["days_signup_from_school_first"] = (
        (df["signup_at"] - df["current_roster_first_signup_at"]).dt.total_seconds() / 86400
    ).astype("Float64")
    df["days_signup_from_observed_40th"] = (
        (df["signup_at"] - df["current_roster_40th_signup_at"]).dt.total_seconds() / 86400
    ).astype("Float64")
    df["signup_on_or_after_school_first_flag"] = make_nullable_flag(
        df["signup_at"].ge(df["current_roster_first_signup_at"]), valid_first
    )
    df["signup_on_or_after_observed_40th_flag"] = make_nullable_flag(
        df["signup_at"].ge(df["current_roster_40th_signup_at"]), valid_40
    )

    phase = pd.Series("NO_CURRENT_SCHOOL", index=df.index, dtype="string")
    has_school = df["current_school_id"].notna()
    phase.loc[has_school & df["current_roster_40th_signup_at"].isna()] = "SCHOOL_NOT_OBSERVED_REACHED_40"
    phase.loc[valid_40 & df["signup_at"].lt(df["current_roster_40th_signup_at"])] = "BEFORE_OBSERVED_40TH"
    phase.loc[valid_40 & df["signup_at"].ge(df["current_roster_40th_signup_at"])] = "ON_OR_AFTER_OBSERVED_40TH"
    df["signup_observed_40_phase"] = phase

    user_ids_seen.append(df["user_id"])
    user_lookup_parts.append(df[[
        "user_id", "is_staff", "is_superuser", "analysis_eligible_nonstaff_flag",
        "current_school_id", "signup_at", "current_roster_first_signup_at",
        "current_roster_40th_signup_at",
    ]].copy())
    return df


user_output = write_processed(USER_MART, transform_user_acquisition)
user_lookup = pd.concat(user_lookup_parts, ignore_index=True)
user_ids = pd.concat(user_ids_seen, ignore_index=True)

duplicate_user_ids = int(user_ids.duplicated(keep=False).sum())
add_qa(USER_MART, "duplicate_user_id_rows", duplicate_user_ids, 0,
       "PASS" if duplicate_user_ids == 0 else "FAIL", "CRITICAL", "사용자 마트는 user_id 1행이어야 합니다.")
for col, count in user_invalid_datetime.items():
    add_qa(USER_MART, f"invalid_datetime_{col}", count, 0,
           "PASS" if count == 0 else "FAIL", "HIGH", "값이 있었지만 날짜로 변환되지 않은 행 수입니다.")

user_lookup = user_lookup.set_index("user_id", drop=False)
eligibility_map = user_lookup["analysis_eligible_nonstaff_flag"]
school_map = user_lookup["current_school_id"]
user_position_map = pd.Series(np.arange(len(user_lookup), dtype=np.int64), index=user_lookup.index)

user_derived = {
    "analysis_eligible_nonstaff_flag": "직원과 슈퍼유저가 아닌 계정이면 1. 행은 삭제하지 않음.",
    "has_current_school_flag": "현재 학교 ID가 관측되면 1.",
    "signup_date": "가입 시각에서 만든 날짜.",
    "signup_year_month": "가입 연월 YYYY-MM.",
    "is_2023_may_signup_flag": "가입 시각이 2023년 5월이면 1.",
    "days_signup_from_school_first": "현재 roster 기준 학교 첫 가입부터 해당 사용자 가입까지 경과일.",
    "days_signup_from_observed_40th": "현재 roster 기준 학교 40번째 가입부터 해당 사용자 가입까지 경과일.",
    "signup_on_or_after_school_first_flag": "사용자 가입이 현재 roster의 첫 가입과 같거나 이후면 1.",
    "signup_on_or_after_observed_40th_flag": "사용자 가입이 관측된 40번째 가입과 같거나 이후면 1. 40명 미도달 학교는 결측.",
    "signup_observed_40_phase": "NO_CURRENT_SCHOOL, SCHOOL_NOT_OBSERVED_REACHED_40, BEFORE_OBSERVED_40TH, ON_OR_AFTER_OBSERVED_40TH 구분.",
}
add_dictionary(USER_MART, pd.read_csv(user_output, nrows=0).columns, user_derived)

display(pd.DataFrame(processing_log).tail(1))
display(pd.Series({
    "전체 사용자": len(user_lookup),
    "분석 대상 비직원·비슈퍼": int(eligibility_map.fillna(0).sum()),
    "현재 학교 ID 보유": int(school_map.notna().sum()),
    "2023년 5월 가입": int(pd.read_csv(user_output, usecols=["is_2023_may_signup_flag"], na_values=NA_VALUES)["is_2023_may_signup_flag"].sum()),
}))
"""
    ),
    md(
        r"""
## 2. 연락처·초대자 스냅샷

이 데이터에는 발송 시각이 없으므로 초대 이벤트처럼 사용하지 않는다. JSON 배열의
유효성과 길이를 다시 계산하고, 현재 사용자 기준정보와 연결 가능한지만 표시한다.
"""
    ),
    code(
        r"""
CONTACT_MART = "mart_user_contact_record_v2"
contact_ids = []
contact_json_invalid_recalc = 0


def parse_invite_json(value):
    if pd.isna(value):
        return pd.NA
    try:
        parsed = json.loads(value)
        return len(parsed) if isinstance(parsed, list) else pd.NA
    except Exception:
        return pd.NA


def transform_contact(df):
    global contact_json_invalid_recalc
    for c in ["contact_record_id", "user_id", "contacts_count_source", "invite_user_id_list_length"]:
        df[c] = nullable_int(df[c])
    for c in ["invite_list_json_valid", "orphan_user_flag"]:
        df[c] = nullable_flag(df[c])

    recalc = df["invite_user_id_list_json"].map(parse_invite_json).astype("Int64")
    df["invite_user_id_list_length_recalc"] = recalc
    df["invite_length_match_flag"] = flag_equal(df["invite_user_id_list_length"], recalc)
    df["has_inviter_reference_flag"] = recalc.fillna(0).gt(0).astype("Int8")
    df["contact_owner_known_user_flag"] = df["user_id"].isin(user_lookup.index).astype("Int8")
    df["analysis_eligible_nonstaff_flag"] = df["user_id"].map(eligibility_map).astype("Int8")
    df["contact_owner_current_school_id"] = df["user_id"].map(school_map).astype("Int64")
    df["contacts_count_nonnegative_flag"] = make_nullable_flag(
        df["contacts_count_source"].ge(0), df["contacts_count_source"].notna()
    )
    contact_json_invalid_recalc += int(df["invite_user_id_list_json"].notna().sum() - recalc.notna().sum())
    contact_ids.append(df["contact_record_id"])
    return df


contact_output = write_processed(CONTACT_MART, transform_contact)
contact_id_series = pd.concat(contact_ids, ignore_index=True)
contact_dup = int(contact_id_series.duplicated(keep=False).sum())
add_qa(CONTACT_MART, "duplicate_contact_record_id_rows", contact_dup, 0,
       "PASS" if contact_dup == 0 else "FAIL", "CRITICAL", "contact_record_id는 원행 기본 키입니다.")
add_qa(CONTACT_MART, "invalid_invite_json_recalculated", contact_json_invalid_recalc, 0,
       "PASS" if contact_json_invalid_recalc == 0 else "WARN", "MEDIUM", "JSON 값이 있으나 리스트로 읽히지 않은 행 수입니다.")

contact_clean = pd.read_csv(contact_output, compression="gzip", encoding="utf-8-sig",
                            na_values=NA_VALUES, keep_default_na=False, low_memory=False)
contact_profile = pd.read_csv(
    user_output,
    compression="gzip",
    encoding="utf-8-sig",
    usecols=["user_id", "contacts_observed_flag", "contact_record_id", "contacts_count_source", "invite_user_id_list_length"],
    na_values=NA_VALUES,
    keep_default_na=False,
    low_memory=False,
)
contact_join = contact_clean.merge(contact_profile, on="user_id", how="left", suffixes=("_contact", "_profile"))
contact_reconcile = (
    contact_join["contact_record_id_contact"].fillna(-1).eq(contact_join["contact_record_id_profile"].fillna(-1))
    & contact_join["contacts_count_source_contact"].fillna(-1).eq(contact_join["contacts_count_source_profile"].fillna(-1))
    & contact_join["invite_user_id_list_length_contact"].fillna(-1).eq(contact_join["invite_user_id_list_length_profile"].fillna(-1))
)
contact_mismatch = int((~contact_reconcile).sum())
add_qa(CONTACT_MART, "contact_fields_match_user_profile_rows", contact_mismatch, 0,
       "PASS" if contact_mismatch == 0 else "FAIL", "HIGH", "연락처 원행과 사용자 프로필의 연락처 파생값 대사입니다.")

contact_derived = {
    "invite_user_id_list_length_recalc": "JSON 배열을 Python에서 다시 센 길이.",
    "invite_length_match_flag": "원천 파생 길이와 Python 재계산 길이가 같으면 1.",
    "has_inviter_reference_flag": "초대자 ID 배열에 원소가 하나 이상 있으면 1.",
    "contact_owner_known_user_flag": "연락처 소유 user_id가 사용자 기준정보에 있으면 1.",
    "analysis_eligible_nonstaff_flag": "연락처 소유자가 직원·슈퍼유저가 아닌 계정이면 1.",
    "contact_owner_current_school_id": "연락처 소유자의 현재 학교 ID. 과거 초대 당시 학교가 아님.",
    "contacts_count_nonnegative_flag": "연락처 수가 0 이상이면 1.",
}
add_dictionary(CONTACT_MART, contact_clean.columns, contact_derived)
display(contact_clean.head())
"""
    ),
    md(
        r"""
## 3. 친구요청 원행

17,147,175개 요청을 청크로 처리한다. 사용자 기준정보를 연결하되 컬럼명에
`current`를 남겨 요청 당시 학교로 오해하지 않도록 한다. 비정상 시각, 자기 자신에게
보낸 요청, 사용자 연결 실패를 삭제하지 않고 플래그로 남긴다.
"""
    ),
    code(
        r"""
EVENT_MART = "mart_friend_request_event_v2"
n_users = len(user_lookup)
sent_arrays = {k: np.zeros(n_users, dtype=np.int64) for k in ["total", "A", "P", "R"]}
recv_arrays = {k: np.zeros(n_users, dtype=np.int64) for k in ["total", "A", "P", "R"]}
event_agg = defaultdict(int)
event_min_created = None
event_max_created = None
event_school_daily_parts = []
event_pair_sample_parts = []


def accumulate_user_array(ids, status, arrays):
    positions = ids.map(user_position_map)
    valid = positions.notna()
    pos = positions.loc[valid].astype(np.int64).to_numpy()
    arrays["total"] += np.bincount(pos, minlength=n_users)
    for status_code in ["A", "P", "R"]:
        selected = valid & status.eq(status_code)
        selected_pos = positions.loc[selected].astype(np.int64).to_numpy()
        arrays[status_code] += np.bincount(selected_pos, minlength=n_users)


def transform_friend_event(df):
    global event_min_created, event_max_created
    original_created = df["request_created_at_raw"].copy()
    original_updated = df["request_updated_at_raw"].copy()
    for c in ["request_id", "send_user_id", "receive_user_id", "created_to_last_update_seconds"]:
        df[c] = nullable_int(df[c])
    for c in ["negative_update_interval_flag", "self_request_flag"]:
        df[c] = nullable_flag(df[c])
    for c in ["request_created_at_raw", "request_updated_at_raw"]:
        df[c] = datetime_series(df[c])
    for c in ["request_created_date_raw", "request_updated_date_raw"]:
        df[c] = date_series(df[c])

    event_agg["invalid_created_datetime"] += int(original_created.notna().sum() - df["request_created_at_raw"].notna().sum())
    event_agg["invalid_updated_datetime"] += int(original_updated.notna().sum() - df["request_updated_at_raw"].notna().sum())
    event_agg["rows"] += len(df)
    for code_value in ["A", "P", "R"]:
        event_agg[f"status_{code_value}"] += int(df["final_status_code"].eq(code_value).sum())

    df["final_status_label"] = df["final_status_code"].map({
        "A": "FINAL_ACCEPTED", "P": "FINAL_PENDING", "R": "FINAL_REJECTED"
    }).astype("string")
    df["status_code_valid_flag"] = df["final_status_code"].isin(["A", "P", "R"]).astype("Int8")
    df["request_created_year_month"] = df["request_created_at_raw"].dt.strftime("%Y-%m").astype("string")
    df["is_2023_may_request_flag"] = (
        df["request_created_at_raw"].dt.to_period("M").eq(pd.Period("2023-05"))
    ).astype("Int8")
    interval_recalc = (
        df["request_updated_at_raw"] - df["request_created_at_raw"]
    ).dt.total_seconds().round().astype("Int64")
    df["created_to_last_update_seconds_recalc"] = interval_recalc
    df["interval_seconds_match_flag"] = flag_equal(df["created_to_last_update_seconds"], interval_recalc)
    valid_timestamp = df["request_created_at_raw"].notna() & df["request_updated_at_raw"].notna()
    df["request_timestamp_valid_flag"] = valid_timestamp.astype("Int8")

    df["sender_known_user_flag"] = df["send_user_id"].isin(user_lookup.index).astype("Int8")
    df["receiver_known_user_flag"] = df["receive_user_id"].isin(user_lookup.index).astype("Int8")
    df["sender_analysis_eligible_nonstaff_flag"] = df["send_user_id"].map(eligibility_map).astype("Int8")
    df["receiver_analysis_eligible_nonstaff_flag"] = df["receive_user_id"].map(eligibility_map).astype("Int8")
    df["sender_current_school_id"] = df["send_user_id"].map(school_map).astype("Int64")
    df["receiver_current_school_id"] = df["receive_user_id"].map(school_map).astype("Int64")
    same_school_valid = df["sender_current_school_id"].notna() & df["receiver_current_school_id"].notna()
    df["same_school_current_flag"] = make_nullable_flag(
        df["sender_current_school_id"].eq(df["receiver_current_school_id"]), same_school_valid
    )
    df["analysis_eligible_request_flag"] = (
        df["sender_known_user_flag"].eq(1)
        & df["receiver_known_user_flag"].eq(1)
        & df["sender_analysis_eligible_nonstaff_flag"].fillna(0).eq(1)
        & df["receiver_analysis_eligible_nonstaff_flag"].fillna(0).eq(1)
        & df["self_request_flag"].fillna(0).eq(0)
        & df["negative_update_interval_flag"].fillna(0).eq(0)
        & df["request_timestamp_valid_flag"].eq(1)
        & df["status_code_valid_flag"].eq(1)
    ).astype("Int8")

    accumulate_user_array(df["send_user_id"], df["final_status_code"], sent_arrays)
    accumulate_user_array(df["receive_user_id"], df["final_status_code"], recv_arrays)

    for side, user_col, school_col in [
        ("sent", "send_user_id", "sender_current_school_id"),
        ("received", "receive_user_id", "receiver_current_school_id"),
    ]:
        valid_school = df[school_col].notna() & df["request_created_date_raw"].notna()
        side_df = df.loc[valid_school, [school_col, "request_created_date_raw", "final_status_code"]].copy()
        side_df["request_count"] = 1
        side_df["accepted_count"] = side_df["final_status_code"].eq("A").astype(np.int64)
        side_df["pending_count"] = side_df["final_status_code"].eq("P").astype(np.int64)
        side_df["rejected_count"] = side_df["final_status_code"].eq("R").astype(np.int64)
        grouped = side_df.groupby([school_col, "request_created_date_raw"], observed=True)[
            ["request_count", "accepted_count", "pending_count", "rejected_count"]
        ].sum().reset_index()
        grouped.columns = ["school_id", "activity_date", f"{side}_request_count_event",
                           f"{side}_accepted_count_event", f"{side}_pending_count_event", f"{side}_rejected_count_event"]
        event_school_daily_parts.append((side, grouped))

    sample_mask = (
        (df["send_user_id"].fillna(-1).astype("int64") * 31
         + df["receive_user_id"].fillna(-1).astype("int64")) % 1000
    ).eq(0)
    sample = df.loc[sample_mask, [
        "send_user_id", "receive_user_id", "final_status_code",
        "request_created_at_raw", "request_updated_at_raw",
    ]].copy()
    if len(sample):
        sample["request_record_count"] = 1
        sample["final_accepted_record_count"] = sample["final_status_code"].eq("A").astype(np.int64)
        sample["final_pending_record_count"] = sample["final_status_code"].eq("P").astype(np.int64)
        sample["final_rejected_record_count"] = sample["final_status_code"].eq("R").astype(np.int64)
        event_pair_sample_parts.append(sample)

    chunk_min = df["request_created_at_raw"].min()
    chunk_max = df["request_created_at_raw"].max()
    if pd.notna(chunk_min):
        event_min_created = chunk_min if event_min_created is None else min(event_min_created, chunk_min)
    if pd.notna(chunk_max):
        event_max_created = chunk_max if event_max_created is None else max(event_max_created, chunk_max)
    return df


event_output = write_processed(EVENT_MART, transform_friend_event)
event_status_total = sum(event_agg[f"status_{x}"] for x in ["A", "P", "R"])
add_qa(EVENT_MART, "valid_status_code_rows", event_status_total, event_agg["rows"],
       "PASS" if event_status_total == event_agg["rows"] else "FAIL", "CRITICAL", "상태 코드는 A/P/R 중 하나여야 합니다.")
for metric in ["invalid_created_datetime", "invalid_updated_datetime"]:
    add_qa(EVENT_MART, metric, event_agg[metric], 0,
           "PASS" if event_agg[metric] == 0 else "FAIL", "HIGH", "값이 있었지만 날짜로 변환되지 않은 행 수입니다.")

event_derived = {
    "final_status_label": "A/P/R을 FINAL_ACCEPTED/FINAL_PENDING/FINAL_REJECTED로 읽기 쉽게 표시. 최종 상태임.",
    "status_code_valid_flag": "최종 상태 코드가 A/P/R이면 1.",
    "request_created_year_month": "요청 생성 연월 YYYY-MM.",
    "is_2023_may_request_flag": "요청 생성 시각이 2023년 5월이면 1.",
    "created_to_last_update_seconds_recalc": "수정 시각-생성 시각을 초로 재계산.",
    "interval_seconds_match_flag": "원천 경과초와 Python 재계산 값이 같으면 1.",
    "request_timestamp_valid_flag": "생성·수정 시각이 모두 유효하면 1.",
    "sender_known_user_flag": "발신자가 사용자 기준정보에 있으면 1.",
    "receiver_known_user_flag": "수신자가 사용자 기준정보에 있으면 1.",
    "sender_analysis_eligible_nonstaff_flag": "발신자가 비직원·비슈퍼 계정이면 1.",
    "receiver_analysis_eligible_nonstaff_flag": "수신자가 비직원·비슈퍼 계정이면 1.",
    "sender_current_school_id": "발신자의 현재 학교 ID. 요청 당시 학교가 아님.",
    "receiver_current_school_id": "수신자의 현재 학교 ID. 요청 당시 학교가 아님.",
    "same_school_current_flag": "현재 학교 ID가 둘 다 관측되고 같으면 1. 요청 당시 동일 학교를 뜻하지 않음.",
    "analysis_eligible_request_flag": "양쪽 사용자 식별·비직원·유효 시각·유효 상태·비자기요청·비음수 구간 조건을 모두 만족하면 1.",
}
add_dictionary(EVENT_MART, pd.read_csv(event_output, nrows=0).columns, event_derived)
display(pd.Series({
    "요청 원행": event_agg["rows"],
    "최종 수락 A": event_agg["status_A"],
    "최종 대기 P": event_agg["status_P"],
    "최종 거절 R": event_agg["status_R"],
    "최초 요청 생성": event_min_created,
    "최종 요청 생성": event_max_created,
}))
"""
    ),
    md(
        r"""
## 4. 발신자→수신자 관계 요약

방향이 있는 사용자쌍을 보존한다. 반대 방향은 다른 쌍이다. 원행 상태 합계와
요약 상태 합계가 일치하는지 전체 합계와 결정적 0.1% 쌍 표본으로 검증한다.
"""
    ),
    code(
        r"""
PAIR_MART = "mart_friend_request_pair_summary_v2"
pair_agg = defaultdict(int)
pair_sample_clean_parts = []


def transform_pair(df):
    for c in [
        "send_user_id", "receive_user_id", "request_record_count", "final_accepted_record_count",
        "final_pending_record_count", "final_rejected_record_count",
    ]:
        df[c] = nullable_int(df[c])
    for c in [
        "first_request_created_at_raw", "last_request_created_at_raw",
        "first_accepted_effective_at_proxy", "last_accepted_effective_at_proxy",
        "first_last_update_at_raw", "last_last_update_at_raw",
    ]:
        df[c] = datetime_series(df[c])

    status_sum = (
        df["final_accepted_record_count"].fillna(0)
        + df["final_pending_record_count"].fillna(0)
        + df["final_rejected_record_count"].fillna(0)
    ).astype("Int64")
    df["status_count_sum"] = status_sum
    df["status_total_match_flag"] = flag_equal(df["request_record_count"], status_sum)
    df["repeated_directional_pair_flag"] = df["request_record_count"].fillna(0).gt(1).astype("Int8")
    df["pair_created_interval_valid_flag"] = make_nullable_flag(
        df["first_request_created_at_raw"].le(df["last_request_created_at_raw"]),
        df["first_request_created_at_raw"].notna() & df["last_request_created_at_raw"].notna(),
    )
    df["sender_known_user_flag"] = df["send_user_id"].isin(user_lookup.index).astype("Int8")
    df["receiver_known_user_flag"] = df["receive_user_id"].isin(user_lookup.index).astype("Int8")
    df["sender_analysis_eligible_nonstaff_flag"] = df["send_user_id"].map(eligibility_map).astype("Int8")
    df["receiver_analysis_eligible_nonstaff_flag"] = df["receive_user_id"].map(eligibility_map).astype("Int8")
    df["sender_current_school_id"] = df["send_user_id"].map(school_map).astype("Int64")
    df["receiver_current_school_id"] = df["receive_user_id"].map(school_map).astype("Int64")
    valid_school = df["sender_current_school_id"].notna() & df["receiver_current_school_id"].notna()
    df["same_school_current_flag"] = make_nullable_flag(
        df["sender_current_school_id"].eq(df["receiver_current_school_id"]), valid_school
    )
    df["analysis_eligible_pair_flag"] = (
        df["sender_known_user_flag"].eq(1)
        & df["receiver_known_user_flag"].eq(1)
        & df["sender_analysis_eligible_nonstaff_flag"].fillna(0).eq(1)
        & df["receiver_analysis_eligible_nonstaff_flag"].fillna(0).eq(1)
        & df["send_user_id"].ne(df["receive_user_id"])
        & df["status_total_match_flag"].fillna(0).eq(1)
        & df["pair_created_interval_valid_flag"].fillna(0).eq(1)
    ).astype("Int8")

    pair_agg["rows"] += len(df)
    for col in ["request_record_count", "final_accepted_record_count", "final_pending_record_count", "final_rejected_record_count"]:
        pair_agg[col] += int(df[col].fillna(0).sum())
    pair_agg["status_mismatch_rows"] += int(df["status_total_match_flag"].fillna(0).ne(1).sum())

    sample_mask = (
        (df["send_user_id"].fillna(-1).astype("int64") * 31
         + df["receive_user_id"].fillna(-1).astype("int64")) % 1000
    ).eq(0)
    pair_sample_clean_parts.append(df.loc[sample_mask, [
        "send_user_id", "receive_user_id", "request_record_count",
        "final_accepted_record_count", "final_pending_record_count", "final_rejected_record_count",
    ]].copy())
    return df


pair_output = write_processed(PAIR_MART, transform_pair)
add_qa(PAIR_MART, "status_count_mismatch_rows", pair_agg["status_mismatch_rows"], 0,
       "PASS" if pair_agg["status_mismatch_rows"] == 0 else "FAIL", "CRITICAL", "A+P+R 합은 요청 원행 수와 같아야 합니다.")

event_sample = pd.concat(event_pair_sample_parts, ignore_index=True)
event_sample_agg = event_sample.groupby(["send_user_id", "receive_user_id"], observed=True).agg(
    request_record_count=("request_record_count", "sum"),
    final_accepted_record_count=("final_accepted_record_count", "sum"),
    final_pending_record_count=("final_pending_record_count", "sum"),
    final_rejected_record_count=("final_rejected_record_count", "sum"),
).reset_index()
pair_sample = pd.concat(pair_sample_clean_parts, ignore_index=True)
pair_sample_check = pair_sample.merge(
    event_sample_agg, on=["send_user_id", "receive_user_id"], how="outer", suffixes=("_pair", "_event"), indicator=True
)
pair_sample_mismatch = int(
    pair_sample_check["_merge"].ne("both").sum()
    + sum(
        pair_sample_check[f"{c}_pair"].fillna(-1).ne(pair_sample_check[f"{c}_event"].fillna(-1)).sum()
        for c in ["request_record_count", "final_accepted_record_count", "final_pending_record_count", "final_rejected_record_count"]
    )
)
add_qa(PAIR_MART, "deterministic_pair_sample_mismatches", pair_sample_mismatch, 0,
       "PASS" if pair_sample_mismatch == 0 else "FAIL", "HIGH", "결정적 해시 0.1% 방향쌍을 원행에서 다시 집계해 비교합니다.")

pair_event_tests = [
    ("request_record_count", event_agg["rows"]),
    ("final_accepted_record_count", event_agg["status_A"]),
    ("final_pending_record_count", event_agg["status_P"]),
    ("final_rejected_record_count", event_agg["status_R"]),
]
for metric, event_value in pair_event_tests:
    pair_value = pair_agg[metric]
    add_qa(PAIR_MART, f"{metric}_matches_event", pair_value, event_value,
           "PASS" if pair_value == event_value else "FAIL", "CRITICAL", "방향쌍 합계와 친구요청 원행 합계를 대사합니다.")

pair_derived = {
    "status_count_sum": "방향쌍의 최종 A/P/R 레코드 수 합.",
    "status_total_match_flag": "A/P/R 합이 전체 요청 레코드 수와 같으면 1.",
    "repeated_directional_pair_flag": "같은 발신자→수신자 방향으로 2건 이상 요청이 있으면 1.",
    "pair_created_interval_valid_flag": "최초 요청시각이 최종 요청시각보다 늦지 않으면 1.",
    "sender_known_user_flag": "발신자가 사용자 기준정보에 있으면 1.",
    "receiver_known_user_flag": "수신자가 사용자 기준정보에 있으면 1.",
    "sender_analysis_eligible_nonstaff_flag": "발신자가 비직원·비슈퍼 계정이면 1.",
    "receiver_analysis_eligible_nonstaff_flag": "수신자가 비직원·비슈퍼 계정이면 1.",
    "sender_current_school_id": "발신자의 현재 학교 ID.",
    "receiver_current_school_id": "수신자의 현재 학교 ID.",
    "same_school_current_flag": "현재 학교가 둘 다 관측되고 같으면 1.",
    "analysis_eligible_pair_flag": "양쪽 사용자 식별·비직원·비자기쌍·내부 합계·시각 조건을 모두 만족하면 1.",
}
add_dictionary(PAIR_MART, pd.read_csv(pair_output, nrows=0).columns, pair_derived)
display(pd.Series(pair_agg))
"""
    ),
    md(
        r"""
## 5. 사용자별 바이럴 프로필

사용자별 발신·수신·상태 수를 친구요청 원행에서 다시 집계한 값과 전수 대사한다.
비율의 분모가 0이면 실제 0%가 아니라 계산 불가이므로 결측으로 유지한다.
"""
    ),
    code(
        r"""
VIRAL_MART = "mart_user_viral_profile_v2"
viral_ids = []
viral_agg = defaultdict(int)


def transform_viral_profile(df):
    int_cols = [
        "user_id", "current_group_id", "current_school_id", "current_grade", "current_class_num",
        "current_friend_list_length", "contacts_count_source", "invite_user_id_list_length",
        "sent_request_count", "sent_unique_partner_count", "sent_final_accepted_count",
        "sent_final_pending_count", "sent_final_rejected_count", "sent_repeated_request_count",
        "sent_request_first_24h_count", "sent_request_first_72h_count", "sent_request_first_7d_count",
        "received_request_count", "received_unique_partner_count", "received_final_accepted_count",
        "received_final_pending_count", "received_final_rejected_count", "received_request_first_24h_count",
        "received_request_first_72h_count", "received_request_first_7d_count",
        "total_request_endpoint_activity_count",
    ]
    for c in int_cols:
        df[c] = nullable_int(df[c])
    for c in ["is_staff", "is_superuser", "contacts_observed_flag", "has_any_friend_request_activity_flag"]:
        df[c] = nullable_flag(df[c])
    for c in [
        "signup_at", "current_roster_first_signup_at", "current_roster_40th_signup_at",
        "first_sent_request_at_raw", "last_sent_request_at_raw",
        "first_received_request_at_raw", "last_received_request_at_raw",
    ]:
        df[c] = datetime_series(df[c])
    for c in [
        "sent_eventual_acceptance_rate_including_pending", "sent_eventual_decision_acceptance_rate",
        "received_eventual_acceptance_rate_including_pending",
    ]:
        df[c] = nullable_float(df[c])

    df["analysis_eligible_nonstaff_flag"] = (
        df["is_staff"].fillna(0).eq(0) & df["is_superuser"].fillna(0).eq(0)
    ).astype("Int8")
    df["signup_date"] = df["signup_at"].dt.normalize()
    df["signup_year_month"] = df["signup_at"].dt.strftime("%Y-%m").astype("string")
    df["is_2023_may_signup_flag"] = df["signup_at"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")

    sent_status_sum = (
        df["sent_final_accepted_count"].fillna(0) + df["sent_final_pending_count"].fillna(0)
        + df["sent_final_rejected_count"].fillna(0)
    ).astype("Int64")
    recv_status_sum = (
        df["received_final_accepted_count"].fillna(0) + df["received_final_pending_count"].fillna(0)
        + df["received_final_rejected_count"].fillna(0)
    ).astype("Int64")
    df["sent_status_total_match_flag"] = flag_equal(df["sent_request_count"], sent_status_sum)
    df["received_status_total_match_flag"] = flag_equal(df["received_request_count"], recv_status_sum)
    df["endpoint_total_match_flag"] = flag_equal(
        df["total_request_endpoint_activity_count"],
        (df["sent_request_count"].fillna(0) + df["received_request_count"].fillna(0)).astype("Int64"),
    )

    df["sent_acceptance_rate_recalc"] = safe_rate(df["sent_final_accepted_count"], df["sent_request_count"])
    decision_den = (df["sent_final_accepted_count"].fillna(0) + df["sent_final_rejected_count"].fillna(0)).astype("Int64")
    df["sent_decision_acceptance_rate_recalc"] = safe_rate(df["sent_final_accepted_count"], decision_den)
    df["received_acceptance_rate_recalc"] = safe_rate(df["received_final_accepted_count"], df["received_request_count"])
    df["sent_acceptance_rate_match_flag"] = flag_equal(
        df["sent_eventual_acceptance_rate_including_pending"], df["sent_acceptance_rate_recalc"], 1e-9
    )
    df["sent_decision_rate_match_flag"] = flag_equal(
        df["sent_eventual_decision_acceptance_rate"], df["sent_decision_acceptance_rate_recalc"], 1e-9
    )
    df["received_acceptance_rate_match_flag"] = flag_equal(
        df["received_eventual_acceptance_rate_including_pending"], df["received_acceptance_rate_recalc"], 1e-9
    )

    for prefix in ["sent", "received"]:
        denominator = df[f"{prefix}_request_count"]
        for window in ["24h", "72h", "7d"]:
            df[f"{prefix}_first_{window}_share"] = safe_rate(df[f"{prefix}_request_first_{window}_count"], denominator)

    positions = df["user_id"].map(user_position_map)
    valid_pos = positions.notna()
    pos = positions.loc[valid_pos].astype(np.int64).to_numpy()
    for side, arrays in [("sent", sent_arrays), ("received", recv_arrays)]:
        for label, profile_col in [
            ("total", f"{side}_request_count"),
            ("A", f"{side}_final_accepted_count"),
            ("P", f"{side}_final_pending_count"),
            ("R", f"{side}_final_rejected_count"),
        ]:
            event_values = pd.Series(pd.NA, index=df.index, dtype="Int64")
            event_values.loc[valid_pos] = arrays[label][pos]
            flag_name = f"{profile_col}_matches_event_flag"
            df[flag_name] = flag_equal(df[profile_col], event_values)
            viral_agg[f"{flag_name}_mismatch"] += int(df[flag_name].fillna(0).ne(1).sum())

    viral_ids.append(df["user_id"])
    viral_agg["rows"] += len(df)
    viral_agg["sent_status_mismatch"] += int(df["sent_status_total_match_flag"].fillna(0).ne(1).sum())
    viral_agg["received_status_mismatch"] += int(df["received_status_total_match_flag"].fillna(0).ne(1).sum())
    viral_agg["endpoint_total_mismatch"] += int(df["endpoint_total_match_flag"].fillna(0).ne(1).sum())
    for c in ["sent_acceptance_rate_match_flag", "sent_decision_rate_match_flag", "received_acceptance_rate_match_flag"]:
        comparable = df[c].notna()
        viral_agg[f"{c}_mismatch"] += int(df.loc[comparable, c].ne(1).sum())
    return df


viral_output = write_processed(VIRAL_MART, transform_viral_profile)
viral_id_series = pd.concat(viral_ids, ignore_index=True)
viral_dup = int(viral_id_series.duplicated(keep=False).sum())
add_qa(VIRAL_MART, "duplicate_user_id_rows", viral_dup, 0,
       "PASS" if viral_dup == 0 else "FAIL", "CRITICAL", "사용자 프로필은 user_id 1행이어야 합니다.")
for metric, value in viral_agg.items():
    if metric.endswith("_mismatch"):
        add_qa(VIRAL_MART, metric, value, 0, "PASS" if value == 0 else "FAIL", "CRITICAL",
               "사용자 집계와 원행 또는 내부 합계·비율을 대사합니다.")

viral_derived = {
    "analysis_eligible_nonstaff_flag": "직원과 슈퍼유저가 아닌 계정이면 1.",
    "signup_date": "가입 날짜.",
    "signup_year_month": "가입 연월 YYYY-MM.",
    "is_2023_may_signup_flag": "2023년 5월 가입이면 1.",
    "sent_status_total_match_flag": "발신 A+P+R 합이 전체 발신 요청 수와 같으면 1.",
    "received_status_total_match_flag": "수신 A+P+R 합이 전체 수신 요청 수와 같으면 1.",
    "endpoint_total_match_flag": "발신+수신 요청 수가 전체 endpoint 활동 수와 같으면 1.",
    "sent_acceptance_rate_recalc": "최종 수락 발신 건수/전체 발신 건수. 가입 전환율이 아님.",
    "sent_decision_acceptance_rate_recalc": "최종 수락/(최종 수락+최종 거절). 대기 건 제외.",
    "received_acceptance_rate_recalc": "최종 수락 수신 건수/전체 수신 건수.",
    "sent_acceptance_rate_match_flag": "기존 발신 전체분모 비율과 재계산 값이 같으면 1.",
    "sent_decision_rate_match_flag": "기존 발신 결정분모 비율과 재계산 값이 같으면 1.",
    "received_acceptance_rate_match_flag": "기존 수신 비율과 재계산 값이 같으면 1.",
}
for side in ["sent", "received"]:
    for window in ["24h", "72h", "7d"]:
        viral_derived[f"{side}_first_{window}_share"] = f"전체 {side} 요청 중 가입 후 첫 {window} 이내 생성된 요청 비율."
for side in ["sent", "received"]:
    for metric in ["request_count", "final_accepted_count", "final_pending_count", "final_rejected_count"]:
        viral_derived[f"{side}_{metric}_matches_event_flag"] = f"{side} {metric}가 친구요청 원행 재집계와 같으면 1."
add_dictionary(VIRAL_MART, pd.read_csv(viral_output, nrows=0).columns, viral_derived)
display(pd.Series(viral_agg))
"""
    ),
    md(
        r"""
## 6. 학교×일 확산 패널

학교별 일별 파일은 모든 학교에 공통 달력을 붙인 패널이므로 학교 첫 가입 전에도
0행이 존재한다. 이 0을 활동 부재로 곧바로 해석하지 않도록 학교 첫 가입 전·40명
도달 전·도달 후를 구분한다. 친구요청 합계는 원행에 현재 학교를 연결해 다시 만든
값과 일별로 대사한다.
"""
    ),
    code(
        r"""
SCHOOL_MART = "mart_school_viral_daily_v2"


def combine_school_daily_parts(side):
    frames = [frame for part_side, frame in event_school_daily_parts if part_side == side]
    combined = pd.concat(frames, ignore_index=True)
    metric_cols = [c for c in combined.columns if c not in ["school_id", "activity_date"]]
    return combined.groupby(["school_id", "activity_date"], observed=True)[metric_cols].sum().reset_index()


event_school_sent = combine_school_daily_parts("sent")
event_school_received = combine_school_daily_parts("received")
event_school_daily = event_school_sent.merge(
    event_school_received, on=["school_id", "activity_date"], how="outer"
).fillna(0)
event_school_daily["school_id"] = nullable_int(event_school_daily["school_id"])
event_school_daily["activity_date"] = date_series(event_school_daily["activity_date"])
event_school_daily = event_school_daily.set_index(["school_id", "activity_date"])

school_agg = defaultdict(int)
school_keys = []
school_cumulative_parts = []


def transform_school_daily(df):
    id_cols = ["school_id"]
    date_cols = ["activity_date"]
    datetime_cols = ["current_roster_first_signup_at", "current_roster_40th_signup_at", "current_roster_last_signup_at"]
    rate_cols = [
        "sent_eventual_acceptance_rate_including_pending", "sent_eventual_decision_acceptance_rate",
        "received_eventual_acceptance_rate_including_pending",
    ]
    flag_cols = [
        "current_roster_reached_40_flag", "before_observed_40th_signup_flag",
        "on_or_after_observed_40th_signup_flag",
    ]
    skip_numeric = set(id_cols + date_cols + datetime_cols + rate_cols + flag_cols)
    numeric_cols = [c for c in df.columns if c not in skip_numeric]
    for c in id_cols + numeric_cols:
        df[c] = nullable_int(df[c])
    for c in date_cols:
        df[c] = date_series(df[c])
    for c in datetime_cols:
        df[c] = datetime_series(df[c])
    for c in rate_cols:
        df[c] = nullable_float(df[c])
    for c in flag_cols:
        df[c] = nullable_flag(df[c])

    df["is_2023_may_activity_flag"] = df["activity_date"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")
    activity_day = df["activity_date"]
    first_day = df["current_roster_first_signup_at"].dt.normalize()
    day40 = df["current_roster_40th_signup_at"].dt.normalize()
    phase = pd.Series("NO_CURRENT_ROSTER_SIGNUP", index=df.index, dtype="string")
    after_first = first_day.notna() & activity_day.ge(first_day)
    phase.loc[after_first & day40.isna()] = "NOT_OBSERVED_REACHED_40"
    phase.loc[after_first & day40.notna() & activity_day.lt(day40)] = "BEFORE_OBSERVED_40TH_DAY"
    phase.loc[after_first & day40.notna() & activity_day.ge(day40)] = "ON_OR_AFTER_OBSERVED_40TH_DAY"
    phase.loc[first_day.notna() & activity_day.lt(first_day)] = "BEFORE_FIRST_SIGNUP_DAY"
    df["school_observed_phase"] = phase
    df["school_has_started_by_day_flag"] = after_first.astype("Int8")
    df["analysis_eligible_school_day_flag"] = (
        after_first & df["current_roster_nonstaff_account_count"].fillna(0).gt(0)
    ).astype("Int8")

    internal_checks = pd.DataFrame(index=df.index)
    internal_checks["new_account"] = df["new_account_count_current_roster"].fillna(0).eq(
        df["new_nonstaff_account_count_current_roster"].fillna(0)
        + df["new_staff_or_superuser_account_count_current_roster"].fillna(0)
    )
    internal_checks["sent_status"] = df["sent_request_created_count"].fillna(0).eq(
        df["sent_created_final_accepted_count"].fillna(0)
        + df["sent_created_final_pending_count"].fillna(0)
        + df["sent_created_final_rejected_count"].fillna(0)
    )
    internal_checks["received_status"] = df["received_request_created_count"].fillna(0).eq(
        df["received_created_final_accepted_count"].fillna(0)
        + df["received_created_final_pending_count"].fillna(0)
        + df["received_created_final_rejected_count"].fillna(0)
    )
    internal_checks["sent_relation"] = df["sent_request_created_count"].fillna(0).eq(
        df["sent_within_school_count_current"].fillna(0)
        + df["sent_cross_school_count_current"].fillna(0)
        + df["sent_unknown_school_relation_count"].fillna(0)
    )
    internal_checks["received_relation"] = df["received_request_created_count"].fillna(0).eq(
        df["received_within_school_count_current"].fillna(0)
        + df["received_cross_school_count_current"].fillna(0)
        + df["received_unknown_school_relation_count"].fillna(0)
    )
    df["daily_internal_consistency_flag"] = internal_checks.all(axis=1).astype("Int8")

    keys = pd.MultiIndex.from_arrays([df["school_id"], df["activity_date"]])
    matched = event_school_daily.reindex(keys)
    matched.index = df.index
    for c in matched.columns:
        matched[c] = nullable_int(matched[c]).fillna(0)
    df["sent_event_reaggregate_count"] = matched["sent_request_count_event"].astype("Int64")
    df["received_event_reaggregate_count"] = matched["received_request_count_event"].astype("Int64")
    df["sent_event_reconcile_flag"] = flag_equal(
        df["sent_request_created_count"], df["sent_event_reaggregate_count"]
    )
    df["received_event_reconcile_flag"] = flag_equal(
        df["received_request_created_count"], df["received_event_reaggregate_count"]
    )

    school_agg["rows"] += len(df)
    school_agg["internal_mismatch"] += int(df["daily_internal_consistency_flag"].ne(1).sum())
    school_agg["sent_reconcile_mismatch"] += int(df["sent_event_reconcile_flag"].fillna(0).ne(1).sum())
    school_agg["received_reconcile_mismatch"] += int(df["received_event_reconcile_flag"].fillna(0).ne(1).sum())
    school_keys.append(df[["school_id", "activity_date"]].copy())
    school_cumulative_parts.append(df[[
        "school_id", "activity_date", "new_account_count_current_roster",
        "new_nonstaff_account_count_current_roster", "sent_request_created_count",
        "received_request_created_count", "cumulative_account_count_current_roster",
        "cumulative_nonstaff_account_count_current_roster", "cumulative_sent_request_created_count",
        "cumulative_received_request_created_count",
    ]].copy())
    return df


school_output = write_processed(SCHOOL_MART, transform_school_daily)
school_key_df = pd.concat(school_keys, ignore_index=True)
school_dup = int(school_key_df.duplicated(["school_id", "activity_date"], keep=False).sum())
add_qa(SCHOOL_MART, "duplicate_school_day_rows", school_dup, 0,
       "PASS" if school_dup == 0 else "FAIL", "CRITICAL", "학교×날짜는 1행이어야 합니다.")
for metric in ["internal_mismatch", "sent_reconcile_mismatch", "received_reconcile_mismatch"]:
    value = school_agg[metric]
    add_qa(SCHOOL_MART, metric, value, 0, "PASS" if value == 0 else "FAIL", "CRITICAL",
           "학교 일별 내부 합계 또는 친구요청 원행 재집계 대사입니다.")

cum = pd.concat(school_cumulative_parts, ignore_index=True).sort_values(["school_id", "activity_date"])
cum["account_recalc"] = cum.groupby("school_id", observed=True)["new_account_count_current_roster"].cumsum()
cum["nonstaff_recalc"] = cum.groupby("school_id", observed=True)["new_nonstaff_account_count_current_roster"].cumsum()
cum["sent_recalc"] = cum.groupby("school_id", observed=True)["sent_request_created_count"].cumsum()
cum["received_recalc"] = cum.groupby("school_id", observed=True)["received_request_created_count"].cumsum()
for source_col, recalc_col in [
    ("cumulative_account_count_current_roster", "account_recalc"),
    ("cumulative_nonstaff_account_count_current_roster", "nonstaff_recalc"),
    ("cumulative_sent_request_created_count", "sent_recalc"),
    ("cumulative_received_request_created_count", "received_recalc"),
]:
    mismatch = int(cum[source_col].fillna(-1).ne(cum[recalc_col].fillna(-1)).sum())
    add_qa(SCHOOL_MART, f"{source_col}_mismatch_rows", mismatch, 0,
           "PASS" if mismatch == 0 else "FAIL", "CRITICAL", "학교별 날짜 순 누적합을 Python에서 재계산합니다.")

school_derived = {
    "is_2023_may_activity_flag": "활동 날짜가 2023년 5월이면 1.",
    "school_observed_phase": "학교 첫 가입 전, 관측 40명 전, 관측 40명 이후, 40명 미도달을 구분.",
    "school_has_started_by_day_flag": "해당 날짜가 현재 roster 기준 학교 첫 가입일과 같거나 이후면 1.",
    "analysis_eligible_school_day_flag": "학교 첫 가입 이후이고 현재 비직원 계정이 1명 이상이면 1.",
    "daily_internal_consistency_flag": "신규계정·상태·학교관계 합계 검증을 모두 통과하면 1.",
    "sent_event_reaggregate_count": "친구요청 원행에 발신자의 현재 학교를 붙여 학교×일로 재집계한 수.",
    "received_event_reaggregate_count": "친구요청 원행에 수신자의 현재 학교를 붙여 학교×일로 재집계한 수.",
    "sent_event_reconcile_flag": "학교 일별 발신 요청 수가 원행 재집계와 같으면 1.",
    "received_event_reconcile_flag": "학교 일별 수신 요청 수가 원행 재집계와 같으면 1.",
}
add_dictionary(SCHOOL_MART, pd.read_csv(school_output, nrows=0).columns, school_derived)
display(pd.Series({
    "학교×일 행": school_agg["rows"],
    "학교 수": school_key_df["school_id"].nunique(),
    "최초 날짜": school_key_df["activity_date"].min(),
    "최종 날짜": school_key_df["activity_date"].max(),
    "내부 합계 불일치": school_agg["internal_mismatch"],
    "발신 원행 대사 불일치": school_agg["sent_reconcile_mismatch"],
    "수신 원행 대사 불일치": school_agg["received_reconcile_mismatch"],
}))
"""
    ),
    md(
        r"""
## 7. 최종 QA, 컬럼 사전, 전처리 기록

실패가 하나라도 있으면 완료로 판정하지 않는다. `WARN`은 행을 삭제하지 않고
해석 제한으로 기록한다.
"""
    ),
    code(
        r"""
# 사용자 가입 프로필과 사용자 바이럴 프로필의 공통 사용자 키 대사
viral_user_set = set(viral_id_series.dropna().astype("int64"))
acq_user_set = set(user_ids.dropna().astype("int64"))
add_qa("CROSS_MART", "acquisition_vs_viral_user_key_symmetric_difference",
       len(acq_user_set.symmetric_difference(viral_user_set)), 0,
       "PASS" if acq_user_set == viral_user_set else "FAIL", "CRITICAL",
       "두 사용자 grain 마트의 사용자 모집단이 정확히 같아야 합니다.")

# 연락처 관측 사용자 수와 사용자 프로필 플래그 대사
contact_known_users = int(contact_clean["contact_owner_known_user_flag"].sum())
profile_contacts_observed = int(pd.to_numeric(contact_profile["contacts_observed_flag"], errors="coerce").fillna(0).sum())
add_qa("CROSS_MART", "contact_known_users_vs_profile_observed",
       contact_known_users, profile_contacts_observed,
       "PASS" if contact_known_users == profile_contacts_observed else "FAIL", "HIGH",
       "연락처 원행 사용자 수와 사용자 프로필의 연락처 관측 플래그 합을 비교합니다.")

# 사용자 현재 학교와 학교×일 패널 학교의 범위를 명시적으로 대사한다.
# 학교×일은 전체 학교 기준 테이블을 기반으로 하므로 사용자 0명인 학교도 포함될 수 있다.
acquisition_school_set = set(school_map.dropna().astype("int64"))
school_daily_school_set = set(school_key_df["school_id"].dropna().astype("int64"))
common_school_set = acquisition_school_set & school_daily_school_set
acquisition_only_school_set = acquisition_school_set - school_daily_school_set
school_daily_only_school_set = school_daily_school_set - acquisition_school_set

add_qa("CROSS_MART", "acquisition_only_school_id_count",
       len(acquisition_only_school_set), 0,
       "PASS" if not acquisition_only_school_set else "WARN", "HIGH",
       "사용자 현재 학교에는 있으나 학교×일 패널에 없는 학교 수입니다. 행을 삭제하지 않고 공통 학교 분석에서 제외합니다.")
add_qa("CROSS_MART", "school_daily_only_school_id_count",
       len(school_daily_only_school_set), 0,
       "PASS" if not school_daily_only_school_set else "WARN", "HIGH",
       "학교×일 패널에는 있으나 현재 사용자가 없는 학교 수입니다. 학교 기준 테이블의 0명 학교를 보존한 결과입니다.")

scope_school_ids = sorted(acquisition_school_set | school_daily_school_set)
scope_df = pd.DataFrame({"school_id": pd.Series(scope_school_ids, dtype="Int64")})
scope_df["in_user_acquisition_current_school_flag"] = scope_df["school_id"].isin(acquisition_school_set).astype("Int8")
scope_df["in_school_daily_panel_flag"] = scope_df["school_id"].isin(school_daily_school_set).astype("Int8")
scope_df["common_school_analysis_flag"] = scope_df["school_id"].isin(common_school_set).astype("Int8")
scope_df["school_scope_note"] = np.select(
    [
        scope_df["common_school_analysis_flag"].eq(1),
        scope_df["in_user_acquisition_current_school_flag"].eq(1),
    ],
    [
        "COMMON_USER_AND_SCHOOL_DAILY",
        "USER_CURRENT_SCHOOL_NOT_IN_SCHOOL_DAILY",
    ],
    default="SCHOOL_DAILY_DIMENSION_ONLY_NO_CURRENT_USER",
)

qa = pd.DataFrame(qa_rows)
qa["status_order"] = qa["status"].map({"FAIL": 0, "WARN": 1, "PASS": 2}).fillna(9)
qa = qa.sort_values(["status_order", "severity", "mart", "test_name"]).drop(columns="status_order")
dictionary_df = pd.DataFrame(dictionary_rows).drop_duplicates(["mart", "column_name"])
processing_df = pd.DataFrame(processing_log)

qa_path = REPORT_DIR / "viral_school_qa_summary.csv"
dictionary_path = REPORT_DIR / "viral_school_column_dictionary.csv"
processing_path = REPORT_DIR / "viral_school_processing_log.csv"
scope_path = REPORT_DIR / "viral_school_scope_school_ids.csv"
qa.to_csv(qa_path, index=False, encoding="utf-8-sig")
dictionary_df.to_csv(dictionary_path, index=False, encoding="utf-8-sig")
processing_df.to_csv(processing_path, index=False, encoding="utf-8-sig")
scope_df.to_csv(scope_path, index=False, encoding="utf-8-sig")

fail_count = int(qa["status"].eq("FAIL").sum())
warn_count = int(qa["status"].eq("WARN").sum())
pass_count = int(qa["status"].eq("PASS").sum())

report_lines = [
    "# 바이럴·학교 확산 6개 마트 전처리 기록",
    "",
    "## 결과",
    "",
    f"- PASS: {pass_count}건",
    f"- WARN: {warn_count}건",
    f"- FAIL: {fail_count}건",
    f"- 원본 행 삭제: 0건",
    f"- 결측값 임의 대체: 0건",
    f"- 이상치 임의 제거: 0건",
    "",
    "## 공통 적용 사항",
    "",
    "- SQL NULL 표기 `\\N`을 결측으로 변환",
    "- ID·건수·플래그·날짜·비율 자료형 표준화",
    "- 직원·슈퍼유저 분석 대상 플래그 추가",
    "- 2023년 5월 가입·요청·학교 일자 플래그 추가",
    "- 현재 학교 문맥과 과거 이벤트 시점을 컬럼명으로 분리",
    "- 최종 친구요청 상태 A/P/R 라벨 추가",
    "- 사용자·방향쌍·학교 일별 합계를 친구요청 원행과 재대사",
    "- 학교 첫 가입 전과 관측 40명 도달 전후를 별도 단계로 구분",
    "",
    "## 모집단 통일 규칙",
    "",
    f"- 사용자 현재 학교: {len(acquisition_school_set):,}개",
    f"- 학교×일 원본 패널 학교: {len(school_daily_school_set):,}개",
    f"- 두 마트 공통 학교: {len(common_school_set):,}개",
    f"- 사용자 마트에만 있는 학교: {len(acquisition_only_school_set):,}개 ({sorted(acquisition_only_school_set)})",
    f"- 학교×일 마트에만 있는 현재 사용자 0명 학교: {len(school_daily_only_school_set):,}개",
    "- 사용자와 학교×일 결과를 직접 비교할 때는 `viral_school_scope_school_ids.csv`의 `common_school_analysis_flag=1`을 공통 필터로 사용",
    "- 사용자 수준 전체 가입 분석은 5,551개 현재 학교를 유지하고, 학교×일 확산 비교만 공통 5,550개 학교로 제한",
    "",
    "## 해석 제한",
    "",
    "- 친구요청 상태는 최종 상태이며 상태 변경 이력이 아니다.",
    "- 친구요청 수락은 신규 가입 전환이 아니다. 수신자도 이미 계정이 있다.",
    "- 현재 학교·친구·학년·반을 과거 이벤트 당시 상태로 사용하면 안 된다.",
    "- 연락처 초대자 목록은 기준시각이 없는 스냅샷이며 발송 이벤트가 아니다.",
    "- 관측된 학교 40번째 가입 시각은 실제 기능 해금 시각으로 확정되지 않았다.",
    "- 시간대 표기가 없으므로 임의 UTC/KST 보정을 하지 않았다.",
    "",
    "## 생성 파일",
    "",
]
for row in processing_log:
    report_lines.append(f"- `{Path(row['output_path']).name}`: {row['output_rows']:,}행, {row['output_columns']}열")
report_lines.extend([
    "",
    "## QA 실패 항목",
    "",
])
failed = qa.loc[qa["status"].eq("FAIL")]
if failed.empty:
    report_lines.append("- 없음")
else:
    for row in failed.itertuples(index=False):
        report_lines.append(f"- `{row.mart}` / `{row.test_name}`: actual={row.actual}, expected={row.expected}")

report_lines.extend([
    "",
    "## QA 경고 항목",
    "",
])
warned = qa.loc[qa["status"].eq("WARN")]
if warned.empty:
    report_lines.append("- 없음")
else:
    for row in warned.itertuples(index=False):
        report_lines.append(
            f"- `{row.mart}` / `{row.test_name}`: actual={row.actual}, expected={row.expected} — {row.explanation}"
        )

report_path = REPORT_DIR / "viral_school_preprocessing_report.md"
report_path.write_text("\n".join(report_lines), encoding="utf-8")

display(Markdown(f"### QA 결과: PASS {pass_count} / WARN {warn_count} / FAIL {fail_count}"))
display(qa.reset_index(drop=True))
display(processing_df)
print(f"QA 요약: {qa_path}")
print(f"컬럼 사전: {dictionary_path}")
print(f"처리 기록: {processing_path}")
print(f"공통 학교 범위표: {scope_path}")
print(f"전처리 보고서: {report_path}")
display(scope_df.groupby("school_scope_note", dropna=False).size().rename("school_count"))

if fail_count:
    raise AssertionError(f"최종 QA FAIL이 {fail_count}건 있습니다. 결과를 완료본으로 사용하지 마세요.")
print("바이럴·학교 확산 6개 마트 전처리와 최종 QA가 완료되었습니다.")
"""
    ),
]

nb = nbf.v4.new_notebook(
    cells=cells,
    metadata={
        "kernelspec": {
            "display_name": "Python 3 (.venv)",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3"},
    },
)

NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, NOTEBOOK_PATH)
print(NOTEBOOK_PATH)
