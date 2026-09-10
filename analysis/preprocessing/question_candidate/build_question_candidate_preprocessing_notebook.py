from pathlib import Path

import nbformat as nbf


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "question_candidate_4marts_preprocessing.ipynb"


def md(text: str):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text: str):
    return nbf.v4.new_code_cell(text.strip())


cells = [
    md(
        r"""
# 질문·후보 관계 4개 마트 전처리

질문세트, 질문조각, 투표·Ping 현재상태, 후보 원행을 같은 기준으로 연결한다.
원본 행과 원본 컬럼은 삭제하지 않으며, 세트 JSON에서 질문 위치를 복원해
`세트 주인 → 질문조각 → 후보 → 선택대상`을 대사한다.

## 처리 대상

| 마트 | 한 행의 의미 | 기본 키 |
|---|---|---|
| `mart_question_set_record_v2` | 질문세트 1개 | `question_set_id` |
| `mart_question_piece_record_v2` | 질문조각 1개 | `question_piece_id` |
| `mart_vote_record_v2` | 투표·Ping 현재상태 레코드 1개 | `user_question_record_id` |
| `mart_question_candidate_exposure_v2` | 후보 원초 행 1개 | `candidate_exposure_id` |

## 원칙

1. 원본 행을 삭제하거나 결측을 임의로 채우지 않는다.
2. 상태 코드는 의미를 추정해 바꾸지 않고 `CURRENT_STATUS_*` 라벨만 추가한다.
3. 현재 친구·학교·학년·반은 질문 당시 관계가 아닌 현재 스냅샷으로만 사용한다.
4. 후보 원초 중복은 보존하고 비율 계산용 canonical 행을 별도로 표시한다.
5. 세트에 참조됐지만 현재 조각 원천에 없는 ID도 세트별 누락 수로 보존한다.
6. F를 완주, opening_time을 실제 노출, 후보 생성시각을 실제 노출로 해석하지 않는다.
"""
    ),
    code(
        r"""
from __future__ import annotations

import gc
import gzip
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import display, Markdown

pd.set_option("display.max_columns", 160)
pd.set_option("display.max_rows", 140)
pd.set_option("display.width", 240)

def find_repo_root(start: Path | None = None) -> Path:
    start = (start or Path.cwd()).resolve()
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    raise FileNotFoundError("Git 저장소 루트를 찾지 못했습니다.")


REPO_ROOT = find_repo_root()
SOURCE_DIR = REPO_ROOT / "data" / "marts" / "23_marts_csv"
REFERENCE_USER_PATH = REPO_ROOT / "data" / "processed" / "viral_school" / "mart_user_acquisition_profile_v2_clean.csv.gz"
OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "question_candidate"
REPORT_DIR = REPO_ROOT / "analysis" / "preprocessing" / "question_candidate" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 250_000
NA_VALUES = [r"\N"]
SOURCE_NAMES = [
    "mart_question_set_record_v2",
    "mart_question_piece_record_v2",
    "mart_vote_record_v2",
    "mart_question_candidate_exposure_v2",
]

if not REFERENCE_USER_PATH.exists():
    raise FileNotFoundError("바이럴·학교 확산 축의 정제 사용자 기준정보가 필요합니다.")

manifest = pd.read_csv(SOURCE_DIR / "export_manifest.csv", dtype="string")
source_manifest = manifest.loc[manifest["object_name"].isin(SOURCE_NAMES)].copy()
source_manifest["exported_rows"] = pd.to_numeric(source_manifest["exported_rows"], errors="coerce").astype("Int64")
source_manifest["column_count"] = pd.to_numeric(source_manifest["column_count"], errors="coerce").astype("Int64")
expected_rows = dict(zip(source_manifest["object_name"], source_manifest["exported_rows"].astype(int)))

if set(expected_rows) != set(SOURCE_NAMES):
    raise AssertionError("질문·후보 4개 마트가 export_manifest에 모두 존재하지 않습니다.")

print(f"원본 폴더: {SOURCE_DIR}")
print(f"정제 결과 폴더: {OUTPUT_DIR}")
display(source_manifest[["object_name", "exported_rows", "column_count", "status", "sha256"]].reset_index(drop=True))
"""
    ),
    md(
        r"""
## 공통 함수와 QA 규칙

원본 네 마트는 메모리 범위 안에서 타입을 통일하고, 가장 큰 후보 마트의 출력은
25만 행씩 나누어 작성한다. 모든 파생 플래그는 0/1/결측을 구분한다.
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
    return pd.to_datetime(series, format="mixed", errors="coerce")


def make_nullable_flag(condition, valid_mask=None):
    result = pd.Series(pd.NA, index=condition.index, dtype="Int8")
    if valid_mask is None:
        valid_mask = pd.Series(True, index=condition.index)
    result.loc[valid_mask] = condition.loc[valid_mask].astype("int8")
    return result


def read_source(name, usecols=None):
    return pd.read_csv(
        SOURCE_DIR / f"{name}.csv.gz",
        compression="gzip",
        encoding="utf-8-sig",
        na_values=NA_VALUES,
        keep_default_na=False,
        dtype="string",
        usecols=usecols,
        low_memory=False,
    )


def source_chunks(name):
    return pd.read_csv(
        SOURCE_DIR / f"{name}.csv.gz",
        compression="gzip",
        encoding="utf-8-sig",
        na_values=NA_VALUES,
        keep_default_na=False,
        dtype="string",
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )


def write_full(name, frame):
    output_path = OUTPUT_DIR / f"{name}_clean.csv.gz"
    frame.to_csv(
        output_path,
        index=False,
        compression={"method": "gzip", "compresslevel": 3},
        encoding="utf-8-sig",
        na_rep="",
        date_format="%Y-%m-%d %H:%M:%S.%f",
    )
    processing_log.append({
        "mart": name,
        "source_path": str(SOURCE_DIR / f"{name}.csv.gz"),
        "output_path": str(output_path),
        "source_rows": expected_rows[name],
        "output_rows": len(frame),
        "row_preserved": len(frame) == expected_rows[name],
        "source_columns": int(source_manifest.loc[source_manifest["object_name"].eq(name), "column_count"].iloc[0]),
        "output_columns": len(frame.columns),
    })
    add_qa(name, "row_count_preserved", len(frame), expected_rows[name],
           "PASS" if len(frame) == expected_rows[name] else "FAIL", "CRITICAL",
           "전처리 전후 행 수가 같아야 합니다.")
    print(f"완료: {name} → {len(frame):,}행, {len(frame.columns):,}열", flush=True)
    return output_path


def add_dictionary(mart, columns, derived_descriptions):
    for col in columns:
        dictionary_rows.append({
            "mart": mart,
            "column_name": col,
            "column_origin": "DERIVED" if col in derived_descriptions else "SOURCE_PRESERVED",
            "description": derived_descriptions.get(col, "원본 v2 마트 컬럼. 원래 이름과 의미를 보존함."),
        })


def parse_json_array(value):
    if pd.isna(value):
        return 0, 0, None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0, 0, None
    if not isinstance(parsed, list):
        return 1, 0, None
    cleaned = []
    for item in parsed:
        if item is None:
            cleaned.append(None)
            continue
        try:
            cleaned.append(int(item))
        except (TypeError, ValueError):
            cleaned.append(None)
    return 1, 1, cleaned


def map_nullable_flag(series, mapping):
    return nullable_flag(series.map(mapping))


print("공통 함수 준비 완료")
"""
    ),
    md(
        r"""
## 기준 사용자와 세 개의 소형 원장 로드

이전 축에서 검증한 사용자 기준정보는 현재 친구·학교·학년·반을 연결하는 조회용으로만
사용한다. 질문 마트의 행 수나 모집단을 사용자 마트로 제한하지 않는다.
"""
    ),
    code(
        r"""
USER_COLS = [
    "user_id", "is_staff", "is_superuser", "current_group_id", "current_school_id",
    "current_grade", "current_class_num", "current_friend_id_list_json",
    "current_friend_json_valid", "analysis_eligible_nonstaff_flag",
]
user_profile = pd.read_csv(
    REFERENCE_USER_PATH,
    compression="gzip",
    encoding="utf-8-sig",
    na_values=NA_VALUES,
    keep_default_na=False,
    dtype="string",
    usecols=USER_COLS,
    low_memory=False,
)
for c in ["user_id", "current_group_id", "current_school_id", "current_grade", "current_class_num"]:
    user_profile[c] = nullable_int(user_profile[c])
for c in ["is_staff", "is_superuser", "current_friend_json_valid", "analysis_eligible_nonstaff_flag"]:
    user_profile[c] = nullable_flag(user_profile[c])
if user_profile["user_id"].duplicated().any():
    raise AssertionError("기준 사용자 프로필의 user_id가 중복됩니다.")
user_idx = user_profile.set_index("user_id", drop=False)
known_user_ids = set(user_profile["user_id"].dropna().astype("int64"))

SET_MART = "mart_question_set_record_v2"
PIECE_MART = "mart_question_piece_record_v2"
VOTE_MART = "mart_vote_record_v2"
CANDIDATE_MART = "mart_question_candidate_exposure_v2"

qset = read_source(SET_MART)
piece = read_source(PIECE_MART)
vote = read_source(VOTE_MART)

for c in ["question_set_id", "question_set_owner_user_id", "piece_list_length", "opening_delay_seconds"]:
    qset[c] = nullable_int(qset[c])
for c in ["piece_list_json_valid_flag", "piece_list_array_valid_flag", "orphan_owner_user_flag"]:
    qset[c] = nullable_flag(qset[c])
for c in ["question_set_created_at", "question_set_opening_time"]:
    qset[c] = datetime_series(qset[c])

for c in ["question_piece_id", "question_id"]:
    piece[c] = nullable_int(piece[c])
for c in ["raw_is_voted", "raw_is_skipped", "orphan_question_flag", "voted_and_skipped_flag"]:
    piece[c] = nullable_flag(piece[c])
piece["question_piece_created_at"] = datetime_series(piece["question_piece_created_at"])

for c in [
    "user_question_record_id", "voter_user_id", "chosen_user_id", "question_id",
    "question_piece_id", "ping_report_count_current", "ping_opened_times_current",
]:
    vote[c] = nullable_int(vote[c])
for c in [
    "ping_has_read_current", "orphan_voter_flag", "orphan_chosen_user_flag",
    "orphan_question_piece_flag", "orphan_question_flag", "piece_question_mismatch_flag",
]:
    vote[c] = nullable_flag(vote[c])
for c in ["vote_record_created_at", "ping_answer_status_updated_at"]:
    vote[c] = datetime_series(vote[c])

for name, frame, key in [
    (SET_MART, qset, "question_set_id"),
    (PIECE_MART, piece, "question_piece_id"),
    (VOTE_MART, vote, "user_question_record_id"),
]:
    duplicate_rows = int(frame[key].duplicated(keep=False).sum())
    null_rows = int(frame[key].isna().sum())
    add_qa(name, "duplicate_primary_key_rows", duplicate_rows, 0,
           "PASS" if duplicate_rows == 0 else "FAIL", "CRITICAL", f"{key}는 고유해야 합니다.")
    add_qa(name, "null_primary_key_rows", null_rows, 0,
           "PASS" if null_rows == 0 else "FAIL", "CRITICAL", f"{key}는 결측이면 안 됩니다.")

vote_piece_dup = int(vote["question_piece_id"].duplicated(keep=False).sum())
add_qa(VOTE_MART, "duplicate_question_piece_vote_rows", vote_piece_dup, 0,
       "PASS" if vote_piece_dup == 0 else "FAIL", "CRITICAL",
       "현재 원천에서는 질문조각당 투표·Ping 레코드가 최대 1건이어야 합니다.")

display(pd.DataFrame({
    "mart": [SET_MART, PIECE_MART, VOTE_MART],
    "rows": [len(qset), len(piece), len(vote)],
    "columns_before": [11, 7, 17],
}))
"""
    ),
    md(
        r"""
## 질문세트 JSON 전개와 질문자(owner) 해석

세트의 질문조각 목록을 위치와 함께 펼친 뒤 현재 조각 원장·투표 원장과 대사한다.
질문자 해석은 SQL 생성 규칙과 같은 0~5 코드를 사용한다.
"""
    ),
    code(
        r"""
parsed_json = qset["question_piece_id_list_json"].map(parse_json_array)
qset["piece_list_json_valid_recalc_flag"] = pd.Series([x[0] for x in parsed_json], dtype="Int8")
qset["piece_list_array_valid_recalc_flag"] = pd.Series([x[1] for x in parsed_json], dtype="Int8")
qset["_piece_id_list_parsed"] = [x[2] for x in parsed_json]
qset["piece_list_length_recalc"] = pd.Series(
    [len(x) if isinstance(x, list) else pd.NA for x in qset["_piece_id_list_parsed"]], dtype="Int64"
)
qset["piece_list_json_flag_match"] = (
    qset["piece_list_json_valid_flag"].eq(qset["piece_list_json_valid_recalc_flag"])
).astype("Int8")
qset["piece_list_array_flag_match"] = (
    qset["piece_list_array_valid_flag"].eq(qset["piece_list_array_valid_recalc_flag"])
).astype("Int8")
qset["piece_list_length_match_flag"] = (
    qset["piece_list_length"].eq(qset["piece_list_length_recalc"])
).astype("Int8")

bridge = qset[["question_set_id", "question_set_owner_user_id", "_piece_id_list_parsed"]].explode(
    "_piece_id_list_parsed", ignore_index=True
)
bridge = bridge.loc[bridge["_piece_id_list_parsed"].notna()].rename(
    columns={"_piece_id_list_parsed": "question_piece_id"}
)
bridge["question_piece_id"] = nullable_int(bridge["question_piece_id"])
bridge["question_position"] = (
    bridge.groupby("question_set_id", observed=True).cumcount() + 1
).astype("Int64")

piece_id_set = set(piece["question_piece_id"].dropna().astype("int64"))
bridge["question_piece_exists_flag"] = bridge["question_piece_id"].isin(piece_id_set).astype("Int8")

set_ref = bridge.groupby("question_set_id", observed=True).agg(
    expanded_position_count=("question_piece_id", "size"),
    referenced_piece_existing_count=("question_piece_exists_flag", "sum"),
    distinct_piece_id_count=("question_piece_id", "nunique"),
).reset_index()
set_ref["referenced_piece_missing_count"] = (
    set_ref["expanded_position_count"] - set_ref["referenced_piece_existing_count"]
).astype("Int64")
set_ref["duplicate_piece_within_set_flag"] = (
    set_ref["distinct_piece_id_count"].lt(set_ref["expanded_position_count"])
).astype("Int8")
qset = qset.merge(set_ref, on="question_set_id", how="left", validate="one_to_one")
for c in ["expanded_position_count", "referenced_piece_existing_count", "distinct_piece_id_count", "referenced_piece_missing_count"]:
    qset[c] = nullable_int(qset[c]).fillna(0)
qset["duplicate_piece_within_set_flag"] = nullable_flag(qset["duplicate_piece_within_set_flag"]).fillna(0)

qset["question_set_status_valid_flag"] = qset["question_set_status"].isin(["F", "O", "C"]).astype("Int8")
qset["question_set_status_label"] = qset["question_set_status"].map({
    "F": "CURRENT_STATUS_F", "O": "CURRENT_STATUS_O", "C": "CURRENT_STATUS_C"
}).astype("string")
qset["question_set_created_date"] = qset["question_set_created_at"].dt.normalize()
qset["question_set_created_year_month"] = qset["question_set_created_at"].dt.to_period("M").astype("string")
qset["is_2023_may_question_set_flag"] = qset["question_set_created_at"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")
qset["opening_delay_seconds_recalc"] = (
    qset["question_set_opening_time"] - qset["question_set_created_at"]
).dt.total_seconds().round().astype("Int64")
qset["opening_delay_match_flag"] = qset["opening_delay_seconds"].eq(qset["opening_delay_seconds_recalc"]).astype("Int8")
delay = qset["opening_delay_seconds_recalc"]
qset["opening_delay_band"] = np.select(
    [delay.lt(0), delay.between(0, 5), delay.between(2395, 2405), delay.between(2995, 3005), delay.gt(5)],
    ["NEGATIVE", "ZERO_TO_5_SECONDS", "AROUND_40_MINUTES", "AROUND_50_MINUTES", "OTHER_POSITIVE"],
    default="MISSING",
)
qset["owner_profile_match_flag"] = qset["question_set_owner_user_id"].isin(known_user_ids).astype("Int8")
qset["owner_analysis_eligible_nonstaff_flag"] = map_nullable_flag(
    qset["question_set_owner_user_id"], user_idx["analysis_eligible_nonstaff_flag"]
)
qset["owner_current_school_id"] = nullable_int(qset["question_set_owner_user_id"].map(user_idx["current_school_id"]))
qset["complete_10_piece_reference_flag"] = (
    qset["piece_list_array_valid_recalc_flag"].eq(1)
    & qset["piece_list_length_recalc"].eq(10)
    & qset["referenced_piece_missing_count"].eq(0)
    & qset["distinct_piece_id_count"].eq(10)
).astype("Int8")
qset["analysis_eligible_question_set_flag"] = (
    qset["question_set_id"].notna()
    & qset["owner_profile_match_flag"].eq(1)
    & qset["question_set_status_valid_flag"].eq(1)
    & qset["question_set_created_at"].notna()
    & qset["question_set_opening_time"].notna()
    & qset["piece_list_array_valid_recalc_flag"].eq(1)
).astype("Int8")

# 질문조각별 세트 소속 요약
qroll = bridge.groupby("question_piece_id", observed=True).agg(
    question_set_membership_row_count=("question_set_id", "size"),
    question_set_id_count=("question_set_id", "nunique"),
    question_set_owner_user_count=("question_set_owner_user_id", "nunique"),
    question_set_id_min=("question_set_id", "min"),
    question_set_id_max=("question_set_id", "max"),
).reset_index()
single_membership = bridge.loc[~bridge["question_piece_id"].duplicated(keep=False), [
    "question_piece_id", "question_set_id", "question_position"
]].rename(columns={
    "question_set_id": "single_question_set_id",
    "question_position": "single_question_position",
})
owner_unique = bridge.groupby("question_piece_id", observed=True)["question_set_owner_user_id"].agg(
    lambda s: s.dropna().iloc[0] if s.dropna().nunique() == 1 else pd.NA
).rename("unambiguous_qset_owner_user_id").reset_index()
qroll = qroll.merge(single_membership, on="question_piece_id", how="left", validate="one_to_one")
qroll = qroll.merge(owner_unique, on="question_piece_id", how="left", validate="one_to_one")

vote_piece_cols = [
    "question_piece_id", "user_question_record_id", "voter_user_id", "chosen_user_id",
    "question_id", "vote_record_created_at",
]
vote_by_piece = vote[vote_piece_cols].rename(columns={
    "voter_user_id": "vote_actor_user_id",
    "question_id": "vote_question_id",
})
owner_info = piece[["question_piece_id"]].merge(qroll, on="question_piece_id", how="left", validate="one_to_one")
owner_info = owner_info.merge(vote_by_piece, on="question_piece_id", how="left", validate="one_to_one")
for c in ["question_set_membership_row_count", "question_set_id_count", "question_set_owner_user_count"]:
    owner_info[c] = nullable_int(owner_info[c]).fillna(0)
for c in [
    "question_set_id_min", "question_set_id_max", "single_question_set_id", "single_question_position",
    "unambiguous_qset_owner_user_id", "user_question_record_id", "vote_actor_user_id", "chosen_user_id", "vote_question_id",
]:
    owner_info[c] = nullable_int(owner_info[c])
owner_info["question_set_membership_observed_flag"] = owner_info["question_set_membership_row_count"].gt(0).astype("Int8")
owner_info["ambiguous_question_set_mapping_flag"] = owner_info["question_set_membership_row_count"].gt(1).astype("Int8")
owner_info["vote_record_exists_flag"] = owner_info["user_question_record_id"].notna().astype("Int8")

qset_owner = owner_info["unambiguous_qset_owner_user_id"]
vote_actor = owner_info["vote_actor_user_id"]
owner_count = owner_info["question_set_owner_user_count"]
conditions = [
    owner_count.eq(1) & vote_actor.notna() & qset_owner.eq(vote_actor),
    owner_count.eq(1) & vote_actor.isna(),
    owner_count.eq(0) & vote_actor.notna(),
    owner_count.eq(1) & vote_actor.notna() & qset_owner.ne(vote_actor),
    owner_count.gt(1),
]
owner_info["resolved_owner_user_id"] = pd.Series(pd.NA, index=owner_info.index, dtype="Int64")
owner_info.loc[conditions[0] | conditions[1], "resolved_owner_user_id"] = qset_owner.loc[conditions[0] | conditions[1]]
owner_info.loc[conditions[2], "resolved_owner_user_id"] = vote_actor.loc[conditions[2]]
owner_info["owner_resolution_code"] = np.select(conditions, [1, 2, 3, 4, 5], default=0).astype("int8")
resolution_labels = {
    0: "OWNER_NOT_RESOLVED", 1: "QSET_OWNER_EQUALS_VOTER", 2: "QSET_OWNER_ONLY",
    3: "VOTER_FALLBACK", 4: "QSET_OWNER_VOTER_CONFLICT", 5: "MULTIPLE_QSET_OWNERS",
}
owner_info["owner_resolution_label"] = pd.Series(owner_info["owner_resolution_code"]).map(resolution_labels).astype("string")
owner_info["resolved_owner_profile_match_flag"] = owner_info["resolved_owner_user_id"].isin(known_user_ids).astype("Int8")
owner_info["resolved_owner_analysis_eligible_nonstaff_flag"] = map_nullable_flag(
    owner_info["resolved_owner_user_id"], user_idx["analysis_eligible_nonstaff_flag"]
)
owner_info["resolved_owner_current_school_id"] = nullable_int(
    owner_info["resolved_owner_user_id"].map(user_idx["current_school_id"])
)

expected_positions = int(qset["piece_list_length_recalc"].fillna(0).sum())
missing_positions = int(bridge["question_piece_exists_flag"].eq(0).sum())
add_qa(SET_MART, "expanded_position_count_matches_json_lengths", len(bridge), expected_positions,
       "PASS" if len(bridge) == expected_positions else "FAIL", "CRITICAL",
       "세트 JSON 배열 길이 합과 펼친 위치 행 수가 같아야 합니다.")
add_qa("CROSS_MART", "referenced_piece_missing_from_current_piece_source", missing_positions, 0,
       "WARN" if missing_positions else "PASS", "HIGH",
       "세트가 참조하지만 현재 질문조각 원장에 없는 위치입니다. 삭제하지 않고 세트별 누락 수로 보존합니다.")

qset_output_columns = [c for c in qset.columns if c != "_piece_id_list_parsed"]
qset_output = qset[qset_output_columns].copy()
SET_DERIVED = {
    "piece_list_json_valid_recalc_flag": "질문조각 목록 JSON을 다시 검사한 결과.",
    "piece_list_array_valid_recalc_flag": "유효 JSON 배열이면 1.",
    "piece_list_length_recalc": "JSON 배열 길이 재계산값.",
    "piece_list_json_flag_match": "원천 JSON 유효 플래그와 재계산값 일치 여부.",
    "piece_list_array_flag_match": "원천 배열 플래그와 재계산값 일치 여부.",
    "piece_list_length_match_flag": "원천 배열 길이와 재계산값 일치 여부.",
    "expanded_position_count": "세트 JSON을 위치별로 펼친 행 수.",
    "referenced_piece_existing_count": "현재 질문조각 원장에 존재하는 참조 수.",
    "referenced_piece_missing_count": "현재 질문조각 원장에 없는 참조 수.",
    "distinct_piece_id_count": "세트 내부 고유 질문조각 ID 수.",
    "duplicate_piece_within_set_flag": "같은 세트 안에 동일 질문조각 ID가 반복되면 1.",
    "question_set_status_valid_flag": "현재 상태 코드가 F/O/C이면 1.",
    "question_set_status_label": "의미 추정 없이 현재 상태 코드를 표시한 라벨.",
    "question_set_created_date": "질문세트 생성 날짜.",
    "question_set_created_year_month": "질문세트 생성 연월.",
    "is_2023_may_question_set_flag": "2023년 5월 생성 질문세트이면 1.",
    "opening_delay_seconds_recalc": "opening_time-created_at 초 재계산값.",
    "opening_delay_match_flag": "원천 opening delay와 재계산값 일치 여부.",
    "opening_delay_band": "생성-공개예정 간격 구간.",
    "owner_profile_match_flag": "세트 owner가 사용자 기준정보에 존재하면 1.",
    "owner_analysis_eligible_nonstaff_flag": "세트 owner가 비직원·비슈퍼 사용자이면 1.",
    "owner_current_school_id": "세트 owner의 현재 학교 ID.",
    "complete_10_piece_reference_flag": "10개 고유 조각이 모두 현재 원장에 존재하면 1.",
    "analysis_eligible_question_set_flag": "세트 메타데이터 분석 기본 조건을 충족하면 1.",
}
write_full(SET_MART, qset_output)
add_dictionary(SET_MART, qset_output.columns, SET_DERIVED)
display(qset_output[[
    "question_set_status", "piece_list_length_recalc", "referenced_piece_missing_count",
    "opening_delay_band", "analysis_eligible_question_set_flag",
]].describe(include="all"))
"""
    ),
    md(
        r"""
## 후보 원행 1차 검사와 현재 관계 기준표

후보 원초 476만 행은 삭제하지 않는다. 동일 질문조각·동일 후보의 반복 원행을
검증하고, 질문자와 후보의 고유 쌍에 대해서만 현재 친구·학교·학년·반 관계를 계산한다.
"""
    ),
    code(
        r"""
candidate_core = read_source(CANDIDATE_MART)
for c in [
    "candidate_exposure_id", "question_piece_id", "candidate_user_id",
    "source_candidate_pair_row_count", "candidate_pair_occurrence_number",
]:
    candidate_core[c] = nullable_int(candidate_core[c])
for c in ["duplicate_candidate_pair_flag", "canonical_candidate_pair_row_flag", "orphan_question_piece_flag"]:
    candidate_core[c] = nullable_flag(candidate_core[c])
candidate_core["candidate_source_created_at"] = datetime_series(candidate_core["candidate_source_created_at"])

candidate_pk_dup = int(candidate_core["candidate_exposure_id"].duplicated(keep=False).sum())
candidate_pk_null = int(candidate_core["candidate_exposure_id"].isna().sum())
add_qa(CANDIDATE_MART, "duplicate_primary_key_rows", candidate_pk_dup, 0,
       "PASS" if candidate_pk_dup == 0 else "FAIL", "CRITICAL", "candidate_exposure_id는 고유해야 합니다.")
add_qa(CANDIDATE_MART, "null_primary_key_rows", candidate_pk_null, 0,
       "PASS" if candidate_pk_null == 0 else "FAIL", "CRITICAL", "candidate_exposure_id는 결측이면 안 됩니다.")

dup_mask = candidate_core.duplicated(["question_piece_id", "candidate_user_id"], keep=False)
nondup = candidate_core.loc[~dup_mask]
nondup_mismatch = int((
    nondup["source_candidate_pair_row_count"].ne(1)
    | nondup["candidate_pair_occurrence_number"].ne(1)
    | nondup["duplicate_candidate_pair_flag"].ne(0)
    | nondup["canonical_candidate_pair_row_flag"].ne(1)
).sum())
dup_check = candidate_core.loc[dup_mask].groupby(
    ["question_piece_id", "candidate_user_id"], observed=True
).agg(
    actual_rows=("candidate_exposure_id", "size"),
    source_count_min=("source_candidate_pair_row_count", "min"),
    source_count_max=("source_candidate_pair_row_count", "max"),
    occurrence_min=("candidate_pair_occurrence_number", "min"),
    occurrence_max=("candidate_pair_occurrence_number", "max"),
    occurrence_nunique=("candidate_pair_occurrence_number", "nunique"),
    duplicate_flag_min=("duplicate_candidate_pair_flag", "min"),
    duplicate_flag_max=("duplicate_candidate_pair_flag", "max"),
    canonical_sum=("canonical_candidate_pair_row_flag", "sum"),
).reset_index()
dup_group_mismatch = int((
    dup_check["actual_rows"].ne(dup_check["source_count_min"])
    | dup_check["actual_rows"].ne(dup_check["source_count_max"])
    | dup_check["occurrence_min"].ne(1)
    | dup_check["occurrence_max"].ne(dup_check["actual_rows"])
    | dup_check["occurrence_nunique"].ne(dup_check["actual_rows"])
    | dup_check["duplicate_flag_min"].ne(1)
    | dup_check["duplicate_flag_max"].ne(1)
    | dup_check["canonical_sum"].ne(1)
).sum())
add_qa(CANDIDATE_MART, "nonduplicate_pair_flag_mismatch_rows", nondup_mismatch, 0,
       "PASS" if nondup_mismatch == 0 else "FAIL", "CRITICAL", "비중복 후보쌍 플래그를 검증합니다.")
add_qa(CANDIDATE_MART, "duplicate_pair_group_mismatches", dup_group_mismatch, 0,
       "PASS" if dup_group_mismatch == 0 else "FAIL", "CRITICAL", "중복 후보쌍의 건수·순번·canonical을 검증합니다.")

piece_candidate_counts = candidate_core.groupby("question_piece_id", observed=True).agg(
    candidate_count_raw_per_piece=("candidate_exposure_id", "size"),
    candidate_count_canonical_per_piece=("canonical_candidate_pair_row_flag", "sum"),
    candidate_distinct_user_count_per_piece=("candidate_user_id", "nunique"),
).astype("Int64")

owner_lookup = owner_info.set_index("question_piece_id")
resolved_owner_for_candidate = nullable_int(candidate_core["question_piece_id"].map(owner_lookup["resolved_owner_user_id"]))
pair_seed = pd.DataFrame({
    "owner_user_id": resolved_owner_for_candidate,
    "candidate_user_id": candidate_core["candidate_user_id"],
}).dropna().drop_duplicates().reset_index(drop=True)
pair_seed["owner_user_id"] = nullable_int(pair_seed["owner_user_id"])
pair_seed["candidate_user_id"] = nullable_int(pair_seed["candidate_user_id"])

needed_user_ids = set(pair_seed["owner_user_id"].astype("int64")) | set(pair_seed["candidate_user_id"].astype("int64"))
relation_users = user_profile.loc[user_profile["user_id"].isin(needed_user_ids)].copy().set_index("user_id", drop=False)

friend_sets = {}
friend_observed = {}
for row in relation_users[["user_id", "current_friend_id_list_json", "current_friend_json_valid"]].itertuples(index=False):
    uid = int(row.user_id)
    if row.current_friend_json_valid != 1 or pd.isna(row.current_friend_id_list_json):
        friend_sets[uid] = set()
        friend_observed[uid] = 0
        continue
    try:
        parsed = json.loads(row.current_friend_id_list_json)
        friend_sets[uid] = {int(x) for x in parsed if x is not None}
        friend_observed[uid] = 1
    except (TypeError, ValueError, json.JSONDecodeError):
        friend_sets[uid] = set()
        friend_observed[uid] = 0

relation = pair_seed.copy()
for role, id_col in [("owner", "owner_user_id"), ("candidate", "candidate_user_id")]:
    relation[f"{role}_profile_match_flag"] = relation[id_col].isin(known_user_ids).astype("Int8")
    relation[f"{role}_analysis_eligible_nonstaff_flag"] = map_nullable_flag(
        relation[id_col], user_idx["analysis_eligible_nonstaff_flag"]
    )
    for source_col, suffix in [
        ("current_group_id", "current_group_id"), ("current_school_id", "current_school_id"),
        ("current_grade", "current_grade"), ("current_class_num", "current_class_num"),
    ]:
        relation[f"{role}_{suffix}"] = nullable_int(relation[id_col].map(user_idx[source_col]))

owner_vals = relation["owner_user_id"].astype("int64").to_numpy()
candidate_vals = relation["candidate_user_id"].astype("int64").to_numpy()
owner_friend_flag = []
candidate_friend_flag = []
for owner_id, candidate_id in zip(owner_vals, candidate_vals):
    owner_friend_flag.append(int(candidate_id in friend_sets.get(owner_id, set())) if friend_observed.get(owner_id, 0) else pd.NA)
    candidate_friend_flag.append(int(owner_id in friend_sets.get(candidate_id, set())) if friend_observed.get(candidate_id, 0) else pd.NA)
relation["owner_lists_candidate_current_friend_flag"] = pd.Series(owner_friend_flag, dtype="Int8")
relation["candidate_lists_owner_current_friend_flag"] = pd.Series(candidate_friend_flag, dtype="Int8")
friend_any_valid = relation[
    ["owner_lists_candidate_current_friend_flag", "candidate_lists_owner_current_friend_flag"]
].notna().any(axis=1)
relation["current_friend_any_direction_flag"] = make_nullable_flag(
    relation["owner_lists_candidate_current_friend_flag"].fillna(0).eq(1)
    | relation["candidate_lists_owner_current_friend_flag"].fillna(0).eq(1),
    friend_any_valid,
)
friend_mutual_valid = relation[
    ["owner_lists_candidate_current_friend_flag", "candidate_lists_owner_current_friend_flag"]
].notna().all(axis=1)
relation["current_friend_mutual_flag"] = make_nullable_flag(
    relation["owner_lists_candidate_current_friend_flag"].eq(1)
    & relation["candidate_lists_owner_current_friend_flag"].eq(1),
    friend_mutual_valid,
)

def equality_flag(cols):
    valid = relation[cols].notna().all(axis=1)
    first = relation[cols[0]]
    equal = pd.Series(True, index=relation.index)
    for c in cols[1:]:
        equal &= first.eq(relation[c]) if c.endswith(cols[0].split("_")[-1]) else True
    return valid, equal

school_valid = relation[["owner_current_school_id", "candidate_current_school_id"]].notna().all(axis=1)
relation["same_school_current_flag"] = make_nullable_flag(
    relation["owner_current_school_id"].eq(relation["candidate_current_school_id"]), school_valid
)
grade_valid = school_valid & relation[["owner_current_grade", "candidate_current_grade"]].notna().all(axis=1)
relation["same_school_grade_current_flag"] = make_nullable_flag(
    relation["owner_current_school_id"].eq(relation["candidate_current_school_id"])
    & relation["owner_current_grade"].eq(relation["candidate_current_grade"]), grade_valid
)
class_valid = grade_valid & relation[["owner_current_class_num", "candidate_current_class_num"]].notna().all(axis=1)
relation["same_school_grade_class_current_flag"] = make_nullable_flag(
    relation["owner_current_school_id"].eq(relation["candidate_current_school_id"])
    & relation["owner_current_grade"].eq(relation["candidate_current_grade"])
    & relation["owner_current_class_num"].eq(relation["candidate_current_class_num"]), class_valid
)
group_valid = relation[["owner_current_group_id", "candidate_current_group_id"]].notna().all(axis=1)
relation["same_group_current_flag"] = make_nullable_flag(
    relation["owner_current_group_id"].eq(relation["candidate_current_group_id"]), group_valid
)
relation["self_candidate_flag"] = relation["owner_user_id"].eq(relation["candidate_user_id"]).astype("Int8")

relation_idx = relation.set_index(["owner_user_id", "candidate_user_id"], drop=False)

chosen_by_piece = vote.set_index("question_piece_id")["chosen_user_id"]
candidate_chosen = nullable_int(candidate_core["question_piece_id"].map(chosen_by_piece))
selected_raw = candidate_chosen.notna() & candidate_core["candidate_user_id"].eq(candidate_chosen)
selected_canonical = selected_raw & candidate_core["canonical_candidate_pair_row_flag"].eq(1)
selected_count_raw_by_piece = selected_raw.groupby(candidate_core["question_piece_id"]).sum().astype("Int64")
selected_count_canonical_by_piece = selected_canonical.groupby(candidate_core["question_piece_id"]).sum().astype("Int64")

print(f"고유 owner-candidate 현재 관계쌍: {len(relation):,}")
display(pd.Series({
    "후보 원행": len(candidate_core),
    "canonical 후보쌍 행": int(candidate_core["canonical_candidate_pair_row_flag"].sum()),
    "중복 후보쌍 소속 원행": int(candidate_core["duplicate_candidate_pair_flag"].sum()),
    "owner 해석 가능 후보 원행": int(resolved_owner_for_candidate.notna().sum()),
    "현재 친구관계 관측 고유쌍": int(relation["current_friend_any_direction_flag"].notna().sum()),
}))
"""
    ),
    md(
        r"""
## 질문조각과 투표·Ping 현재상태 정제

질문조각에는 세트 위치·해석된 owner·후보 수·투표 존재 여부를 붙인다. 투표 원장에는
선택대상이 실제 후보 목록에 있었는지와 현재 사용자·학교 관계를 붙인다.
"""
    ),
    code(
        r"""
# 질문조각 정제
piece = piece.merge(owner_info, on="question_piece_id", how="left", validate="one_to_one")
piece = piece.merge(piece_candidate_counts.reset_index(), on="question_piece_id", how="left", validate="one_to_one")
for c in ["candidate_count_raw_per_piece", "candidate_count_canonical_per_piece", "candidate_distinct_user_count_per_piece"]:
    piece[c] = nullable_int(piece[c]).fillna(0)
piece["question_piece_created_date"] = piece["question_piece_created_at"].dt.normalize()
piece["question_piece_created_year_month"] = piece["question_piece_created_at"].dt.to_period("M").astype("string")
piece["is_2023_may_question_piece_flag"] = piece["question_piece_created_at"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")
piece["piece_snapshot_state"] = np.select(
    [
        piece["raw_is_voted"].eq(1) & piece["raw_is_skipped"].eq(1),
        piece["raw_is_voted"].eq(1),
        piece["raw_is_skipped"].eq(1),
    ],
    ["VOTED_AND_SKIPPED_HISTORY", "VOTED_HISTORY_ONLY", "SKIPPED_HISTORY_ONLY"],
    default="NO_VOTE_OR_SKIP_HISTORY",
)
piece["voted_and_skipped_recalc_flag"] = (
    piece["raw_is_voted"].eq(1) & piece["raw_is_skipped"].eq(1)
).astype("Int8")
piece["voted_and_skipped_match_flag"] = piece["voted_and_skipped_flag"].eq(
    piece["voted_and_skipped_recalc_flag"]
).astype("Int8")
piece["raw_voted_without_vote_record_flag"] = (
    piece["raw_is_voted"].eq(1) & piece["vote_record_exists_flag"].eq(0)
).astype("Int8")
piece["vote_record_without_raw_voted_flag"] = (
    piece["raw_is_voted"].eq(0) & piece["vote_record_exists_flag"].eq(1)
).astype("Int8")
piece["resolved_owner_profile_match_flag"] = piece["resolved_owner_user_id"].isin(known_user_ids).astype("Int8")
piece["resolved_owner_analysis_eligible_nonstaff_flag"] = map_nullable_flag(
    piece["resolved_owner_user_id"], user_idx["analysis_eligible_nonstaff_flag"]
)
piece["resolved_owner_current_school_id"] = nullable_int(piece["resolved_owner_user_id"].map(user_idx["current_school_id"]))
piece["analysis_eligible_piece_relation_flag"] = (
    piece["question_piece_id"].notna()
    & piece["orphan_question_flag"].eq(0)
    & piece["resolved_owner_user_id"].notna()
    & piece["resolved_owner_profile_match_flag"].eq(1)
).astype("Int8")

raw_voted_without = int(piece["raw_voted_without_vote_record_flag"].sum())
vote_without_raw = int(piece["vote_record_without_raw_voted_flag"].sum())
voted_skipped = int(piece["voted_and_skipped_recalc_flag"].sum())
add_qa(PIECE_MART, "voted_and_skipped_source_flag_mismatch", int(piece["voted_and_skipped_match_flag"].ne(1).sum()), 0,
       "PASS" if piece["voted_and_skipped_match_flag"].eq(1).all() else "FAIL", "CRITICAL",
       "투표·스킵 동시 이력 플래그를 재계산합니다.")
add_qa("CROSS_MART", "raw_voted_without_vote_record_rows", raw_voted_without, 0,
       "WARN" if raw_voted_without else "PASS", "HIGH",
       "현재 조각은 voted 이력이지만 현재 투표 원장에는 대응 레코드가 없는 행입니다.")
add_qa(PIECE_MART, "vote_record_without_raw_voted_rows", vote_without_raw, 0,
       "PASS" if vote_without_raw == 0 else "WARN", "HIGH",
       "투표 원장은 있지만 조각 raw_is_voted가 0인 행입니다.")
add_qa(PIECE_MART, "voted_and_skipped_history_rows", voted_skipped, 0,
       "WARN" if voted_skipped else "PASS", "MEDIUM",
       "같은 조각에 투표와 스킵 이력이 모두 남은 현재 스냅샷 상태입니다.")

PIECE_DERIVED = {c: "질문세트·투표·후보 원장을 같은 question_piece_id로 대사해 만든 파생 컬럼." for c in piece.columns[7:]}
PIECE_DERIVED.update({
    "piece_snapshot_state": "raw_is_voted와 raw_is_skipped의 현재 이력 조합.",
    "raw_voted_without_vote_record_flag": "voted 이력은 있으나 현재 투표 원장 레코드가 없으면 1.",
    "vote_record_without_raw_voted_flag": "투표 원장은 있으나 raw_is_voted가 0이면 1.",
    "analysis_eligible_piece_relation_flag": "질문·owner 관계 분석 기본 조건을 충족하면 1.",
})
write_full(PIECE_MART, piece)
add_dictionary(PIECE_MART, piece.columns, PIECE_DERIVED)

# 투표·Ping 현재상태 정제
vote = vote.merge(
    owner_info[["question_piece_id", "resolved_owner_user_id", "owner_resolution_code", "owner_resolution_label"]],
    on="question_piece_id", how="left", validate="one_to_one"
)
vote = vote.merge(piece_candidate_counts.reset_index(), on="question_piece_id", how="left", validate="one_to_one")
for c in ["candidate_count_raw_per_piece", "candidate_count_canonical_per_piece", "candidate_distinct_user_count_per_piece"]:
    vote[c] = nullable_int(vote[c]).fillna(0)
vote["selected_candidate_raw_row_count"] = nullable_int(vote["question_piece_id"].map(selected_count_raw_by_piece)).fillna(0)
vote["selected_candidate_canonical_row_count"] = nullable_int(vote["question_piece_id"].map(selected_count_canonical_by_piece)).fillna(0)
vote["chosen_in_candidate_raw_flag"] = vote["selected_candidate_raw_row_count"].gt(0).astype("Int8")
vote["chosen_in_candidate_canonical_flag"] = vote["selected_candidate_canonical_row_count"].gt(0).astype("Int8")
vote["vote_status_code_valid_flag"] = vote["vote_status_current"].isin(["C", "I", "B"]).astype("Int8")
vote["vote_status_label"] = ("CURRENT_STATUS_" + vote["vote_status_current"].astype("string")).astype("string")
vote["ping_answer_status_code_valid_flag"] = vote["ping_answer_status_current"].isin(["N", "A", "P"]).astype("Int8")
vote["ping_answer_status_label"] = ("CURRENT_ANSWER_STATUS_" + vote["ping_answer_status_current"].astype("string")).astype("string")
vote["ping_answered_current_flag"] = vote["ping_answer_status_current"].isin(["A", "P"]).astype("Int8")
vote["vote_record_created_date"] = vote["vote_record_created_at"].dt.normalize()
vote["vote_record_created_year_month"] = vote["vote_record_created_at"].dt.to_period("M").astype("string")
vote["is_2023_may_vote_record_flag"] = vote["vote_record_created_at"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")
vote["vote_timestamp_valid_flag"] = vote["vote_record_created_at"].notna().astype("Int8")
vote["ping_count_nonnegative_flag"] = (
    vote["ping_report_count_current"].fillna(0).ge(0) & vote["ping_opened_times_current"].fillna(0).ge(0)
).astype("Int8")
vote["voter_profile_match_flag"] = vote["voter_user_id"].isin(known_user_ids).astype("Int8")
vote["chosen_profile_match_flag"] = vote["chosen_user_id"].isin(known_user_ids).astype("Int8")
vote["voter_analysis_eligible_nonstaff_flag"] = map_nullable_flag(vote["voter_user_id"], user_idx["analysis_eligible_nonstaff_flag"])
vote["chosen_analysis_eligible_nonstaff_flag"] = map_nullable_flag(vote["chosen_user_id"], user_idx["analysis_eligible_nonstaff_flag"])
vote["voter_current_school_id"] = nullable_int(vote["voter_user_id"].map(user_idx["current_school_id"]))
vote["chosen_current_school_id"] = nullable_int(vote["chosen_user_id"].map(user_idx["current_school_id"]))
vote_school_valid = vote[["voter_current_school_id", "chosen_current_school_id"]].notna().all(axis=1)
vote["voter_chosen_same_school_current_flag"] = make_nullable_flag(
    vote["voter_current_school_id"].eq(vote["chosen_current_school_id"]), vote_school_valid
)
vote["voter_matches_resolved_owner_flag"] = make_nullable_flag(
    vote["voter_user_id"].eq(vote["resolved_owner_user_id"]), vote["resolved_owner_user_id"].notna()
)
vote["analysis_eligible_vote_record_flag"] = (
    vote["user_question_record_id"].notna()
    & vote["voter_profile_match_flag"].eq(1)
    & vote["chosen_profile_match_flag"].eq(1)
    & vote["orphan_question_piece_flag"].eq(0)
    & vote["orphan_question_flag"].eq(0)
    & vote["piece_question_mismatch_flag"].eq(0)
    & vote["vote_status_code_valid_flag"].eq(1)
    & vote["ping_answer_status_code_valid_flag"].eq(1)
    & vote["vote_timestamp_valid_flag"].eq(1)
    & vote["ping_count_nonnegative_flag"].eq(1)
).astype("Int8")

chosen_missing = int(vote["chosen_in_candidate_canonical_flag"].eq(0).sum())
add_qa("CROSS_MART", "vote_chosen_user_missing_from_canonical_candidates", chosen_missing, 0,
       "PASS" if chosen_missing == 0 else "FAIL", "CRITICAL",
       "모든 투표 선택대상이 해당 질문조각의 canonical 후보 목록에 있어야 합니다.")
add_qa(VOTE_MART, "invalid_vote_status_code_rows", int(vote["vote_status_code_valid_flag"].eq(0).sum()), 0,
       "PASS" if vote["vote_status_code_valid_flag"].eq(1).all() else "FAIL", "CRITICAL", "C/I/B 코드만 관측됩니다.")
add_qa(VOTE_MART, "invalid_ping_answer_status_code_rows", int(vote["ping_answer_status_code_valid_flag"].eq(0).sum()), 0,
       "PASS" if vote["ping_answer_status_code_valid_flag"].eq(1).all() else "FAIL", "CRITICAL", "N/A/P 코드만 관측됩니다.")

VOTE_DERIVED = {c: "질문조각·후보·현재 사용자 기준정보를 대사해 만든 파생 컬럼." for c in vote.columns[17:]}
VOTE_DERIVED.update({
    "vote_status_label": "의미 추정 없이 현재 vote 상태 코드를 표시한 라벨.",
    "ping_answer_status_label": "의미 추정 없이 현재 Ping 답변 상태 코드를 표시한 라벨.",
    "chosen_in_candidate_canonical_flag": "선택대상이 해당 질문조각의 canonical 후보 목록에 있으면 1.",
    "analysis_eligible_vote_record_flag": "투표·Ping 현재상태 분석 기본 조건을 충족하면 1.",
})
write_full(VOTE_MART, vote)
add_dictionary(VOTE_MART, vote.columns, VOTE_DERIVED)

display(pd.Series({
    "질문조각": len(piece),
    "투표 원장": len(vote),
    "raw voted지만 투표 원장 없음": raw_voted_without,
    "투표·스킵 동시 이력": voted_skipped,
    "선택 후보 목록 불일치": chosen_missing,
}))
"""
    ),
    md(
        r"""
## 후보 원행 분석 준비

원본 후보 행은 그대로 유지하면서 질문·세트·owner·선택대상·현재 관계 플래그를 붙인다.
빈도 분석은 전체 원행, 관계 비율은 canonical 및 owner 해석 가능 행을 사용한다.
"""
    ),
    code(
        r"""
owner_attach_cols = [
    "question_piece_id", "question_set_membership_row_count", "single_question_set_id",
    "single_question_position", "unambiguous_qset_owner_user_id", "resolved_owner_user_id",
    "owner_resolution_code", "owner_resolution_label", "user_question_record_id",
    "chosen_user_id", "vote_record_created_at",
]
owner_attach = owner_info[owner_attach_cols].set_index("question_piece_id")
piece_attach = piece.set_index("question_piece_id")[["question_id", "piece_snapshot_state"]]
qset_status_map = qset.set_index("question_set_id")["question_set_status"]
relation_attach_cols = [
    "owner_profile_match_flag", "candidate_profile_match_flag",
    "owner_analysis_eligible_nonstaff_flag", "candidate_analysis_eligible_nonstaff_flag",
    "owner_current_group_id", "owner_current_school_id", "owner_current_grade", "owner_current_class_num",
    "candidate_current_group_id", "candidate_current_school_id", "candidate_current_grade", "candidate_current_class_num",
    "owner_lists_candidate_current_friend_flag", "candidate_lists_owner_current_friend_flag",
    "current_friend_any_direction_flag", "current_friend_mutual_flag",
    "same_school_current_flag", "same_school_grade_current_flag",
    "same_school_grade_class_current_flag", "same_group_current_flag", "self_candidate_flag",
]

candidate_output_path = OUTPUT_DIR / f"{CANDIDATE_MART}_clean.csv.gz"
candidate_output_rows = 0
candidate_output_columns = None
candidate_agg = defaultdict(int)

with gzip.open(candidate_output_path, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
    for start in range(0, len(candidate_core), CHUNK_SIZE):
        df = candidate_core.iloc[start:start + CHUNK_SIZE].copy()
        df["candidate_source_date"] = df["candidate_source_created_at"].dt.normalize()
        df["candidate_source_year_month"] = df["candidate_source_created_at"].dt.to_period("M").astype("string")
        df["is_2023_may_candidate_flag"] = df["candidate_source_created_at"].dt.to_period("M").eq(pd.Period("2023-05")).astype("Int8")
        df["candidate_timestamp_valid_flag"] = df["candidate_source_created_at"].notna().astype("Int8")

        matched_owner = owner_attach.reindex(df["question_piece_id"].to_numpy())
        matched_owner.index = df.index
        for c in owner_attach_cols[1:]:
            df[c] = matched_owner[c]
        df = df.rename(columns={
            "single_question_set_id": "question_set_id",
            "single_question_position": "question_position",
            "unambiguous_qset_owner_user_id": "question_set_owner_user_id",
        })
        df["question_set_status_current"] = df["question_set_id"].map(qset_status_map).astype("string")

        matched_piece = piece_attach.reindex(df["question_piece_id"].to_numpy())
        matched_piece.index = df.index
        df["question_id"] = matched_piece["question_id"]
        df["piece_snapshot_state"] = matched_piece["piece_snapshot_state"]

        matched_counts = piece_candidate_counts.reindex(df["question_piece_id"].to_numpy())
        matched_counts.index = df.index
        for c in piece_candidate_counts.columns:
            df[c] = nullable_int(matched_counts[c]).fillna(0)
        count = df["candidate_count_canonical_per_piece"]
        df["candidate_choice_count_band"] = np.select(
            [count.eq(0), count.eq(1), count.eq(2), count.eq(3), count.eq(4), count.gt(4)],
            ["0", "1", "2", "3", "4", "5_PLUS"], default="MISSING"
        )

        df["selected_candidate_raw_row_flag"] = (
            df["chosen_user_id"].notna() & df["candidate_user_id"].eq(df["chosen_user_id"])
        ).astype("Int8")
        df["selected_candidate_canonical_pair_flag"] = (
            df["selected_candidate_raw_row_flag"].eq(1) & df["canonical_candidate_pair_row_flag"].eq(1)
        ).astype("Int8")

        relation_keys = pd.MultiIndex.from_arrays([df["resolved_owner_user_id"], df["candidate_user_id"]])
        matched_relation = relation_idx.reindex(relation_keys)
        matched_relation.index = df.index
        for c in relation_attach_cols:
            df[c] = matched_relation[c]

        df["analysis_eligible_candidate_raw_flag"] = (
            df["candidate_exposure_id"].notna()
            & df["question_piece_id"].notna()
            & df["orphan_question_piece_flag"].eq(0)
            & df["resolved_owner_user_id"].notna()
            & df["owner_profile_match_flag"].eq(1)
            & df["candidate_profile_match_flag"].eq(1)
            & df["candidate_timestamp_valid_flag"].eq(1)
        ).astype("Int8")
        df["analysis_eligible_candidate_canonical_flag"] = (
            df["analysis_eligible_candidate_raw_flag"].eq(1)
            & df["canonical_candidate_pair_row_flag"].eq(1)
        ).astype("Int8")

        if candidate_output_columns is None:
            candidate_output_columns = df.columns.tolist()
        elif candidate_output_columns != df.columns.tolist():
            raise AssertionError("후보 출력 청크별 컬럼 구성이 다릅니다.")
        df.to_csv(
            handle, index=False, header=(start == 0), na_rep="",
            date_format="%Y-%m-%d %H:%M:%S.%f",
        )
        candidate_output_rows += len(df)
        candidate_agg["analysis_raw"] += int(df["analysis_eligible_candidate_raw_flag"].sum())
        candidate_agg["analysis_canonical"] += int(df["analysis_eligible_candidate_canonical_flag"].sum())
        candidate_agg["selected_canonical"] += int(df["selected_candidate_canonical_pair_flag"].sum())
        candidate_agg["owner_missing"] += int(df["resolved_owner_user_id"].isna().sum())
        candidate_agg["friend_observed"] += int(df["current_friend_any_direction_flag"].notna().sum())
        if start == 0 or (start // CHUNK_SIZE) % 5 == 0:
            print(f"  {CANDIDATE_MART}: {candidate_output_rows:,}행 작성", flush=True)

processing_log.append({
    "mart": CANDIDATE_MART,
    "source_path": str(SOURCE_DIR / f"{CANDIDATE_MART}.csv.gz"),
    "output_path": str(candidate_output_path),
    "source_rows": expected_rows[CANDIDATE_MART],
    "output_rows": candidate_output_rows,
    "row_preserved": candidate_output_rows == expected_rows[CANDIDATE_MART],
    "source_columns": 9,
    "output_columns": len(candidate_output_columns or []),
})
add_qa(CANDIDATE_MART, "row_count_preserved", candidate_output_rows, expected_rows[CANDIDATE_MART],
       "PASS" if candidate_output_rows == expected_rows[CANDIDATE_MART] else "FAIL", "CRITICAL",
       "전처리 전후 후보 원행 수가 같아야 합니다.")
add_qa("CROSS_MART", "selected_candidate_canonical_rows_match_vote_rows",
       candidate_agg["selected_canonical"], int(vote["chosen_user_id"].notna().sum()),
       "PASS" if candidate_agg["selected_canonical"] == int(vote["chosen_user_id"].notna().sum()) else "FAIL",
       "CRITICAL", "선택된 canonical 후보 행 수와 선택대상이 있는 투표 원장 수가 같아야 합니다.")
add_qa(CANDIDATE_MART, "candidate_rows_without_resolved_owner", candidate_agg["owner_missing"], 0,
       "PASS" if candidate_agg["owner_missing"] == 0 else "WARN", "HIGH",
       "owner를 해석하지 못한 후보 원행입니다.")

CANDIDATE_DERIVED = {c: "질문조각·세트·투표·현재 사용자 관계를 후보 원행에 연결한 파생 컬럼." for c in candidate_output_columns[9:]}
CANDIDATE_DERIVED.update({
    "candidate_choice_count_band": "질문조각별 canonical 후보 수 구간.",
    "selected_candidate_canonical_pair_flag": "해당 후보가 선택대상이며 canonical 행이면 1.",
    "current_friend_any_direction_flag": "현재 친구 목록 어느 한쪽에 상대가 있으면 1.",
    "current_friend_mutual_flag": "현재 친구 목록 양쪽에 서로가 있으면 1.",
    "same_school_current_flag": "현재 학교가 같으면 1.",
    "same_school_grade_current_flag": "현재 학교·학년이 같으면 1.",
    "same_school_grade_class_current_flag": "현재 학교·학년·반이 같으면 1.",
    "analysis_eligible_candidate_canonical_flag": "현재 관계 비율 계산에 사용하는 canonical 후보 행이면 1.",
})
add_dictionary(CANDIDATE_MART, candidate_output_columns, CANDIDATE_DERIVED)
print(f"완료: {CANDIDATE_MART} → {candidate_output_rows:,}행, {len(candidate_output_columns):,}열")
display(pd.Series(candidate_agg))
"""
    ),
    md(
        r"""
## 최종 QA와 전처리 기록

FAIL이 하나라도 있으면 완료본으로 사용하지 않는다. WARN은 행을 삭제할 문제가 아니라
원천 스냅샷의 해석 범위를 표시한다.
"""
    ),
    code(
        r"""
# 네 마트 공통 정합성 QA
add_qa("CROSS_MART", "question_set_json_flag_mismatch_rows",
       int((qset["piece_list_json_flag_match"].ne(1) | qset["piece_list_array_flag_match"].ne(1)).sum()), 0,
       "PASS" if (qset["piece_list_json_flag_match"].eq(1) & qset["piece_list_array_flag_match"].eq(1)).all() else "FAIL",
       "CRITICAL", "세트 JSON 관련 원천 플래그와 재계산 플래그를 대사합니다.")
add_qa("CROSS_MART", "question_set_json_length_mismatch_rows",
       int(qset["piece_list_length_match_flag"].ne(1).sum()), 0,
       "PASS" if qset["piece_list_length_match_flag"].eq(1).all() else "FAIL",
       "CRITICAL", "세트 JSON 배열 길이 원천값과 재계산값을 대사합니다.")
add_qa("CROSS_MART", "question_set_opening_delay_mismatch_rows",
       int(qset["opening_delay_match_flag"].ne(1).sum()), 0,
       "PASS" if qset["opening_delay_match_flag"].eq(1).all() else "FAIL",
       "CRITICAL", "opening_time-created_at 초를 재계산합니다.")
add_qa("CROSS_MART", "candidate_orphan_piece_rows",
       int(candidate_core["orphan_question_piece_flag"].eq(1).sum()), 0,
       "PASS" if candidate_core["orphan_question_piece_flag"].eq(1).sum() == 0 else "FAIL",
       "CRITICAL", "후보 원행의 질문조각이 현재 조각 원장에 존재해야 합니다.")
add_qa("CROSS_MART", "vote_piece_question_mismatch_rows",
       int(vote["piece_question_mismatch_flag"].eq(1).sum()), 0,
       "PASS" if vote["piece_question_mismatch_flag"].eq(1).sum() == 0 else "FAIL",
       "CRITICAL", "투표 question_id와 질문조각의 question_id가 일치해야 합니다.")

qa = pd.DataFrame(qa_rows)
qa["status_order"] = qa["status"].map({"FAIL": 0, "WARN": 1, "PASS": 2}).fillna(9)
qa = qa.sort_values(["status_order", "severity", "mart", "test_name"]).drop(columns="status_order")
dictionary_df = pd.DataFrame(dictionary_rows).drop_duplicates(["mart", "column_name"])
processing_df = pd.DataFrame(processing_log)

qa_path = REPORT_DIR / "question_candidate_qa_summary.csv"
dictionary_path = REPORT_DIR / "question_candidate_column_dictionary.csv"
processing_path = REPORT_DIR / "question_candidate_processing_log.csv"
report_path = REPORT_DIR / "question_candidate_preprocessing_report.md"
qa.to_csv(qa_path, index=False, encoding="utf-8-sig")
dictionary_df.to_csv(dictionary_path, index=False, encoding="utf-8-sig")
processing_df.to_csv(processing_path, index=False, encoding="utf-8-sig")

fail_count = int(qa["status"].eq("FAIL").sum())
warn_count = int(qa["status"].eq("WARN").sum())
pass_count = int(qa["status"].eq("PASS").sum())
status_counts = qset["question_set_status"].value_counts(dropna=False).to_dict()
owner_resolution_counts = owner_info["owner_resolution_label"].value_counts(dropna=False).to_dict()

report_lines = [
    "# 질문·후보 관계 4개 마트 전처리 기록", "",
    "## 결과", "",
    f"- PASS: {pass_count}건", f"- WARN: {warn_count}건", f"- FAIL: {fail_count}건",
    "- 원본 행 삭제: 0건", "- 원본 컬럼 삭제: 0건", "- 결측값 임의 대체: 0건", "- 중복 원행 삭제: 0건", "",
    "## 공통 연결 결과", "",
    f"- 질문세트: {len(qset):,}개", f"- 세트 위치: {len(bridge):,}행",
    f"- 현재 원장에 없는 세트 참조 조각: {missing_positions:,}행",
    f"- 현재 질문조각: {len(piece):,}개", f"- 투표·Ping 현재상태: {len(vote):,}건",
    f"- 후보 원행: {len(candidate_core):,}행",
    f"- canonical 질문조각-후보쌍: {int(candidate_core['canonical_candidate_pair_row_flag'].sum()):,}행",
    f"- 고유 owner-candidate 현재 관계쌍: {len(relation):,}쌍",
    f"- 질문세트 현재 코드 분포: {status_counts}",
    f"- owner 해석 코드 분포: {owner_resolution_counts}", "",
    "## 사용 규칙", "",
    "- 후보 원행 빈도는 전체 행을 사용한다.",
    "- 후보 관계 비율은 `analysis_eligible_candidate_canonical_flag=1`을 사용한다.",
    "- 질문조각 관계 분석은 `analysis_eligible_piece_relation_flag=1`을 사용한다.",
    "- 투표·Ping 현재상태 분석은 `analysis_eligible_vote_record_flag=1`을 사용한다.",
    "- 현재 친구·학교·학년·반은 질문 당시 상태가 아니다.",
    "- F/O/C, C/I/B, N/A/P 코드는 의미를 추정하지 않고 현재 코드로만 사용한다.",
    "- F는 완주, opening_time은 실제 노출, raw_is_skipped는 스킵 이벤트 횟수가 아니다.",
    "- 투표·스킵 동시 이력과 현재 원장 누락 참조는 행을 삭제하지 않고 플래그로 보존한다.", "",
    "## 생성 파일", "",
]
for row in processing_log:
    report_lines.append(f"- `{Path(row['output_path']).name}`: {row['output_rows']:,}행, {row['output_columns']}열")
report_lines.extend(["", "## QA 경고", ""])
warned = qa.loc[qa["status"].eq("WARN")]
if warned.empty:
    report_lines.append("- 없음")
else:
    for row in warned.itertuples(index=False):
        report_lines.append(f"- `{row.mart}` / `{row.test_name}`: {row.actual} — {row.explanation}")
report_path.write_text("\n".join(report_lines), encoding="utf-8")

display(Markdown(f"### QA 결과: PASS {pass_count} / WARN {warn_count} / FAIL {fail_count}"))
display(qa.reset_index(drop=True))
display(processing_df)
print(f"QA 요약: {qa_path}")
print(f"컬럼 사전: {dictionary_path}")
print(f"처리 기록: {processing_path}")
print(f"전처리 보고서: {report_path}")

if fail_count:
    raise AssertionError(f"최종 QA FAIL이 {fail_count}건 있습니다. 완료본으로 사용하지 마세요.")
print("질문·후보 관계 4개 마트 전처리와 최종 QA가 완료되었습니다.")
"""
    ),
]


nb = nbf.v4.new_notebook(
    cells=cells,
    metadata={
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3"},
    },
)
NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, NOTEBOOK_PATH)
print(NOTEBOOK_PATH)
