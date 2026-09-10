from __future__ import annotations

import gc
import gzip
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


SOURCE_NAMES = [
    "mart_promo_event_receipt_v2",
    "mart_safety_event_v2",
    "mart_lifecycle_event_v2",
    "mart_attendance_record_v2",
]
NA_VALUES = [r"\N"]
CHUNK_SIZE = 150_000


@dataclass
class Context:
    repo_root: Path
    source_dir: Path
    output_dir: Path
    report_dir: Path
    source_manifest: pd.DataFrame
    expected_rows: dict[str, int]
    profile: pd.DataFrame
    profile_user_ids: set[int]
    signup_date_by_user: dict[int, date]
    qa_rows: list[dict[str, Any]] = field(default_factory=list)
    processing_log: list[dict[str, Any]] = field(default_factory=list)
    dictionary_rows: list[dict[str, Any]] = field(default_factory=list)
    scope_rows: list[dict[str, Any]] = field(default_factory=list)


def find_repo_root(start: str | Path | None = None) -> Path:
    start_path = Path(start or Path.cwd()).resolve()
    for candidate in (start_path, *start_path.parents):
        if (candidate / ".git").exists():
            return candidate
    raise FileNotFoundError("Git 저장소 루트를 찾지 못했습니다.")


def prepare_context(repo_root: str | Path | None = None) -> Context:
    repo_root = Path(repo_root).resolve() if repo_root is not None else find_repo_root()
    source_dir = repo_root / "data" / "marts" / "23_marts_csv"
    output_dir = repo_root / "data" / "processed" / "lifecycle_safety_attendance"
    report_dir = repo_root / "analysis" / "preprocessing" / "lifecycle_safety_attendance" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(source_dir / "export_manifest.csv", dtype="string")
    source_manifest = manifest.loc[manifest["object_name"].isin(SOURCE_NAMES)].copy()
    source_manifest["exported_rows"] = pd.to_numeric(source_manifest["exported_rows"], errors="coerce").astype("Int64")
    source_manifest["column_count"] = pd.to_numeric(source_manifest["column_count"], errors="coerce").astype("Int64")
    expected_rows = dict(zip(source_manifest["object_name"], source_manifest["exported_rows"].astype(int)))
    if set(expected_rows) != set(SOURCE_NAMES):
        raise AssertionError("전처리 대상 4개가 export_manifest에 모두 존재하지 않습니다.")

    profile_path = repo_root / "data" / "processed" / "viral_school" / "mart_user_acquisition_profile_v2_clean.csv.gz"
    if not profile_path.exists():
        raise FileNotFoundError(f"공통 사용자 기준 마트가 없습니다: {profile_path}")
    profile = pd.read_csv(
        profile_path,
        usecols=["user_id", "signup_at", "current_school_id", "is_staff", "is_superuser"],
        dtype="string",
        keep_default_na=False,
        na_values=NA_VALUES,
        low_memory=False,
    )
    profile["user_id"] = pd.to_numeric(profile["user_id"], errors="coerce").astype("Int64")
    if len(profile) != 677_085 or profile["user_id"].isna().any() or profile["user_id"].duplicated().any():
        raise AssertionError("공통 사용자 기준 마트가 677,085명·user_id 고유 조건을 만족하지 않습니다.")
    signup_parsed = pd.to_datetime(profile["signup_at"], errors="coerce")
    if signup_parsed.isna().any():
        raise AssertionError("공통 사용자 기준 마트의 signup_at을 파싱할 수 없습니다.")
    profile_user_ids = set(profile["user_id"].astype("int64").tolist())
    signup_date_by_user = dict(zip(profile["user_id"].astype("int64"), signup_parsed.dt.date))
    return Context(
        repo_root=repo_root,
        source_dir=source_dir,
        output_dir=output_dir,
        report_dir=report_dir,
        source_manifest=source_manifest,
        expected_rows=expected_rows,
        profile=profile,
        profile_user_ids=profile_user_ids,
        signup_date_by_user=signup_date_by_user,
    )


def read_source(ctx: Context, name: str, *, chunksize: int | None = None):
    return pd.read_csv(
        ctx.source_dir / f"{name}.csv.gz",
        compression="gzip",
        encoding="utf-8-sig",
        dtype="string",
        na_values=NA_VALUES,
        keep_default_na=False,
        chunksize=chunksize,
        low_memory=False,
    )


def write_frame(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(
        path,
        index=False,
        compression={"method": "gzip", "compresslevel": 3},
        encoding="utf-8-sig",
        na_rep="",
        date_format="%Y-%m-%d %H:%M:%S.%f",
    )


def to_int(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")


def normalize_text(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    return normalized.mask(normalized.eq(""), pd.NA)


def add_qa(ctx: Context, mart: str, test_name: str, actual: Any, expected: Any,
           status: str, severity: str, explanation: str) -> None:
    ctx.qa_rows.append({
        "mart": mart,
        "test_name": test_name,
        "actual": actual,
        "expected": expected,
        "status": status,
        "severity": severity,
        "explanation": explanation,
    })


def add_dictionary(ctx: Context, mart: str, source_columns: list[str], output_columns: list[str],
                   descriptions: dict[str, str]) -> None:
    source_set = set(source_columns)
    for column in output_columns:
        ctx.dictionary_rows.append({
            "mart": mart,
            "column_name": column,
            "column_role": "SOURCE_PRESERVED" if column in source_set else "DERIVED",
            "description": descriptions.get(
                column,
                "원본 v2 마트 컬럼. 이름과 값을 보존함." if column in source_set else "분석 안전성을 위해 파생한 컬럼.",
            ),
        })


def register_log(ctx: Context, mart: str, source_rows: int, output_rows: int,
                 source_columns: int, output_columns: int, source_path: Path,
                 output_path: Path, row_preserved: Any) -> None:
    ctx.processing_log.append({
        "mart": mart,
        "source_path": str(source_path),
        "output_path": str(output_path),
        "source_rows": source_rows,
        "output_rows": output_rows,
        "row_preserved": row_preserved,
        "source_columns": source_columns,
        "output_columns": output_columns,
    })
    if row_preserved is True:
        add_qa(ctx, mart, "row_count_preserved", output_rows, source_rows,
               "PASS" if output_rows == source_rows else "FAIL", "CRITICAL",
               "원본 행 수를 그대로 보존해야 합니다.")


def process_lifecycle(ctx: Context) -> dict[str, Any]:
    name = "mart_lifecycle_event_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    frame = read_source(ctx, name)
    source_columns = list(frame.columns)
    to_int(frame, ["source_row_id", "service_user_id", "user_link_available_flag"])

    event_at = pd.to_datetime(frame["event_at_raw"], errors="coerce")
    service_user = pd.to_numeric(frame["service_user_id"], errors="coerce")
    is_signup = frame["lifecycle_event_type"].eq("SIGNUP")
    is_withdraw = frame["lifecycle_event_type"].eq("WITHDRAW")
    profile_match = service_user.isin(ctx.profile_user_ids) & service_user.notna()
    link_recalc = profile_match.astype("Int8")

    reason = normalize_text(frame["reason_raw"])
    reason_map = {
        "함께 할 친구가 없어서": "SOCIAL_COLD_START",
        "재밌는 질문이 없어서": "CONTENT_VALUE",
        "버그가 너무 많아서": "PRODUCT_QUALITY",
        "구독료가 너무 비싸서": "PRICE",
        "admin": "INTERNAL_OR_TEST",
        "test": "INTERNAL_OR_TEST",
        "기타 이유": "OTHER",
        "기타": "OTHER",
    }
    reason_category = reason.map(reason_map).astype("string")
    reason_category = reason_category.mask(is_signup, "NOT_APPLICABLE")
    reason_category = reason_category.fillna("UNMAPPED")

    frame["event_at_parse_status"] = np.where(event_at.notna(), "PARSED", "INVALID_OR_MISSING")
    frame["event_date_recomputed"] = event_at.dt.strftime("%Y-%m-%d").astype("string")
    frame["event_year_month"] = event_at.dt.strftime("%Y-%m").astype("string")
    frame["reason_normalized"] = reason
    frame["withdraw_reason_category_rule_v1"] = reason_category
    frame["service_user_profile_match_recalc_flag"] = profile_match.astype("Int8")
    frame["user_link_available_recalc_flag"] = link_recalc
    frame["user_link_flag_match"] = (
        pd.to_numeric(frame["user_link_available_flag"], errors="coerce").fillna(-1).astype("int64")
        == link_recalc.astype("int64")
    ).astype("Int8")
    frame["identity_link_status"] = np.select(
        [is_signup & profile_match, is_withdraw & service_user.isna()],
        ["LINKED_ACCOUNT_SIGNUP", "SOURCE_HAS_NO_USER_ID"],
        default="UNEXPECTED_IDENTITY_STATE",
    )
    frame["event_analysis_role"] = np.where(is_signup, "COHORT_ENTRY", "AGGREGATE_UNLINKED_WITHDRAWAL")
    frame["cohort_entry_eligible_flag"] = is_signup.astype("Int8")
    frame["individual_signup_to_withdraw_path_eligible_flag"] = 0
    frame["aggregate_withdrawal_trend_eligible_flag"] = is_withdraw.astype("Int8")
    frame["time_zone_officially_known_flag"] = 0
    frame["applied_timestamp_offset_minutes"] = 0
    frame["source_observation_scope"] = np.where(
        is_signup, "GLOBAL_ACCOUNT_SIGNUP_LEDGER", "GLOBAL_UNLINKED_WITHDRAWAL_LEDGER"
    )

    write_frame(frame, output_path)
    register_log(ctx, name, ctx.expected_rows[name], len(frame), len(source_columns), len(frame.columns),
                 source_path, output_path, True)

    key_duplicates = int(frame.duplicated(["source_table", "source_row_id"]).sum())
    source_type_mismatch = int((
        ~(
            (frame["source_table"].eq("accounts_user") & is_signup)
            | (frame["source_table"].eq("accounts_userwithdraw") & is_withdraw)
        )
    ).sum())
    forced_withdraw_links = int((is_withdraw & service_user.notna()).sum())
    link_mismatch = int(frame["user_link_flag_match"].eq(0).sum())
    unmapped_withdraw_reasons = int((is_withdraw & frame["withdraw_reason_category_rule_v1"].eq("UNMAPPED")).sum())

    signup_ids = set(service_user.loc[is_signup].dropna().astype("int64").tolist())
    signup_profile_set_difference = len(signup_ids.symmetric_difference(ctx.profile_user_ids))
    profile_times = ctx.profile[["user_id", "signup_at"]].copy()
    profile_times["user_id"] = profile_times["user_id"].astype("Int64")
    signup_check = frame.loc[is_signup, ["service_user_id", "event_at_raw"]].merge(
        profile_times, left_on="service_user_id", right_on="user_id", how="left", validate="one_to_one"
    )
    signup_round_mismatch = int((
        pd.to_datetime(signup_check["event_at_raw"], errors="coerce")
        != pd.to_datetime(signup_check["signup_at"], errors="coerce").dt.round("s")
    ).sum())

    add_qa(ctx, name, "source_key_duplicate_rows", key_duplicates, 0,
           "PASS" if key_duplicates == 0 else "FAIL", "CRITICAL", "source_table×source_row_id는 고유해야 합니다.")
    add_qa(ctx, name, "timestamp_parse_fail_rows", int(event_at.isna().sum()), 0,
           "PASS" if event_at.notna().all() else "FAIL", "CRITICAL", "모든 생애주기 시각은 파싱 가능해야 합니다.")
    add_qa(ctx, name, "source_type_mapping_mismatch_rows", source_type_mismatch, 0,
           "PASS" if source_type_mismatch == 0 else "FAIL", "CRITICAL", "원천 테이블과 이벤트 유형이 일치해야 합니다.")
    add_qa(ctx, name, "signup_count", int(is_signup.sum()), 677_085,
           "PASS" if int(is_signup.sum()) == 677_085 else "FAIL", "CRITICAL", "전체 가입 원장을 보존해야 합니다.")
    add_qa(ctx, name, "withdraw_count", int(is_withdraw.sum()), 70_764,
           "PASS" if int(is_withdraw.sum()) == 70_764 else "FAIL", "CRITICAL", "전체 탈퇴 원장을 보존해야 합니다.")
    add_qa(ctx, name, "signup_profile_user_set_difference", signup_profile_set_difference, 0,
           "PASS" if signup_profile_set_difference == 0 else "FAIL", "CRITICAL", "가입자 집합은 공통 사용자 기준과 같아야 합니다.")
    add_qa(ctx, name, "signup_timestamp_rounding_mismatch_rows", signup_round_mismatch, 0,
           "PASS" if signup_round_mismatch == 0 else "FAIL", "CRITICAL", "가입 시각은 원장 초 단위 반올림값과 같아야 합니다.")
    add_qa(ctx, name, "withdraw_rows_with_forced_user_id", forced_withdraw_links, 0,
           "PASS" if forced_withdraw_links == 0 else "FAIL", "CRITICAL", "user_id 없는 탈퇴를 계정에 강제 연결하지 않습니다.")
    add_qa(ctx, name, "user_link_flag_mismatch_rows", link_mismatch, 0,
           "PASS" if link_mismatch == 0 else "FAIL", "CRITICAL", "사용자 연결 플래그가 공통 사용자 기준과 일치해야 합니다.")
    add_qa(ctx, name, "unmapped_withdraw_reason_rows", unmapped_withdraw_reasons, 0,
           "PASS" if unmapped_withdraw_reasons == 0 else "WARN", "DATA_QUALITY", "탈퇴 사유 규칙의 미분류 값을 확인합니다.")
    add_qa(ctx, name, "withdraw_user_link_unavailable_rows", int(is_withdraw.sum()), "원천 상태 보존",
           "WARN", "SOURCE_LIMITATION", "탈퇴 원천에 user_id가 없어 개인별 이탈 타깃을 만들 수 없습니다.")
    add_qa(ctx, name, "official_timezone_unknown", 0, 1,
           "WARN", "SOURCE_LIMITATION", "공식 시간대가 없어 원시 시각을 보존하고 오프셋을 적용하지 않았습니다.")

    add_dictionary(ctx, name, source_columns, list(frame.columns), {
        "event_at_parse_status": "원시 생애주기 시각의 파싱 상태.",
        "event_date_recomputed": "원시 시각에서 다시 계산한 날짜.",
        "event_year_month": "월별 집계를 위한 원시 시각 기준 연월.",
        "reason_normalized": "원문을 보존한 채 공백만 정규화한 사유.",
        "withdraw_reason_category_rule_v1": "정확한 사유 문자열 규칙으로 만든 탈퇴 사유 분석 범주.",
        "service_user_profile_match_recalc_flag": "공통 677,085명 사용자 원장 재대조 결과.",
        "user_link_available_recalc_flag": "사용자 연결 가능 여부 재계산값.",
        "user_link_flag_match": "원본과 재계산 사용자 연결 플래그 일치 여부.",
        "identity_link_status": "가입 연결 또는 탈퇴 원천 user_id 부재 상태.",
        "event_analysis_role": "가입 코호트 진입 또는 집계용 미연결 탈퇴 역할.",
        "cohort_entry_eligible_flag": "가입 코호트 시작점 사용 가능 여부.",
        "individual_signup_to_withdraw_path_eligible_flag": "개인별 가입→탈퇴 연결 가능 여부. 현재 전부 0.",
        "aggregate_withdrawal_trend_eligible_flag": "탈퇴 건수·사유 추세 사용 가능 여부.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "전처리에서 적용한 시간 오프셋. 현재 0.",
        "source_observation_scope": "가입 원장과 미연결 탈퇴 원장의 분석 범위.",
    })
    ctx.scope_rows.append({
        "object": name,
        "population": "글로벌 가입 677,085건 + 사용자 연결 불가 탈퇴 70,764건",
        "period": f"{event_at.min()}~{event_at.max()}",
        "time_basis": "DB 원시 시각, 공식 시간대 미확정",
        "grain": "source_table×source_row_id",
    })
    return {
        "rows": len(frame),
        "signup_rows": int(is_signup.sum()),
        "withdraw_rows": int(is_withdraw.sum()),
        "linked_withdraw_rows": forced_withdraw_links,
    }


def process_promo(ctx: Context) -> dict[str, Any]:
    name = "mart_promo_event_receipt_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    frame = read_source(ctx, name)
    source_columns = list(frame.columns)
    to_int(frame, [
        "promo_receipt_id", "promo_event_id", "user_id", "actually_granted_point",
        "orphan_event_definition_flag", "orphan_user_flag",
    ])
    event_at = pd.to_datetime(frame["receipt_created_at"], errors="coerce")
    user_id = pd.to_numeric(frame["user_id"], errors="coerce")
    profile_match = user_id.isin(ctx.profile_user_ids) & user_id.notna()
    orphan_user_recalc = (~profile_match).astype("Int8")
    reason_count = frame.groupby("user_id", dropna=False)["promo_receipt_id"].transform("size")
    user_event_count = frame.groupby(["user_id", "promo_event_id"], dropna=False)["promo_receipt_id"].transform("size")
    user_event_rank = frame.groupby(["user_id", "promo_event_id"], dropna=False).cumcount() + 1

    frame["receipt_at_parse_status"] = np.where(event_at.notna(), "PARSED", "INVALID_OR_MISSING")
    frame["receipt_date_recomputed"] = event_at.dt.strftime("%Y-%m-%d").astype("string")
    frame["receipt_year_month"] = event_at.dt.strftime("%Y-%m").astype("string")
    frame["user_profile_match_recalc_flag"] = profile_match.astype("Int8")
    frame["orphan_user_flag_match"] = (
        pd.to_numeric(frame["orphan_user_flag"], errors="coerce").fillna(-1).astype("int64")
        == orphan_user_recalc.astype("int64")
    ).astype("Int8")
    frame["promo_event_definition_status"] = np.where(
        frame["orphan_event_definition_flag"].fillna(1).eq(0), "CURRENT_DEFINITION_FOUND", "CURRENT_DEFINITION_MISSING"
    )
    amount = pd.to_numeric(frame["actually_granted_point"], errors="coerce")
    frame["grant_amount_status"] = np.select(
        [amount.gt(0), amount.eq(0), amount.lt(0)],
        ["POSITIVE_GRANT", "ZERO_GRANT", "NEGATIVE_GRANT"],
        default="MISSING_GRANT_AMOUNT",
    )
    frame["user_receipt_count"] = reason_count.astype("Int64")
    frame["user_promo_event_receipt_count"] = user_event_count.astype("Int64")
    frame["user_promo_event_receipt_rank"] = user_event_rank.astype("Int64")
    frame["repeated_user_promo_event_receipt_flag"] = user_event_count.gt(1).astype("Int8")
    frame["receipt_analysis_eligible_flag"] = (
        event_at.notna() & profile_match & frame["orphan_event_definition_flag"].fillna(1).eq(0) & amount.notna()
    ).astype("Int8")
    frame["conversion_denominator_available_flag"] = 0
    frame["time_zone_officially_known_flag"] = 0
    frame["applied_timestamp_offset_minutes"] = 0
    frame["record_grain"] = "PROMO_RECEIPT_SOURCE_ROW"

    write_frame(frame, output_path)
    register_log(ctx, name, ctx.expected_rows[name], len(frame), len(source_columns), len(frame.columns),
                 source_path, output_path, True)

    duplicate_ids = int(frame["promo_receipt_id"].duplicated().sum())
    user_flag_mismatch = int(frame["orphan_user_flag_match"].eq(0).sum())
    invalid_amount = int((amount.isna() | amount.le(0)).sum())
    invalid_scope = int((~frame["source_scope"].eq("GLOBAL_DB_EVENT_RECEIPT")).sum())

    value_path = ctx.repo_root / "data" / "processed" / "hackle_mixed" / "mart_value_event_v2_clean.csv.gz"
    value = pd.read_csv(
        value_path,
        dtype="string",
        usecols=["source_table", "source_row_id", "event_at_raw", "service_user_id", "promo_event_id", "actually_granted_point"],
        keep_default_na=False,
        low_memory=False,
    )
    value = value.loc[value["source_table"].eq("event_receipts")].copy()
    to_int(value, ["source_row_id", "service_user_id", "promo_event_id", "actually_granted_point"])
    recon = frame[["promo_receipt_id", "receipt_created_at", "user_id", "promo_event_id", "actually_granted_point"]].merge(
        value,
        left_on="promo_receipt_id",
        right_on="source_row_id",
        how="outer",
        suffixes=("_promo", "_value"),
        indicator=True,
        validate="one_to_one",
    )
    value_recon_mismatch = int((
        recon["_merge"].ne("both")
        | (pd.to_datetime(recon["receipt_created_at"], errors="coerce") != pd.to_datetime(recon["event_at_raw"], errors="coerce"))
        | recon["user_id"].ne(recon["service_user_id"])
        | recon["promo_event_id_promo"].ne(recon["promo_event_id_value"])
        | recon["actually_granted_point_promo"].ne(recon["actually_granted_point_value"])
    ).sum())

    cumulative_path = ctx.repo_root / "data" / "processed" / "hackle_mixed" / "mart_user_cumulative_state_1y_v2_clean.csv.gz"
    cumulative = pd.read_csv(cumulative_path, usecols=["user_id", "promo_receipt_history_count_global_scope"], dtype="string")
    to_int(cumulative, ["user_id", "promo_receipt_history_count_global_scope"])
    promo_user = frame.groupby("user_id", as_index=False).size().rename(columns={"size": "promo_count_recalc"})
    cumulative_recon = cumulative.merge(promo_user, on="user_id", how="left", validate="one_to_one")
    promo_cumulative_mismatch = int((
        cumulative_recon["promo_receipt_history_count_global_scope"].fillna(0)
        != cumulative_recon["promo_count_recalc"].fillna(0)
    ).sum())

    add_qa(ctx, name, "promo_receipt_id_duplicate_rows", duplicate_ids, 0,
           "PASS" if duplicate_ids == 0 else "FAIL", "CRITICAL", "프로모션 지급 ID는 고유해야 합니다.")
    add_qa(ctx, name, "timestamp_parse_fail_rows", int(event_at.isna().sum()), 0,
           "PASS" if event_at.notna().all() else "FAIL", "CRITICAL", "모든 지급 시각은 파싱 가능해야 합니다.")
    add_qa(ctx, name, "orphan_user_flag_mismatch_rows", user_flag_mismatch, 0,
           "PASS" if user_flag_mismatch == 0 else "FAIL", "CRITICAL", "고아 사용자 플래그가 공통 사용자 원장과 일치해야 합니다.")
    add_qa(ctx, name, "orphan_event_definition_rows", int(frame["orphan_event_definition_flag"].fillna(1).eq(1).sum()), 0,
           "PASS" if frame["orphan_event_definition_flag"].fillna(1).eq(0).all() else "WARN", "DATA_QUALITY", "현재 프로모션 정의가 없는 지급을 표시합니다.")
    add_qa(ctx, name, "nonpositive_or_missing_grant_rows", invalid_amount, 0,
           "PASS" if invalid_amount == 0 else "WARN", "DATA_QUALITY", "지급량이 양수인지 확인합니다.")
    add_qa(ctx, name, "source_scope_mismatch_rows", invalid_scope, 0,
           "PASS" if invalid_scope == 0 else "FAIL", "CRITICAL", "프로모션 지급 범위 라벨이 일관되어야 합니다.")
    add_qa(ctx, name, "value_event_reconciliation_mismatch_rows", value_recon_mismatch, 0,
           "PASS" if value_recon_mismatch == 0 else "FAIL", "CRITICAL", "기존 가치 행동 원장의 프로모션 행과 값까지 일치해야 합니다.")
    add_qa(ctx, name, "cumulative_user_count_reconciliation_mismatch_rows", promo_cumulative_mismatch, 0,
           "PASS" if promo_cumulative_mismatch == 0 else "FAIL", "CRITICAL", "사용자 누적상태의 프로모션 횟수와 일치해야 합니다.")
    add_qa(ctx, name, "repeated_recipient_excess_rows", len(frame) - frame["user_id"].nunique(), ">=0",
           "PASS", "INFO", "동일 사용자 다회 지급은 삭제하지 않고 횟수와 순위를 표시합니다.")
    add_qa(ctx, name, "conversion_denominator_unavailable_rows", len(frame), "원천 상태 보존",
           "WARN", "SOURCE_LIMITATION", "지급 대상 노출·참여 분모가 없어 전환율을 계산할 수 없습니다.")
    add_qa(ctx, name, "official_timezone_unknown", 0, 1,
           "WARN", "SOURCE_LIMITATION", "공식 시간대가 없어 원시 시각을 보존하고 오프셋을 적용하지 않았습니다.")

    add_dictionary(ctx, name, source_columns, list(frame.columns), {
        "receipt_at_parse_status": "원시 지급 시각 파싱 상태.",
        "receipt_date_recomputed": "원시 시각에서 다시 계산한 지급 날짜.",
        "receipt_year_month": "월별 지급 집계를 위한 연월.",
        "user_profile_match_recalc_flag": "공통 사용자 원장 재대조 결과.",
        "orphan_user_flag_match": "원본 고아 사용자 플래그와 재계산값 일치 여부.",
        "promo_event_definition_status": "현재 프로모션 정의 연결 상태.",
        "grant_amount_status": "양수·0·음수·결측 지급량 구분.",
        "user_receipt_count": "사용자의 전체 프로모션 지급 원행 수.",
        "user_promo_event_receipt_count": "사용자×프로모션 정의별 지급 원행 수.",
        "user_promo_event_receipt_rank": "사용자×프로모션 정의 안의 원행 순번.",
        "repeated_user_promo_event_receipt_flag": "같은 사용자·프로모션의 다회 지급 여부.",
        "receipt_analysis_eligible_flag": "시각·사용자·이벤트 정의·지급량이 분석 가능한지.",
        "conversion_denominator_available_flag": "노출·참여 전환율 분모 존재 여부. 현재 0.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "전처리에서 적용한 시간 오프셋. 현재 0.",
        "record_grain": "프로모션 지급 원본 1건이라는 행 단위.",
    })
    ctx.scope_rows.append({
        "object": name,
        "population": "글로벌 DB 프로모션 지급 원행",
        "period": f"{event_at.min()}~{event_at.max()}",
        "time_basis": "DB 원시 시각, 공식 시간대 미확정",
        "grain": "promo_receipt_id 1행",
    })
    return {
        "rows": len(frame),
        "users": int(frame["user_id"].nunique()),
        "promo_events": int(frame["promo_event_id"].nunique()),
        "granted_points": int(amount.fillna(0).sum()),
    }


SAFETY_REASON_DOMAIN = {
    "그냥 싫어": "PERSONAL_PREFERENCE",
    "나랑 맞지 않는 질문인 것 같음": "PERSONAL_PREFERENCE",
    "불쾌한 질문 내용": "CONTENT_SAFETY",
    "자꾸 같은 내용의 질문 반복": "CONTENT_REPETITION",
    "어떻게 이런 생각을? 이 질문 최고!": "POSITIVE_FEEDBACK",
    "한 친구가 질문을 반복적으로 보냄": "RELATIONSHIP_FREQUENCY",
    "기타": "OTHER",
    "이 질문은 재미없어요": "CONTENT_QUALITY",
    "불쾌한 내용이 포함되어 있음": "CONTENT_SAFETY",
    "오타가 있음": "CONTENT_QUALITY",
    "선정적이거나 자극적인 질문": "CONTENT_SAFETY",
    "모르는 사람임": "RELATIONSHIP_MISMATCH",
    "친구 사이가 어색해짐": "RELATIONSHIP_MISMATCH",
    "사칭 계정": "ACCOUNT_INTEGRITY",
    "나랑 관련 없는 질문을 자꾸 보냄": "RELATIONSHIP_RELEVANCE",
    "너무 많은 양의 질문을 보냄": "RELATIONSHIP_FREQUENCY",
    "그냥...": "OTHER",
    "허위 사실 언급": "MISINFORMATION",
    "친구를 비하하거나 조롱하는 어투": "HARASSMENT",
    "선정적이거나 폭력적인 내용": "CONTENT_SAFETY",
    "타인을 사칭함": "ACCOUNT_INTEGRITY",
    "광고": "SPAM",
}


def process_safety(ctx: Context) -> dict[str, Any]:
    name = "mart_safety_event_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    frame = read_source(ctx, name)
    source_columns = list(frame.columns)
    int_columns = [
        "source_row_id", "actor_user_id", "target_user_id", "question_id", "user_question_record_id",
        "source_row_weight", "report_count_weight", "is_true_event_time_flag", "actor_user_match_flag",
        "target_user_match_flag", "denominator_available_flag",
    ]
    to_int(frame, int_columns)
    event_at = pd.to_datetime(frame["event_at_raw"], errors="coerce")
    actor = pd.to_numeric(frame["actor_user_id"], errors="coerce")
    target = pd.to_numeric(frame["target_user_id"], errors="coerce")
    actor_match = actor.isin(ctx.profile_user_ids) & actor.notna()
    target_match = target.isin(ctx.profile_user_ids) & target.notna()
    reason = normalize_text(frame["reason_raw"])

    reason_domain = reason.map(SAFETY_REASON_DOMAIN).astype("string")
    is_ping_snapshot = frame["record_type"].eq("PING_REPORT_COUNT_SNAPSHOT")
    reason_domain = reason_domain.mask(is_ping_snapshot, "PING_REPORT_SNAPSHOT")
    reason_domain = reason_domain.fillna("UNMAPPED")
    potential_safety_domains = {"CONTENT_SAFETY", "ACCOUNT_INTEGRITY", "MISINFORMATION", "HARASSMENT", "SPAM"}
    product_domains = {
        "PERSONAL_PREFERENCE", "CONTENT_REPETITION", "CONTENT_QUALITY", "RELATIONSHIP_FREQUENCY",
        "RELATIONSHIP_MISMATCH", "RELATIONSHIP_RELEVANCE",
    }
    interpretation = np.select(
        [
            reason_domain.isin(potential_safety_domains),
            reason_domain.isin(product_domains),
            reason_domain.eq("POSITIVE_FEEDBACK"),
            reason_domain.eq("PING_REPORT_SNAPSHOT"),
            reason_domain.eq("OTHER"),
        ],
        ["POTENTIAL_SAFETY", "PRODUCT_OR_RELATIONSHIP_FEEDBACK", "POSITIVE_FEEDBACK", "REPORT_COUNT_SNAPSHOT", "OTHER"],
        default="UNMAPPED",
    )

    question_ids_path = ctx.repo_root / "data" / "processed" / "question_candidate" / "mart_question_piece_record_v2_clean.csv.gz"
    q = pd.read_csv(question_ids_path, usecols=["question_id"], dtype="string")
    question_ids = set(pd.to_numeric(q["question_id"], errors="coerce").dropna().astype("int64").tolist())
    del q
    vote_path = ctx.repo_root / "data" / "processed" / "question_candidate" / "mart_vote_record_v2_clean.csv.gz"
    vote = pd.read_csv(vote_path, usecols=["user_question_record_id"], dtype="string")
    vote_ids = set(pd.to_numeric(vote["user_question_record_id"], errors="coerce").dropna().astype("int64").tolist())
    del vote
    question_id = pd.to_numeric(frame["question_id"], errors="coerce")
    uqr_id = pd.to_numeric(frame["user_question_record_id"], errors="coerce")
    question_match = question_id.isin(question_ids) & question_id.notna()
    uqr_match = uqr_id.isin(vote_ids) & uqr_id.notna()

    signature = [
        "record_type", "event_at_raw", "actor_user_id", "target_user_id", "question_id",
        "user_question_record_id", "reason_raw", "source_row_weight", "report_count_weight", "is_true_event_time_flag",
    ]
    signature_group = frame.groupby(signature, dropna=False, sort=False)
    semantic_count = signature_group["source_row_id"].transform("size")
    semantic_rank = signature_group.cumcount() + 1

    frame["event_at_parse_status"] = np.where(event_at.notna(), "PARSED", "INVALID_OR_MISSING")
    frame["event_date_recomputed"] = event_at.dt.strftime("%Y-%m-%d").astype("string")
    frame["event_year_month"] = event_at.dt.strftime("%Y-%m").astype("string")
    frame["reason_normalized"] = reason
    frame["feedback_domain_rule_v1"] = reason_domain
    frame["safety_interpretation_rule_v1"] = interpretation
    frame["potential_safety_related_rule_flag"] = reason_domain.isin(potential_safety_domains).astype("Int8")
    frame["classification_rule_version"] = "SAFETY_REASON_RULE_V1"
    frame["harm_confirmation_status"] = "NOT_DETERMINABLE_FROM_REASON_CODE"
    frame["actor_user_profile_match_recalc_flag"] = actor_match.astype("Int8")
    frame["target_user_profile_match_recalc_flag"] = pd.Series(pd.NA, index=frame.index, dtype="Int8")
    frame.loc[target.notna(), "target_user_profile_match_recalc_flag"] = target_match.loc[target.notna()].astype("Int8")
    frame["actor_match_flag_matches_source"] = (
        frame["actor_user_match_flag"].fillna(-1).astype("int64") == actor_match.astype("int64")
    ).astype("Int8")
    frame["target_match_flag_matches_source"] = pd.Series(pd.NA, index=frame.index, dtype="Int8")
    frame.loc[target.notna(), "target_match_flag_matches_source"] = (
        frame.loc[target.notna(), "target_user_match_flag"].fillna(-1).astype("int64")
        == target_match.loc[target.notna()].astype("int64")
    ).astype("Int8")
    frame["question_id_current_catalog_match_flag"] = pd.Series(pd.NA, index=frame.index, dtype="Int8")
    frame.loc[question_id.notna(), "question_id_current_catalog_match_flag"] = question_match.loc[question_id.notna()].astype("Int8")
    frame["user_question_record_match_flag"] = pd.Series(pd.NA, index=frame.index, dtype="Int8")
    frame.loc[uqr_id.notna(), "user_question_record_match_flag"] = uqr_match.loc[uqr_id.notna()].astype("Int8")
    frame["self_target_candidate_flag"] = (actor.notna() & target.notna() & actor.eq(target)).astype("Int8")
    frame["semantic_duplicate_candidate_count"] = semantic_count.astype("Int64")
    frame["semantic_duplicate_candidate_rank"] = semantic_rank.astype("Int64")
    frame["semantic_duplicate_candidate_flag"] = semantic_count.gt(1).astype("Int8")
    frame["canonical_semantic_record_flag"] = semantic_rank.eq(1).astype("Int8")
    frame["event_time_interpretation"] = np.where(
        is_ping_snapshot, "UQR_CREATED_AT_NOT_REPORT_OCCURRED_AT", "TRUE_SOURCE_EVENT_TIME"
    )
    frame["preferred_measurement_column"] = np.where(
        is_ping_snapshot, "report_count_weight", "source_row_weight"
    )
    frame["rate_denominator_status"] = "UNAVAILABLE"
    frame["source_observation_scope"] = frame["record_type"].map({
        "QUESTION_FEEDBACK_OR_REPORT": "GLOBAL_DB_QUESTION_FEEDBACK",
        "USER_BLOCK": "GLOBAL_DB_USER_BLOCK",
        "TIMELINE_REPORT": "GLOBAL_DB_TIMELINE_REPORT",
        "PING_REPORT_COUNT_SNAPSHOT": "GLOBAL_DB_PING_REPORT_COUNT_CURRENT_SNAPSHOT",
    }).astype("string")
    frame["time_zone_officially_known_flag"] = 0
    frame["applied_timestamp_offset_minutes"] = 0

    write_frame(frame, output_path)
    register_log(ctx, name, ctx.expected_rows[name], len(frame), len(source_columns), len(frame.columns),
                 source_path, output_path, True)

    expected_source_type = {
        "polls_questionreport": "QUESTION_FEEDBACK_OR_REPORT",
        "accounts_blockrecord": "USER_BLOCK",
        "accounts_timelinereport": "TIMELINE_REPORT",
        "accounts_userquestionrecord": "PING_REPORT_COUNT_SNAPSHOT",
    }
    mapping_mismatch = int((frame["source_table"].map(expected_source_type) != frame["record_type"]).sum())
    true_time_expected = (~is_ping_snapshot).astype("int64")
    true_time_mismatch = int((frame["is_true_event_time_flag"].fillna(-1).astype("int64") != true_time_expected).sum())
    actor_flag_mismatch = int(frame["actor_match_flag_matches_source"].eq(0).sum())
    target_flag_mismatch = int(frame["target_match_flag_matches_source"].eq(0).fillna(False).sum())
    question_nonmatch = int((question_id.notna() & ~question_match).sum())
    uqr_nonmatch = int((uqr_id.notna() & ~uqr_match).sum())
    semantic_excess = int(frame.duplicated(signature).sum())
    semantic_group_rows = int(semantic_count.gt(1).sum())
    self_target_rows = int(frame["self_target_candidate_flag"].eq(1).sum())
    unmapped_rows = int(frame["feedback_domain_rule_v1"].eq("UNMAPPED").sum())
    ping_rows = int(is_ping_snapshot.sum())
    ping_report_weight = int(frame.loc[is_ping_snapshot, "report_count_weight"].fillna(0).sum())

    cumulative_path = ctx.repo_root / "data" / "processed" / "hackle_mixed" / "mart_user_cumulative_state_1y_v2_clean.csv.gz"
    cumulative = pd.read_csv(
        cumulative_path,
        usecols=["user_id", "safety_or_feedback_actor_record_count_global_scope", "safety_or_feedback_actor_weight_global_scope"],
        dtype="string",
    )
    to_int(cumulative, ["user_id", "safety_or_feedback_actor_record_count_global_scope", "safety_or_feedback_actor_weight_global_scope"])
    actor_agg = frame.groupby("actor_user_id", as_index=False).agg(
        actor_record_count_recalc=("source_row_weight", "sum"),
        actor_weight_recalc=("report_count_weight", "sum"),
    ).rename(columns={"actor_user_id": "user_id"})
    cumulative_recon = cumulative.merge(actor_agg, on="user_id", how="left", validate="one_to_one")
    count_mismatch = int((
        cumulative_recon["safety_or_feedback_actor_record_count_global_scope"].fillna(0)
        != cumulative_recon["actor_record_count_recalc"].fillna(0)
    ).sum())
    weight_mismatch = int((
        cumulative_recon["safety_or_feedback_actor_weight_global_scope"].fillna(0)
        != cumulative_recon["actor_weight_recalc"].fillna(0)
    ).sum())

    add_qa(ctx, name, "source_key_duplicate_rows", int(frame.duplicated(["source_table", "source_row_id"]).sum()), 0,
           "PASS" if not frame.duplicated(["source_table", "source_row_id"]).any() else "FAIL", "CRITICAL", "원천별 사건 ID는 고유해야 합니다.")
    add_qa(ctx, name, "timestamp_parse_fail_rows", int(event_at.isna().sum()), 0,
           "PASS" if event_at.notna().all() else "FAIL", "CRITICAL", "모든 원시 시각은 파싱 가능해야 합니다.")
    add_qa(ctx, name, "source_record_type_mapping_mismatch_rows", mapping_mismatch, 0,
           "PASS" if mapping_mismatch == 0 else "FAIL", "CRITICAL", "원천 테이블과 record_type이 일치해야 합니다.")
    add_qa(ctx, name, "source_row_weight_invalid_rows", int(frame["source_row_weight"].fillna(0).ne(1).sum()), 0,
           "PASS" if frame["source_row_weight"].fillna(0).eq(1).all() else "FAIL", "CRITICAL", "모든 원천 행의 row weight는 1이어야 합니다.")
    add_qa(ctx, name, "report_count_weight_nonpositive_rows", int(frame["report_count_weight"].fillna(0).le(0).sum()), 0,
           "PASS" if frame["report_count_weight"].fillna(0).gt(0).all() else "FAIL", "CRITICAL", "신고 가중치는 양수여야 합니다.")
    add_qa(ctx, name, "true_event_time_definition_mismatch_rows", true_time_mismatch, 0,
           "PASS" if true_time_mismatch == 0 else "FAIL", "CRITICAL", "Ping 누적 스냅샷만 실제 신고시각이 아니어야 합니다.")
    add_qa(ctx, name, "denominator_available_flag_nonzero_rows", int(frame["denominator_available_flag"].fillna(0).ne(0).sum()), 0,
           "PASS" if frame["denominator_available_flag"].fillna(0).eq(0).all() else "FAIL", "CRITICAL", "현재 마트에는 신고율 분모가 없습니다.")
    add_qa(ctx, name, "actor_match_flag_mismatch_rows", actor_flag_mismatch, 0,
           "PASS" if actor_flag_mismatch == 0 else "FAIL", "CRITICAL", "actor 연결 플래그가 공통 사용자 기준과 일치해야 합니다.")
    add_qa(ctx, name, "target_match_flag_mismatch_rows", target_flag_mismatch, 0,
           "PASS" if target_flag_mismatch == 0 else "FAIL", "CRITICAL", "target 연결 플래그가 공통 사용자 기준과 일치해야 합니다.")
    add_qa(ctx, name, "unmapped_reason_rows", unmapped_rows, 0,
           "PASS" if unmapped_rows == 0 else "WARN", "DATA_QUALITY", "사유 규칙에서 빠진 값이 없어야 합니다.")
    add_qa(ctx, name, "question_id_not_in_current_catalog_rows", question_nonmatch, "원천 상태 보존 및 플래그",
           "WARN" if question_nonmatch else "PASS", "SOURCE_LIMITATION", "현재 질문 카탈로그에서 찾지 못한 과거 질문 ID를 보존합니다.")
    add_qa(ctx, name, "user_question_record_not_in_vote_mart_rows", uqr_nonmatch, 0,
           "PASS" if uqr_nonmatch == 0 else "FAIL", "CRITICAL", "참조된 UQR은 투표 원장과 연결되어야 합니다.")
    add_qa(ctx, name, "ping_snapshot_row_and_weight", f"{ping_rows} rows / {ping_report_weight} reports", "169 rows / 215 reports",
           "PASS" if ping_rows == 169 and ping_report_weight == 215 else "FAIL", "CRITICAL", "Ping 스냅샷 원행 수와 누적 신고 수를 분리합니다.")
    add_qa(ctx, name, "semantic_duplicate_candidate_excess_rows", semantic_excess, "원천 상태 보존 및 플래그",
           "WARN" if semantic_excess else "PASS", "REVIEW", "의미상 동일 후보는 삭제하지 않고 순위·대표행 플래그를 제공합니다.")
    add_qa(ctx, name, "semantic_duplicate_candidate_group_rows", semantic_group_rows, ">=0", "PASS", "INFO", "중복 후보 그룹에 포함된 전체 행 수입니다.")
    add_qa(ctx, name, "self_target_candidate_rows", self_target_rows, "원천 상태 보존 및 플래그",
           "WARN" if self_target_rows else "PASS", "REVIEW", "actor와 target이 같은 원천 행을 임의 삭제하지 않습니다.")
    add_qa(ctx, name, "cumulative_actor_record_count_mismatch_users", count_mismatch, 0,
           "PASS" if count_mismatch == 0 else "FAIL", "CRITICAL", "사용자 누적상태의 안전 원행 수와 일치해야 합니다.")
    add_qa(ctx, name, "cumulative_actor_weight_mismatch_users", weight_mismatch, 0,
           "PASS" if weight_mismatch == 0 else "FAIL", "CRITICAL", "사용자 누적상태의 신고 가중치와 일치해야 합니다.")
    add_qa(ctx, name, "rate_denominator_unavailable_rows", len(frame), "원천 상태 보존",
           "WARN", "SOURCE_LIMITATION", "노출·수신·활성 사용자 분모가 없어 신고율·차단율을 계산할 수 없습니다.")
    add_qa(ctx, name, "ping_report_time_unavailable_rows", ping_rows, "원천 상태 보존",
           "WARN", "SOURCE_LIMITATION", "Ping 스냅샷의 event_at은 UQR 생성시각이며 신고 발생시각이 아닙니다.")
    add_qa(ctx, name, "official_timezone_unknown", 0, 1,
           "WARN", "SOURCE_LIMITATION", "공식 시간대가 없어 원시 시각을 보존하고 오프셋을 적용하지 않았습니다.")

    add_dictionary(ctx, name, source_columns, list(frame.columns), {
        "event_at_parse_status": "원시 시각 파싱 상태.",
        "event_date_recomputed": "원시 시각에서 다시 계산한 날짜.",
        "event_year_month": "월별 집계를 위한 원시 시각 기준 연월.",
        "reason_normalized": "원문을 보존한 채 공백만 정규화한 사유.",
        "feedback_domain_rule_v1": "정확한 사유 문자열에 기반한 상세 피드백 범주.",
        "safety_interpretation_rule_v1": "제품·관계 피드백, 잠재 안전, 긍정, 스냅샷 구분. 확정 유해 판정이 아님.",
        "potential_safety_related_rule_flag": "규칙상 잠재 안전 관련 사유 여부. 확정 판정이 아님.",
        "classification_rule_version": "사유 분류 규칙 버전.",
        "harm_confirmation_status": "사유 코드만으로 실제 피해를 확정할 수 없다는 상태.",
        "actor_user_profile_match_recalc_flag": "actor를 공통 사용자 원장과 재대조한 결과.",
        "target_user_profile_match_recalc_flag": "target이 있을 때 공통 사용자 원장 재대조 결과.",
        "actor_match_flag_matches_source": "원본 actor match 플래그와 재계산값 일치 여부.",
        "target_match_flag_matches_source": "원본 target match 플래그와 재계산값 일치 여부.",
        "question_id_current_catalog_match_flag": "현재 질문조각 카탈로그와 question_id 연결 여부.",
        "user_question_record_match_flag": "투표/UQR 원장과 user_question_record_id 연결 여부.",
        "self_target_candidate_flag": "actor와 target이 같은 검토 후보.",
        "semantic_duplicate_candidate_count": "의미 서명이 같은 원천 행 수.",
        "semantic_duplicate_candidate_rank": "의미 서명 안의 원행 순번.",
        "semantic_duplicate_candidate_flag": "의미상 동일 후보 그룹 여부.",
        "canonical_semantic_record_flag": "민감도 분석용 의미 서명 대표행. 원천 삭제 근거가 아님.",
        "event_time_interpretation": "실제 사건시각 또는 UQR 생성시각 구분.",
        "preferred_measurement_column": "원행 수 또는 누적 report_count 중 권장 측정 컬럼.",
        "rate_denominator_status": "신고율·차단율 분모 존재 상태. 현재 UNAVAILABLE.",
        "source_observation_scope": "record_type별 실제 관측 범위.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "전처리에서 적용한 시간 오프셋. 현재 0.",
    })
    ctx.scope_rows.append({
        "object": name,
        "population": "글로벌 DB 질문 피드백·차단·타임라인 신고·Ping 누적 스냅샷",
        "period": "원천별 상이: 2023-04-19~2024-05-06",
        "time_basis": "Ping 스냅샷 제외 실제 원천시각; 공식 시간대 미확정",
        "grain": "source_table×source_row_id",
    })
    return {
        "rows": len(frame),
        "actors": int(frame["actor_user_id"].nunique()),
        "source_row_weight_sum": int(frame["source_row_weight"].fillna(0).sum()),
        "report_count_weight_sum": int(frame["report_count_weight"].fillna(0).sum()),
        "question_catalog_missing_rows": question_nonmatch,
        "semantic_duplicate_excess_rows": semantic_excess,
        "self_target_rows": self_target_rows,
    }


def process_attendance(ctx: Context) -> dict[str, Any]:
    name = "mart_attendance_record_v2"
    bridge_name = "bridge_attendance_day_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    bridge_output = ctx.output_dir / f"{bridge_name}_clean.csv.gz"
    frame = read_source(ctx, name)
    source_columns = list(frame.columns)
    to_int(frame, [
        "attendance_record_id", "user_id", "attendance_json_valid_flag",
        "attendance_list_length", "orphan_user_flag",
    ])

    parsed_lists: list[list[Any]] = []
    json_valid: list[int] = []
    array_valid: list[int] = []
    recalculated_length: list[int | None] = []
    for raw in frame["attendance_date_list_json"].tolist():
        try:
            value = json.loads(str(raw))
            valid = 1
        except Exception:
            value = None
            valid = 0
        is_array = int(isinstance(value, list))
        parsed_lists.append(value if is_array else [])
        json_valid.append(valid)
        array_valid.append(is_array)
        recalculated_length.append(len(value) if is_array else None)

    user_id = pd.to_numeric(frame["user_id"], errors="coerce")
    profile_match = user_id.isin(ctx.profile_user_ids) & user_id.notna()
    orphan_recalc = (~profile_match).astype("Int8")
    frame["attendance_json_valid_recalc_flag"] = pd.Series(json_valid, dtype="Int8")
    frame["attendance_array_valid_recalc_flag"] = pd.Series(array_valid, dtype="Int8")
    frame["attendance_list_length_recalc"] = pd.Series(recalculated_length, dtype="Int64")
    frame["attendance_json_flag_match"] = (
        frame["attendance_json_valid_flag"].fillna(-1).astype("int64")
        == frame["attendance_json_valid_recalc_flag"].fillna(-2).astype("int64")
    ).astype("Int8")
    frame["attendance_list_length_match_flag"] = (
        frame["attendance_list_length"].fillna(-1).astype("int64")
        == frame["attendance_list_length_recalc"].fillna(-2).astype("int64")
    ).astype("Int8")
    frame["user_profile_match_recalc_flag"] = profile_match.astype("Int8")
    frame["orphan_user_flag_match"] = (
        frame["orphan_user_flag"].fillna(-1).astype("int64") == orphan_recalc.astype("int64")
    ).astype("Int8")
    frame["empty_attendance_list_flag"] = frame["attendance_list_length_recalc"].fillna(0).eq(0).astype("Int8")
    frame["attendance_day_expansion_eligible_flag"] = (
        frame["attendance_json_valid_recalc_flag"].eq(1)
        & frame["attendance_array_valid_recalc_flag"].eq(1)
        & profile_match
    ).astype("Int8")
    frame["record_grain"] = "ATTENDANCE_JSON_RECORD_NOT_SINGLE_DAY"
    frame["source_observation_scope"] = "GLOBAL_DB_ATTENDANCE_RECORD"

    bridge_columns = [
        "attendance_record_id", "user_id", "attendance_ordinal", "attendance_date_text_raw",
        "attendance_date", "attendance_date_parse_status", "attendance_year_month",
        "same_record_date_occurrence_rank", "same_record_date_occurrence_count",
        "same_user_date_occurrence_rank", "same_user_date_occurrence_count",
        "canonical_user_date_flag", "user_profile_match_flag", "signup_date",
        "days_since_signup", "pre_signup_attendance_flag", "attendance_day_analysis_eligible_flag",
        "source_observation_scope",
    ]
    bridge_rows = invalid_date_rows = duplicate_date_excess = duplicate_group_rows = 0
    pre_signup_rows = 0
    bridge_buffer: list[dict[str, Any]] = []
    first = True
    with gzip.open(bridge_output, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        for row_number, (row, values) in enumerate(zip(frame.itertuples(index=False), parsed_lists), start=1):
            text_values = [str(value).strip() for value in values]
            counts = Counter(text_values)
            seen: dict[str, int] = defaultdict(int)
            uid = int(row.user_id) if pd.notna(row.user_id) else None
            signup_date = ctx.signup_date_by_user.get(uid) if uid is not None else None
            for ordinal, text_value in enumerate(text_values, start=1):
                seen[text_value] += 1
                occurrence_rank = seen[text_value]
                occurrence_count = counts[text_value]
                try:
                    parsed_date = date.fromisoformat(text_value)
                    parse_status = "PARSED"
                except Exception:
                    parsed_date = None
                    parse_status = "INVALID_OR_MISSING"
                if parsed_date is None:
                    invalid_date_rows += 1
                days_since_signup = (
                    (parsed_date - signup_date).days
                    if parsed_date is not None and signup_date is not None else None
                )
                pre_signup = int(days_since_signup is not None and days_since_signup < 0)
                pre_signup_rows += pre_signup
                duplicate_date_excess += int(occurrence_rank > 1)
                duplicate_group_rows += int(occurrence_count > 1)
                canonical = int(parsed_date is not None and occurrence_rank == 1)
                bridge_buffer.append({
                    "attendance_record_id": row.attendance_record_id,
                    "user_id": row.user_id,
                    "attendance_ordinal": ordinal,
                    "attendance_date_text_raw": text_value,
                    "attendance_date": parsed_date.isoformat() if parsed_date else None,
                    "attendance_date_parse_status": parse_status,
                    "attendance_year_month": parsed_date.strftime("%Y-%m") if parsed_date else None,
                    "same_record_date_occurrence_rank": occurrence_rank,
                    "same_record_date_occurrence_count": occurrence_count,
                    "same_user_date_occurrence_rank": occurrence_rank,
                    "same_user_date_occurrence_count": occurrence_count,
                    "canonical_user_date_flag": canonical,
                    "user_profile_match_flag": int(uid in ctx.profile_user_ids) if uid is not None else 0,
                    "signup_date": signup_date.isoformat() if signup_date else None,
                    "days_since_signup": days_since_signup,
                    "pre_signup_attendance_flag": pre_signup,
                    "attendance_day_analysis_eligible_flag": int(canonical == 1 and uid in ctx.profile_user_ids),
                    "source_observation_scope": "GLOBAL_DB_ATTENDANCE_DATE_ELEMENT",
                })
                bridge_rows += 1
                if len(bridge_buffer) >= CHUNK_SIZE:
                    pd.DataFrame(bridge_buffer, columns=bridge_columns).to_csv(
                        handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d"
                    )
                    first = False
                    bridge_buffer.clear()
            if row_number % 75_000 == 0:
                print(f"  출석 원행 {row_number:,}개 / 날짜 원소 {bridge_rows:,}개 처리", flush=True)
        if bridge_buffer:
            pd.DataFrame(bridge_buffer, columns=bridge_columns).to_csv(
                handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d"
            )
            bridge_buffer.clear()

    write_frame(frame, output_path)
    register_log(ctx, name, ctx.expected_rows[name], len(frame), len(source_columns), len(frame.columns),
                 source_path, output_path, True)
    register_log(ctx, bridge_name, int(frame["attendance_list_length_recalc"].fillna(0).sum()), bridge_rows,
                 len(source_columns), len(bridge_columns), source_path, bridge_output, "DERIVED_GRAIN")

    duplicate_record_ids = int(frame["attendance_record_id"].duplicated().sum())
    duplicate_user_rows = int(frame["user_id"].duplicated().sum())
    json_flag_mismatch = int(frame["attendance_json_flag_match"].eq(0).sum())
    list_length_mismatch = int(frame["attendance_list_length_match_flag"].eq(0).sum())
    orphan_flag_mismatch = int(frame["orphan_user_flag_match"].eq(0).sum())
    expected_bridge_rows = int(frame["attendance_list_length_recalc"].fillna(0).sum())

    activity_path = ctx.repo_root / "data" / "processed" / "hackle_mixed" / "mart_user_activity_daily_v2_clean.csv.gz"
    con = duckdb.connect(database=":memory:")
    qbridge = str(bridge_output).replace("\\", "/").replace("'", "''")
    qactivity = str(activity_path).replace("\\", "/").replace("'", "''")
    attendance_daily_mismatch = int(con.execute(f"""
        WITH b AS (
            SELECT CAST(user_id AS BIGINT) AS user_id,
                   CAST(attendance_date AS DATE) AS activity_date,
                   COUNT(*) AS raw_count,
                   COUNT(DISTINCT attendance_record_id) AS record_count
            FROM read_csv_auto('{qbridge}', header=true)
            WHERE attendance_date IS NOT NULL
            GROUP BY 1,2
        ),
        a AS (
            SELECT CAST(user_id AS BIGINT) AS user_id,
                   CAST(activity_date AS DATE) AS activity_date,
                   CAST(attendance_raw_element_count AS BIGINT) AS raw_count,
                   CAST(attendance_record_count AS BIGINT) AS record_count
            FROM read_csv_auto('{qactivity}', header=true)
            WHERE CAST(attendance_raw_element_count AS BIGINT) > 0
               OR CAST(attendance_record_count AS BIGINT) > 0
        )
        SELECT COUNT(*)
        FROM b FULL OUTER JOIN a USING (user_id, activity_date)
        WHERE COALESCE(b.raw_count,0) <> COALESCE(a.raw_count,0)
           OR COALESCE(b.record_count,0) <> COALESCE(a.record_count,0)
    """).fetchone()[0])
    con.close()

    cumulative_path = ctx.repo_root / "data" / "processed" / "hackle_mixed" / "mart_user_cumulative_state_1y_v2_clean.csv.gz"
    cumulative = pd.read_csv(
        cumulative_path,
        usecols=["user_id", "attendance_distinct_day_count", "attendance_raw_element_count"],
        dtype="string",
    )
    to_int(cumulative, ["user_id", "attendance_distinct_day_count", "attendance_raw_element_count"])
    attendance_user = frame[["user_id", "attendance_list_length_recalc"]].copy()
    attendance_user["attendance_distinct_day_count_recalc"] = [len(set(str(v) for v in values)) for values in parsed_lists]
    attendance_user = attendance_user.rename(columns={"attendance_list_length_recalc": "attendance_raw_element_count_recalc"})
    cumulative_recon = cumulative.merge(attendance_user, on="user_id", how="left", validate="one_to_one")
    cumulative_raw_mismatch = int((
        cumulative_recon["attendance_raw_element_count"].fillna(0)
        != cumulative_recon["attendance_raw_element_count_recalc"].fillna(0)
    ).sum())
    cumulative_distinct_mismatch = int((
        cumulative_recon["attendance_distinct_day_count"].fillna(0)
        != cumulative_recon["attendance_distinct_day_count_recalc"].fillna(0)
    ).sum())

    add_qa(ctx, name, "attendance_record_id_duplicate_rows", duplicate_record_ids, 0,
           "PASS" if duplicate_record_ids == 0 else "FAIL", "CRITICAL", "출석 원행 ID는 고유해야 합니다.")
    add_qa(ctx, name, "duplicate_user_record_rows", duplicate_user_rows, 0,
           "PASS" if duplicate_user_rows == 0 else "FAIL", "CRITICAL", "현재 원천은 사용자 1명당 출석 JSON 1행이어야 합니다.")
    add_qa(ctx, name, "json_valid_flag_mismatch_rows", json_flag_mismatch, 0,
           "PASS" if json_flag_mismatch == 0 else "FAIL", "CRITICAL", "원본 JSON 유효성 플래그와 재계산값이 일치해야 합니다.")
    add_qa(ctx, name, "non_array_json_rows", int(frame["attendance_array_valid_recalc_flag"].ne(1).sum()), 0,
           "PASS" if frame["attendance_array_valid_recalc_flag"].eq(1).all() else "FAIL", "CRITICAL", "유효 JSON은 날짜 배열이어야 합니다.")
    add_qa(ctx, name, "list_length_mismatch_rows", list_length_mismatch, 0,
           "PASS" if list_length_mismatch == 0 else "FAIL", "CRITICAL", "원본 배열 길이와 실제 원소 수가 일치해야 합니다.")
    add_qa(ctx, name, "orphan_user_flag_mismatch_rows", orphan_flag_mismatch, 0,
           "PASS" if orphan_flag_mismatch == 0 else "FAIL", "CRITICAL", "고아 사용자 플래그가 공통 사용자 원장과 일치해야 합니다.")
    add_qa(ctx, name, "empty_attendance_list_rows", int(frame["empty_attendance_list_flag"].eq(1).sum()), ">=0",
           "PASS", "INFO", "빈 배열도 유효한 원행으로 보존합니다.")
    add_qa(ctx, bridge_name, "expanded_row_count", bridge_rows, expected_bridge_rows,
           "PASS" if bridge_rows == expected_bridge_rows else "FAIL", "CRITICAL", "JSON 배열 원소 수와 bridge 행 수가 일치해야 합니다.")
    add_qa(ctx, bridge_name, "invalid_attendance_date_rows", invalid_date_rows, 0,
           "PASS" if invalid_date_rows == 0 else "WARN", "DATA_QUALITY", "날짜로 파싱되지 않는 원소를 표시합니다.")
    add_qa(ctx, bridge_name, "duplicate_user_date_excess_rows", duplicate_date_excess, "원천 상태 보존 및 플래그",
           "WARN" if duplicate_date_excess else "PASS", "REVIEW", "같은 사용자·날짜 중복 원소를 삭제하지 않고 순위·대표행을 표시합니다.")
    add_qa(ctx, bridge_name, "duplicate_user_date_group_rows", duplicate_group_rows, ">=0", "PASS", "INFO", "중복 날짜 그룹에 포함된 전체 원소 수입니다.")
    add_qa(ctx, bridge_name, "pre_signup_attendance_rows", pre_signup_rows, "원천 상태 보존 및 플래그",
           "WARN" if pre_signup_rows else "PASS", "SOURCE_LIMITATION", "가입일보다 앞선 출석 날짜를 삭제하지 않고 표시합니다.")
    add_qa(ctx, bridge_name, "activity_daily_reconciliation_mismatch_user_dates", attendance_daily_mismatch, 0,
           "PASS" if attendance_daily_mismatch == 0 else "FAIL", "CRITICAL", "사용자 일별 활동 마트의 출석 두 지표와 일치해야 합니다.")
    add_qa(ctx, bridge_name, "cumulative_raw_count_mismatch_users", cumulative_raw_mismatch, 0,
           "PASS" if cumulative_raw_mismatch == 0 else "FAIL", "CRITICAL", "사용자 누적상태의 출석 원소 수와 일치해야 합니다.")
    add_qa(ctx, bridge_name, "cumulative_distinct_day_mismatch_users", cumulative_distinct_mismatch, 0,
           "PASS" if cumulative_distinct_mismatch == 0 else "FAIL", "CRITICAL", "사용자 누적상태의 서로 다른 출석일 수와 일치해야 합니다.")

    add_dictionary(ctx, name, source_columns, list(frame.columns), {
        "attendance_json_valid_recalc_flag": "Python JSON 파서로 재계산한 유효성.",
        "attendance_array_valid_recalc_flag": "유효 JSON이 배열인지 확인한 값.",
        "attendance_list_length_recalc": "실제 배열 원소 수.",
        "attendance_json_flag_match": "원본 JSON 유효성 플래그와 재계산값 일치 여부.",
        "attendance_list_length_match_flag": "원본 배열 길이와 재계산값 일치 여부.",
        "user_profile_match_recalc_flag": "공통 사용자 원장 재대조 결과.",
        "orphan_user_flag_match": "원본 고아 사용자 플래그와 재계산값 일치 여부.",
        "empty_attendance_list_flag": "유효하지만 날짜 원소가 없는 배열 여부.",
        "attendance_day_expansion_eligible_flag": "날짜 원소 확장에 사용할 수 있는 원행 여부.",
        "record_grain": "원행은 출석 하루가 아니라 사용자별 날짜 JSON 기록임을 표시.",
        "source_observation_scope": "글로벌 DB 출석 원행 범위.",
    })
    add_dictionary(ctx, bridge_name, [], bridge_columns, {
        "attendance_record_id": "출석 JSON 원행 ID.",
        "user_id": "출석 사용자 ID.",
        "attendance_ordinal": "원본 JSON 배열에서의 1부터 시작하는 위치.",
        "attendance_date_text_raw": "배열에 저장된 날짜 원문.",
        "attendance_date": "파싱된 출석 날짜.",
        "attendance_date_parse_status": "날짜 파싱 상태.",
        "attendance_year_month": "월별 집계를 위한 연월.",
        "same_record_date_occurrence_rank": "같은 원행 안 동일 날짜의 순번.",
        "same_record_date_occurrence_count": "같은 원행 안 동일 날짜의 출현 수.",
        "same_user_date_occurrence_rank": "같은 사용자·날짜의 순번. 현재 사용자당 원행 1개이므로 원행 순번과 같음.",
        "same_user_date_occurrence_count": "같은 사용자·날짜 출현 수.",
        "canonical_user_date_flag": "사용자별 서로 다른 출석일 계산에 사용할 대표 원소.",
        "user_profile_match_flag": "공통 사용자 원장 연결 여부.",
        "signup_date": "공통 사용자 원장의 가입일.",
        "days_since_signup": "출석일-가입일.",
        "pre_signup_attendance_flag": "가입일보다 앞선 출석 원소 여부.",
        "attendance_day_analysis_eligible_flag": "유효 날짜·사용자 연결·대표 원소 조건 충족 여부.",
        "source_observation_scope": "글로벌 DB 출석 날짜 원소 범위.",
    })
    ctx.scope_rows.extend([
        {
            "object": name,
            "population": "글로벌 DB 출석 원행 사용자",
            "period": "JSON 원행 스냅샷; 날짜 범위는 bridge에서 확인",
            "time_basis": "날짜 배열",
            "grain": "attendance_record_id 1행",
        },
        {
            "object": bridge_name,
            "population": "유효 출석 JSON 안의 모든 날짜 원소",
            "period": "2023-05-27~2024-05-09",
            "time_basis": "날짜",
            "grain": "attendance_record_id×attendance_ordinal",
        },
    ])
    del parsed_lists
    gc.collect()
    return {
        "record_rows": len(frame),
        "users": int(frame["user_id"].nunique()),
        "empty_list_rows": int(frame["empty_attendance_list_flag"].eq(1).sum()),
        "attendance_day_rows": bridge_rows,
        "invalid_date_rows": invalid_date_rows,
        "pre_signup_rows": pre_signup_rows,
    }


def verify_source_value_preservation(ctx: Context) -> None:
    """Compare every preserved source cell after CSV NULL normalization."""
    for name in SOURCE_NAMES:
        source_path = ctx.source_dir / f"{name}.csv.gz"
        output_path = ctx.output_dir / f"{name}_clean.csv.gz"
        source_header = list(pd.read_csv(source_path, nrows=0, encoding="utf-8-sig").columns)
        source_reader = pd.read_csv(
            source_path, dtype="string", na_values=NA_VALUES, keep_default_na=False,
            chunksize=CHUNK_SIZE, low_memory=False, encoding="utf-8-sig",
        )
        output_reader = pd.read_csv(
            output_path, dtype="string", keep_default_na=False,
            chunksize=CHUNK_SIZE, low_memory=False, encoding="utf-8-sig",
        )
        compared_rows = 0
        cell_mismatches = 0
        for source_chunk, output_chunk in zip(source_reader, output_reader):
            output_chunk = output_chunk[source_header]
            source_normalized = source_chunk.fillna("<NULL>").reset_index(drop=True)
            output_normalized = output_chunk.replace("", pd.NA).fillna("<NULL>").reset_index(drop=True)
            compared_rows += len(source_chunk)
            cell_mismatches += int((source_normalized != output_normalized).sum().sum())
        expected_rows = ctx.expected_rows[name]
        status = "PASS" if cell_mismatches == 0 and compared_rows == expected_rows else "FAIL"
        add_qa(
            ctx, name, "source_column_cell_preservation_mismatches", cell_mismatches, 0,
            status, "CRITICAL",
            f"원본 {len(source_header)}개 컬럼의 {compared_rows:,}행을 전수 비교해 값 보존을 확인합니다.",
        )


def finalize(ctx: Context) -> dict[str, Any]:
    verify_source_value_preservation(ctx)
    qa = pd.DataFrame(ctx.qa_rows)
    processing = pd.DataFrame(ctx.processing_log)
    dictionary = pd.DataFrame(ctx.dictionary_rows).drop_duplicates(["mart", "column_name"], keep="last")
    scope = pd.DataFrame(ctx.scope_rows)

    qa_path = ctx.report_dir / "remaining_four_qa_summary.csv"
    log_path = ctx.report_dir / "remaining_four_processing_log.csv"
    dictionary_path = ctx.report_dir / "remaining_four_column_dictionary.csv"
    scope_path = ctx.report_dir / "remaining_four_scope_summary.csv"
    report_path = ctx.report_dir / "remaining_four_preprocessing_report.md"
    qa.to_csv(qa_path, index=False, encoding="utf-8-sig")
    processing.to_csv(log_path, index=False, encoding="utf-8-sig")
    dictionary.to_csv(dictionary_path, index=False, encoding="utf-8-sig")
    scope.to_csv(scope_path, index=False, encoding="utf-8-sig")

    counts = qa["status"].value_counts().to_dict()
    passed = int(counts.get("PASS", 0))
    warned = int(counts.get("WARN", 0))
    failed = int(counts.get("FAIL", 0))
    report_lines = [
        "# 프로모션·안전·생애주기·출석 마트 전처리 결과",
        "",
        "## 완료 판정",
        "",
        f"- PASS: {passed}건",
        f"- WARN: {warned}건",
        f"- FAIL: {failed}건",
        "- 최종 판정: " + ("전처리 완료" if failed == 0 else "재검토 필요"),
        "",
        "## 공통 기준",
        "",
        "- 공통 사용자 기준: mart_user_acquisition_profile_v2_clean의 677,085명",
        "- 원본 행과 원문을 삭제하지 않고 파싱·연결·해석 플래그만 추가",
        "- 탈퇴 원천은 user_id가 없으므로 개인 가입자에게 강제 연결하지 않음",
        "- 안전 원장은 원행 수와 누적 report_count를 분리하고 신고율 분모 없음으로 표시",
        "- 출석 JSON은 원행과 날짜 원소 bridge를 모두 보존",
        "- DB 시각의 공식 시간대가 확인되지 않아 원시 시각 유지 및 오프셋 0분 적용",
        "",
        "## 정제 산출물",
        "",
    ]
    for row in processing.to_dict("records"):
        report_lines.append(
            f"- `{Path(str(row['output_path'])).name}`: {int(row['output_rows']):,}행, {int(row['output_columns']):,}열"
        )
    report_lines += [
        "",
        "## 주요 원천 한계",
        "",
        "- 프로모션은 지급 기록만 있고 노출·참여 분모가 없어 전환율을 계산할 수 없음",
        "- 안전 원장은 질문 피드백·관계 불만·잠재 안전 사유·Ping 누적 스냅샷이 섞여 있어 라벨별 분리 필요",
        "- 탈퇴 70,764건은 사용자 ID가 없어 개인 이탈 모델 타깃으로 사용할 수 없음",
        "- 출석 원행은 하루가 아니라 날짜 배열이며, 날짜별 분석은 bridge 사용",
    ]
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    if failed:
        raise AssertionError(f"네 번째 묶음 전처리 QA 실패 {failed}건")
    return {
        "qa": qa,
        "processing": processing,
        "dictionary": dictionary,
        "scope": scope,
        "pass": passed,
        "warn": warned,
        "fail": failed,
        "report_path": report_path,
    }
