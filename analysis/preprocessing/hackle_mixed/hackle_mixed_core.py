from __future__ import annotations

import gc
import gzip
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


@dataclass
class Context:
    repo_root: Path
    source_dir: Path
    output_dir: Path
    support_dir: Path
    report_dir: Path
    source_manifest: pd.DataFrame
    expected_rows: dict[str, int]
    qa_rows: list[dict[str, Any]] = field(default_factory=list)
    dictionary_rows: list[dict[str, Any]] = field(default_factory=list)
    processing_log: list[dict[str, Any]] = field(default_factory=list)
    fact_user_daily: pd.DataFrame | None = None
    visit_user_daily: pd.DataFrame | None = None
    hackle_user_daily: pd.DataFrame | None = None
    value_hackle_daily: pd.DataFrame | None = None
    activity_hackle: pd.DataFrame | None = None
    output_summaries: list[dict[str, Any]] = field(default_factory=list)


HACKLE_START = pd.Timestamp("2023-07-18 00:00:00")
HACKLE_END = pd.Timestamp("2023-08-10 23:59:59.999999")
SOURCE_NAMES = [
    "fact_hackle_event_24d_v2",
    "mart_value_event_v2",
    "mart_user_activity_daily_v2",
    "mart_user_cumulative_state_1y_v2",
]
SUPPORT_NAMES = [
    "dim_hackle_event_text_attribute_v2",
    "dim_hackle_device_resolved_v2",
    "dim_hackle_user_resolved_v2",
    "dim_hackle_session_resolved_v2",
    "bridge_hackle_event_visit_assignment_v2",
    "bridge_hackle_event_raw_id_v2",
    "dim_hackle_visit_30m_v2",
]
NA_VALUES = [r"\N"]
CHUNK_SIZE = 100_000


def find_repo_root(start: str | Path | None = None) -> Path:
    start_path = Path(start or Path.cwd()).resolve()
    for candidate in (start_path, *start_path.parents):
        if (candidate / ".git").exists():
            return candidate
    raise FileNotFoundError("Git 저장소 루트를 찾지 못했습니다.")


def prepare_context(repo_root: str | Path | None = None) -> Context:
    repo_root = Path(repo_root).resolve() if repo_root is not None else find_repo_root()
    source_dir = repo_root / "data" / "marts" / "23_marts_csv"
    output_dir = repo_root / "data" / "processed" / "hackle_mixed"
    support_dir = output_dir / "support"
    report_dir = repo_root / "analysis" / "preprocessing" / "hackle_mixed" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(source_dir / "export_manifest.csv", dtype="string")
    source_manifest = manifest.loc[manifest["object_name"].isin(SOURCE_NAMES)].copy()
    source_manifest["exported_rows"] = pd.to_numeric(source_manifest["exported_rows"], errors="coerce").astype("Int64")
    source_manifest["column_count"] = pd.to_numeric(source_manifest["column_count"], errors="coerce").astype("Int64")
    expected_rows = dict(zip(source_manifest["object_name"], source_manifest["exported_rows"].astype(int)))
    if set(expected_rows) != set(SOURCE_NAMES):
        raise AssertionError("Hackle 통합 전처리 대상 4개가 export_manifest에 모두 존재하지 않습니다.")
    missing = [name for name in SUPPORT_NAMES if not (support_dir / f"{name}.csv.gz").exists()]
    if missing:
        raise FileNotFoundError("Hackle 지원표가 없습니다: " + ", ".join(missing))
    return Context(repo_root, source_dir, output_dir, support_dir, report_dir, source_manifest, expected_rows)


def add_qa(ctx: Context, mart: str, test_name: str, actual: Any, expected: Any,
           status: str, severity: str, explanation: str) -> None:
    ctx.qa_rows.append({
        "mart": mart, "test_name": test_name, "actual": actual, "expected": expected,
        "status": status, "severity": severity, "explanation": explanation,
    })


def read_source(ctx: Context, name: str, *, chunksize: int | None = None):
    return pd.read_csv(
        ctx.source_dir / f"{name}.csv.gz", compression="gzip", encoding="utf-8-sig",
        na_values=NA_VALUES, keep_default_na=False, dtype="string", chunksize=chunksize,
        low_memory=False,
    )


def read_support(ctx: Context, name: str, *, chunksize: int | None = None):
    return pd.read_csv(
        ctx.support_dir / f"{name}.csv.gz", compression="gzip", encoding="utf-8-sig",
        keep_default_na=False, dtype="string", chunksize=chunksize, low_memory=False,
    )


def to_number(frame: pd.DataFrame, columns: list[str], kind: str = "Int64") -> pd.DataFrame:
    for col in columns:
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype(kind)
    return frame


def nonblank(series: pd.Series) -> pd.Series:
    return series.notna() & series.astype("string").str.strip().ne("")


def parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="mixed", errors="coerce")


def date_window_flag(parsed: pd.Series) -> pd.Series:
    out = pd.Series(pd.NA, index=parsed.index, dtype="Int8")
    valid = parsed.notna()
    out.loc[valid] = ((parsed.loc[valid] >= HACKLE_START) & (parsed.loc[valid] <= HACKLE_END)).astype("int8")
    return out


def write_frame(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(
        path, index=False, compression={"method": "gzip", "compresslevel": 3},
        encoding="utf-8-sig", na_rep="", date_format="%Y-%m-%d %H:%M:%S.%f",
    )


def source_column_count(ctx: Context, name: str) -> int:
    return int(ctx.source_manifest.loc[ctx.source_manifest["object_name"].eq(name), "column_count"].iloc[0])


def register_log(ctx: Context, name: str, source_rows: int, output_rows: int,
                 source_columns: int, output_columns: int, output_path: Path,
                 source_path: Path, row_preserved: bool | str = True) -> None:
    ctx.processing_log.append({
        "mart": name, "source_path": str(source_path), "output_path": str(output_path),
        "source_rows": int(source_rows), "output_rows": int(output_rows),
        "row_preserved": row_preserved, "source_columns": int(source_columns),
        "output_columns": int(output_columns),
    })
    if isinstance(row_preserved, bool):
        add_qa(
            ctx, name, "row_count_preserved", int(output_rows), int(source_rows),
            "PASS" if row_preserved and int(output_rows) == int(source_rows) else "FAIL",
            "CRITICAL", "전처리 전후 행 수가 같아야 합니다.",
        )


def add_dictionary(ctx: Context, name: str, source_columns: list[str],
                   output_columns: list[str], descriptions: dict[str, str]) -> None:
    for col in output_columns:
        if col in source_columns:
            origin = "SOURCE_PRESERVED"
            description = "원본 v2 마트 컬럼. 이름과 값을 보존함."
        elif col in descriptions:
            origin = "DERIVED"
            description = descriptions[col]
        else:
            origin = "SUPPORT_JOINED"
            description = "Hackle 해석 차원 또는 30분 방문 배정표에서 연결한 컬럼."
        ctx.dictionary_rows.append({
            "mart": name, "column_name": col, "column_origin": origin,
            "description": description,
        })


def collapse_daily(parts: list[pd.DataFrame], keys: list[str], values: list[str]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame(columns=keys + values)
    all_parts = pd.concat(parts, ignore_index=True)
    result = all_parts.groupby(keys, as_index=False, dropna=False)[values].sum()
    for col in values:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype("Int64")
    return result


def qpath(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def duckdb_columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return con.execute(f"DESCRIBE SELECT * FROM {relation}").df()["column_name"].tolist()


def copy_query(con: duckdb.DuckDBPyConnection, query: str, output_path: Path) -> int:
    output_path.unlink(missing_ok=True)
    result = con.execute(
        f"COPY ({query}) TO '{qpath(output_path)}' "
        "(FORMAT CSV, HEADER TRUE, COMPRESSION GZIP, NULL '')"
    ).fetchone()
    return int(result[0])


def process_hackle_base(ctx: Context) -> dict[str, Any]:
    fact_name = "fact_hackle_event_24d_v2"
    visit_name = "mart_hackle_visit_30m_v2"
    fact_output = ctx.output_dir / f"{fact_name}_clean.csv.gz"
    visit_output = ctx.output_dir / f"{visit_name}_clean.csv.gz"
    daily_output = ctx.output_dir / "mart_hackle_user_daily_24d_v2_clean.csv.gz"
    temp_dir = ctx.output_dir / "duckdb_temp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='7GB'")
    con.execute(f"SET temp_directory='{qpath(temp_dir)}'")
    con.execute("SET preserve_insertion_order=false")

    source_fact = ctx.source_dir / f"{fact_name}.csv.gz"
    support = ctx.support_dir
    con.execute(
        f"CREATE VIEW fact_src AS SELECT * FROM read_csv('{qpath(source_fact)}', "
        "header=true, all_varchar=true, nullstr='\\N')"
    )
    con.execute(
        f"CREATE VIEW bridge_src AS SELECT * FROM read_csv('{qpath(support / 'bridge_hackle_event_visit_assignment_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )
    con.execute(
        f"CREATE TEMP TABLE text_dim AS SELECT text_attribute_sk, attribute_type, attribute_value_raw "
        f"FROM read_csv('{qpath(support / 'dim_hackle_event_text_attribute_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )
    con.execute(
        f"CREATE TEMP TABLE session_dim AS SELECT * FROM read_csv('{qpath(support / 'dim_hackle_session_resolved_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )
    con.execute(
        f"CREATE TEMP TABLE user_dim AS SELECT * FROM read_csv('{qpath(support / 'dim_hackle_user_resolved_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )
    con.execute(
        f"CREATE TEMP TABLE device_dim AS SELECT * FROM read_csv('{qpath(support / 'dim_hackle_device_resolved_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )
    con.execute(
        f"CREATE VIEW visit_src AS SELECT * FROM read_csv('{qpath(support / 'dim_hackle_visit_30m_v2.csv.gz')}', "
        "header=true, all_varchar=true, nullstr='')"
    )

    fact_source_columns = duckdb_columns(con, "fact_src")
    fact_query = """
        SELECT
            f.*,
            ek.attribute_value_raw AS event_key,
            rid.attribute_value_raw AS raw_id_attribute,
            item.attribute_value_raw AS item_name_raw,
            page.attribute_value_raw AS page_name_raw,
            a.session_partition_sk AS session_partition_key,
            a.event_order_in_original_session,
            a.previous_event_at_in_original_session,
            a.seconds_since_previous_event,
            a.is_break_15m, a.is_break_30m, a.is_break_60m,
            a.visit_sequence_15m, a.visit_sequence_30m, a.visit_sequence_60m,
            a.analytics_visit_session_id, a.event_order_in_visit_30m,
            s.original_session_id, s.original_session_id_state,
            s.property_row_count AS session_property_row_count,
            s.distinct_raw_user_id_count AS session_distinct_raw_user_id_count,
            s.resolved_raw_user_id,
            s.user_resolution_status AS session_user_resolution_status,
            s.user_conflict_flag AS session_user_conflict_flag,
            s.resolved_language AS session_language,
            s.resolved_osname AS session_osname,
            s.resolved_osversion AS session_osversion,
            s.resolved_versionname AS session_app_version,
            s.language_conflict_flag AS session_language_conflict_flag,
            s.osname_conflict_flag AS session_osname_conflict_flag,
            s.osversion_conflict_flag AS session_osversion_conflict_flag,
            s.versionname_conflict_flag AS session_versionname_conflict_flag,
            s.resolved_device_id,
            s.distinct_device_id_count AS session_distinct_device_id_count,
            s.device_conflict_flag AS session_device_conflict_flag,
            d.resolved_device_model, d.resolved_device_vendor,
            d.model_conflict_flag AS device_model_conflict_flag,
            d.vendor_conflict_flag AS device_vendor_conflict_flag,
            u.hackle_user_property_user_id, u.hackle_user_property_class,
            u.hackle_user_property_gender, u.hackle_user_property_grade,
            u.hackle_user_property_school_id, u.hackle_user_property_match_flag,
            u.service_user_id,
            CASE
                WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
                WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS' THEN 'AMBIGUOUS_NOT_ASSIGNED'
                WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
                ELSE u.account_identity_status
            END AS account_identity_status,
            CASE WHEN u.service_user_id IS NOT NULL AND u.service_user_id<>'' THEN 1 ELSE 0 END AS account_user_match_flag,
            u.current_account_gender, u.current_account_signup_at,
            u.current_account_is_staff, u.current_account_is_superuser,
            u.account_ban_status_current, u.current_account_group_id,
            u.account_grade_current, u.account_class_current,
            u.account_school_id_current, u.account_school_type_current,
            u.current_account_school_address,
            CASE WHEN TRY_CAST(f.event_datetime_raw AS TIMESTAMP) IS NULL THEN 'INVALID_OR_MISSING' ELSE 'PARSED' END AS event_datetime_parse_status,
            CAST(TRY_CAST(f.event_datetime_raw AS TIMESTAMP) AS DATE) AS event_date_recomputed,
            CASE WHEN TRY_CAST(f.event_datetime_raw AS TIMESTAMP)
                      BETWEEN TIMESTAMP '2023-07-18 00:00:00' AND TIMESTAMP '2023-08-10 23:59:59.999999'
                 THEN 1 ELSE 0 END AS hackle_24d_window_flag,
            '30_MIN_INACTIVITY' AS canonical_visit_definition,
            0 AS time_zone_officially_known_flag,
            0 AS applied_timestamp_offset_minutes,
            CASE
                WHEN u.service_user_id IS NOT NULL AND u.service_user_id<>'' THEN 'IDENTIFIED_SERVICE_USER'
                WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS' THEN 'AMBIGUOUS_NOT_ASSIGNED'
                WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
                WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
                ELSE 'UNMATCHED_OR_NONNUMERIC'
            END AS identity_assignment_status,
            1 AS current_profile_not_event_time_flag
        FROM fact_src f
        JOIN bridge_src a ON CAST(a.event_sk AS BIGINT)=CAST(f.event_sk AS BIGINT)
        JOIN text_dim ek ON ek.text_attribute_sk=f.event_key_attribute_sk
        JOIN text_dim rid ON rid.text_attribute_sk=f.raw_id_attribute_sk
        JOIN text_dim item ON item.text_attribute_sk=f.item_name_attribute_sk
        JOIN text_dim page ON page.text_attribute_sk=f.page_name_attribute_sk
        JOIN session_dim s ON s.session_sk=f.original_session_sk
        LEFT JOIN user_dim u ON u.hackle_user_sk=s.resolved_hackle_user_sk
        LEFT JOIN device_dim d ON d.device_sk=s.resolved_device_sk
    """
    fact_output_columns = con.execute(f"DESCRIBE ({fact_query})").df()["column_name"].tolist()
    fact_rows = copy_query(con, fact_query, fact_output)

    fact_stats = con.execute("""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT event_sk) AS distinct_event_sk,
            COUNT(DISTINCT event_id) AS distinct_event_id,
            MIN(TRY_CAST(event_sk AS BIGINT)) AS min_event_sk,
            MAX(TRY_CAST(event_sk AS BIGINT)) AS max_event_sk,
            SUM(CASE WHEN TRY_CAST(event_datetime_raw AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS parse_fail,
            SUM(CASE WHEN TRY_CAST(event_datetime_raw AS TIMESTAMP) NOT BETWEEN
                TIMESTAMP '2023-07-18 00:00:00' AND TIMESTAMP '2023-08-10 23:59:59.999999' THEN 1 ELSE 0 END) AS outside_window
        FROM fact_src
    """).df().iloc[0]

    fact_user_daily = con.execute("""
        SELECT
            u.service_user_id AS user_id,
            CAST(TRY_CAST(f.event_datetime_raw AS TIMESTAMP) AS DATE) AS activity_date,
            COUNT(*)::BIGINT AS hackle_event_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='click_question_start')::BIGINT AS hackle_question_start_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='complete_question')::BIGINT AS hackle_question_complete_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='skip_question')::BIGINT AS hackle_question_skip_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='open_ping')::BIGINT AS hackle_ping_open_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw IN ('view_shop','click_purchase','complete_purchase'))::BIGINT AS hackle_shop_event_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='view_shop')::BIGINT AS hackle_shop_view_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='click_purchase')::BIGINT AS hackle_purchase_click_count,
            COUNT(*) FILTER (WHERE ek.attribute_value_raw='complete_purchase')::BIGINT AS hackle_purchase_complete_count
        FROM fact_src f
        JOIN session_dim s ON s.session_sk=f.original_session_sk
        JOIN user_dim u ON u.hackle_user_sk=s.resolved_hackle_user_sk
        JOIN text_dim ek ON ek.text_attribute_sk=f.event_key_attribute_sk
        WHERE u.service_user_id IS NOT NULL AND u.service_user_id<>''
          AND TRY_CAST(f.event_datetime_raw AS TIMESTAMP) IS NOT NULL
        GROUP BY 1,2
        ORDER BY 1,2
    """).df()
    fact_user_daily["user_id"] = fact_user_daily["user_id"].astype("string")
    fact_user_daily["activity_date"] = pd.to_datetime(fact_user_daily["activity_date"]).dt.strftime("%Y-%m-%d")
    for col in fact_user_daily.columns[2:]:
        fact_user_daily[col] = pd.to_numeric(fact_user_daily[col], errors="coerce").fillna(0).astype("Int64")
    ctx.fact_user_daily = fact_user_daily

    identified_event_rows = int(fact_user_daily["hackle_event_count"].sum())
    ambiguous_forced = int(con.execute("""
        SELECT COUNT(*) FROM session_dim s
        LEFT JOIN user_dim u ON u.hackle_user_sk=s.resolved_hackle_user_sk
        WHERE s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS'
          AND u.service_user_id IS NOT NULL AND u.service_user_id<>''
    """).fetchone()[0])

    register_log(ctx, fact_name, ctx.expected_rows[fact_name], fact_rows,
                 source_column_count(ctx, fact_name), len(fact_output_columns),
                 fact_output, source_fact, fact_rows == ctx.expected_rows[fact_name])
    add_qa(ctx, fact_name, "event_sk_unique", int(fact_stats.row_count - fact_stats.distinct_event_sk), 0,
           "PASS" if fact_stats.row_count == fact_stats.distinct_event_sk else "FAIL", "CRITICAL", "event_sk는 고유해야 합니다.")
    add_qa(ctx, fact_name, "event_id_unique", int(fact_stats.row_count - fact_stats.distinct_event_id), 0,
           "PASS" if fact_stats.row_count == fact_stats.distinct_event_id else "FAIL", "CRITICAL", "event_id는 고유해야 합니다.")
    contiguous = int(fact_stats.min_event_sk) == 1 and int(fact_stats.max_event_sk) == int(fact_stats.row_count)
    add_qa(ctx, fact_name, "event_sk_contiguous", f"{int(fact_stats.min_event_sk)}~{int(fact_stats.max_event_sk)}", f"1~{int(fact_stats.row_count)}",
           "PASS" if contiguous else "FAIL", "CRITICAL", "event_sk 범위는 행 수와 일치해야 합니다.")
    add_qa(ctx, fact_name, "timestamp_parse_fail_rows", int(fact_stats.parse_fail), 0,
           "PASS" if int(fact_stats.parse_fail) == 0 else "FAIL", "CRITICAL", "원시 이벤트 시각은 파싱 가능해야 합니다.")
    add_qa(ctx, fact_name, "events_outside_hackle_24d_window", int(fact_stats.outside_window), 0,
           "PASS" if int(fact_stats.outside_window) == 0 else "FAIL", "CRITICAL", "모든 이벤트는 24일 범위 안에 있어야 합니다.")
    add_qa(ctx, fact_name, "ambiguous_session_forced_user_assignment", ambiguous_forced, 0,
           "PASS" if ambiguous_forced == 0 else "FAIL", "CRITICAL", "사용자 충돌 세션은 계정에 강제 귀속하지 않습니다.")
    unidentified = fact_rows - identified_event_rows
    add_qa(ctx, fact_name, "unidentified_event_rows", unidentified, "원천 상태 보존",
           "WARN" if unidentified else "PASS", "SOURCE_LIMITATION", "식별 불가능한 이벤트를 삭제하지 않고 미귀속 상태로 보존했습니다.")
    add_qa(ctx, fact_name, "official_timezone_unknown", 0, 1, "WARN", "SOURCE_LIMITATION",
           "공식 시간대가 없으므로 원시 시각을 보존하고 +9시간을 적용하지 않았습니다.")

    fact_descriptions = {
        "event_key": "문자열 사전에서 복원한 Hackle 이벤트명.",
        "raw_id_attribute": "이벤트의 원시 id 속성.",
        "analytics_visit_session_id": "원래 세션을 30분 비활동 기준으로 재분할한 방문 ID.",
        "event_datetime_parse_status": "원시 시각 파싱 상태.",
        "event_date_recomputed": "원시 시각에서 다시 계산한 날짜.",
        "hackle_24d_window_flag": "Hackle 공식 24일 범위 포함 여부.",
        "canonical_visit_definition": "방문 기준: 30분 비활동.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "정제 중 적용한 시간 보정. 현재 0.",
        "identity_assignment_status": "서비스 사용자 귀속 상태.",
        "current_profile_not_event_time_flag": "계정 프로필이 이벤트 당시가 아닌 현재 스냅샷임을 표시.",
    }
    add_dictionary(ctx, fact_name, fact_source_columns, fact_output_columns, fact_descriptions)

    visit_source_columns = duckdb_columns(con, "visit_src")
    visit_query = """
        SELECT
            v.*,
            s.original_session_id,
            s.resolved_raw_user_id,
            u.service_user_id,
            CASE
                WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
                WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS' THEN 'AMBIGUOUS_NOT_ASSIGNED'
                WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
                ELSE u.account_identity_status
            END AS account_identity_status,
            s.user_conflict_flag AS session_user_conflict_flag,
            s.resolved_device_id,
            s.device_conflict_flag AS session_device_conflict_flag,
            s.resolved_osname AS osname,
            s.resolved_osversion AS osversion,
            s.resolved_versionname AS app_version,
            d.resolved_device_model AS device_model,
            d.resolved_device_vendor AS device_vendor,
            u.hackle_user_property_gender AS hackle_gender,
            u.hackle_user_property_grade AS hackle_grade,
            u.hackle_user_property_class AS hackle_class,
            u.hackle_user_property_school_id AS hackle_school_id,
            u.account_school_id_current,
            CAST(TRY_CAST(v.visit_start_at_30m AS TIMESTAMP) AS DATE) AS visit_date,
            CASE WHEN TRY_CAST(v.visit_start_at_30m AS TIMESTAMP) IS NOT NULL
                       AND TRY_CAST(v.visit_end_at_30m AS TIMESTAMP) IS NOT NULL
                 THEN 'PARSED' ELSE 'INVALID_OR_MISSING' END AS visit_time_parse_status,
            CASE WHEN TRY_CAST(v.visit_start_at_30m AS TIMESTAMP)
                      BETWEEN TIMESTAMP '2023-07-18 00:00:00' AND TIMESTAMP '2023-08-10 23:59:59.999999'
                 THEN 1 ELSE 0 END AS hackle_24d_window_flag,
            '30_MIN_INACTIVITY' AS canonical_visit_definition,
            0 AS time_zone_officially_known_flag,
            0 AS applied_timestamp_offset_minutes,
            CASE
                WHEN u.service_user_id IS NOT NULL AND u.service_user_id<>'' THEN 'IDENTIFIED_SERVICE_USER'
                WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS' THEN 'AMBIGUOUS_NOT_ASSIGNED'
                WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
                WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
                ELSE 'UNMATCHED_OR_NONNUMERIC'
            END AS identity_assignment_status,
            1 AS current_profile_not_event_time_flag
        FROM visit_src v
        JOIN session_dim s ON s.session_sk=v.original_session_sk
        LEFT JOIN user_dim u ON u.hackle_user_sk=s.resolved_hackle_user_sk
        LEFT JOIN device_dim d ON d.device_sk=s.resolved_device_sk
    """
    visit_output_columns = con.execute(f"DESCRIBE ({visit_query})").df()["column_name"].tolist()
    visit_rows = copy_query(con, visit_query, visit_output)
    visit_stats = con.execute("""
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT analytics_visit_session_id) AS distinct_visit,
               SUM(TRY_CAST(visit_event_count_30m AS BIGINT)) AS event_sum,
               SUM(CASE WHEN TRY_CAST(visit_duration_seconds_30m AS BIGINT)<0 THEN 1 ELSE 0 END) AS negative_duration,
               SUM(CASE WHEN TRY_CAST(visit_start_at_30m AS TIMESTAMP) NOT BETWEEN
                   TIMESTAMP '2023-07-18 00:00:00' AND TIMESTAMP '2023-08-10 23:59:59.999999' THEN 1 ELSE 0 END) AS outside_window
        FROM visit_src
    """).df().iloc[0]
    visit_user_daily = con.execute("""
        SELECT u.service_user_id AS user_id,
               CAST(TRY_CAST(v.visit_start_at_30m AS TIMESTAMP) AS DATE) AS activity_date,
               COUNT(*)::BIGINT AS hackle_visit_count
        FROM visit_src v
        JOIN session_dim s ON s.session_sk=v.original_session_sk
        JOIN user_dim u ON u.hackle_user_sk=s.resolved_hackle_user_sk
        WHERE u.service_user_id IS NOT NULL AND u.service_user_id<>''
          AND TRY_CAST(v.visit_start_at_30m AS TIMESTAMP) IS NOT NULL
        GROUP BY 1,2 ORDER BY 1,2
    """).df()
    visit_user_daily["user_id"] = visit_user_daily["user_id"].astype("string")
    visit_user_daily["activity_date"] = pd.to_datetime(visit_user_daily["activity_date"]).dt.strftime("%Y-%m-%d")
    visit_user_daily["hackle_visit_count"] = pd.to_numeric(visit_user_daily["hackle_visit_count"], errors="coerce").fillna(0).astype("Int64")
    ctx.visit_user_daily = visit_user_daily

    hackle_user_daily = fact_user_daily.merge(visit_user_daily, on=["user_id", "activity_date"], how="outer", validate="one_to_one")
    for col in [c for c in hackle_user_daily.columns if c not in ["user_id", "activity_date"]]:
        hackle_user_daily[col] = pd.to_numeric(hackle_user_daily[col], errors="coerce").fillna(0).astype("Int64")
    hackle_user_daily["hackle_observation_scope"] = "HACKLE_24D_IDENTIFIED_USER"
    hackle_user_daily["canonical_visit_definition"] = "30_MIN_INACTIVITY"
    hackle_user_daily["time_zone_officially_known_flag"] = 0
    hackle_user_daily["applied_timestamp_offset_minutes"] = 0
    hackle_user_daily = hackle_user_daily.sort_values(["user_id", "activity_date"], kind="stable").reset_index(drop=True)
    write_frame(hackle_user_daily, daily_output)
    ctx.hackle_user_daily = hackle_user_daily

    register_log(ctx, visit_name, visit_rows, visit_rows, len(visit_source_columns),
                 len(visit_output_columns), visit_output,
                 ctx.support_dir / "dim_hackle_visit_30m_v2.csv.gz", True)
    register_log(ctx, "mart_hackle_user_daily_24d_v2", fact_rows, len(hackle_user_daily),
                 len(fact_output_columns), len(hackle_user_daily.columns), daily_output,
                 fact_output, "DERIVED_GRAIN")
    add_qa(ctx, visit_name, "visit_id_unique", int(visit_stats.row_count - visit_stats.distinct_visit), 0,
           "PASS" if visit_stats.row_count == visit_stats.distinct_visit else "FAIL", "CRITICAL", "30분 방문 ID는 고유해야 합니다.")
    add_qa(ctx, visit_name, "visit_event_sum_equals_fact_rows", int(visit_stats.event_sum), fact_rows,
           "PASS" if int(visit_stats.event_sum) == fact_rows else "FAIL", "CRITICAL", "모든 이벤트는 정확히 한 방문에 포함되어야 합니다.")
    add_qa(ctx, visit_name, "negative_visit_duration_rows", int(visit_stats.negative_duration), 0,
           "PASS" if int(visit_stats.negative_duration) == 0 else "FAIL", "CRITICAL", "방문 종료가 시작보다 빠를 수 없습니다.")
    add_qa(ctx, visit_name, "visits_outside_hackle_24d_window", int(visit_stats.outside_window), 0,
           "PASS" if int(visit_stats.outside_window) == 0 else "FAIL", "CRITICAL", "방문 시작은 24일 범위 안이어야 합니다.")
    add_dictionary(ctx, visit_name, visit_source_columns, visit_output_columns, {
        "visit_date": "방문 시작 원시 시각의 날짜.",
        "visit_time_parse_status": "방문 시작·종료 시각 파싱 상태.",
        "hackle_24d_window_flag": "방문 시작의 24일 범위 포함 여부.",
        "canonical_visit_definition": "30분 비활동 방문 기준.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "정제 중 적용한 시간 보정. 현재 0.",
        "identity_assignment_status": "서비스 사용자 귀속 상태.",
        "current_profile_not_event_time_flag": "현재 프로필임을 표시.",
    })
    add_dictionary(ctx, "mart_hackle_user_daily_24d_v2", [], list(hackle_user_daily.columns), {
        col: "식별 가능한 Hackle 이벤트와 30분 방문을 사용자×원시 날짜로 집계한 값."
        for col in hackle_user_daily.columns
    })

    con.close()
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    return {
        "fact_rows": fact_rows,
        "visit_rows": visit_rows,
        "identified_event_rows": identified_event_rows,
        "unidentified_event_rows": unidentified,
        "identified_user_days": len(hackle_user_daily),
        "identified_users": int(hackle_user_daily["user_id"].nunique()),
    }


def process_hackle_base_streaming(ctx: Context) -> dict[str, Any]:
    """Memory-safe version: three event-aligned streams plus small dimensions."""
    fact_name = "fact_hackle_event_24d_v2"
    visit_name = "mart_hackle_visit_30m_v2"
    fact_source_path = ctx.source_dir / f"{fact_name}.csv.gz"
    fact_output = ctx.output_dir / f"{fact_name}_clean.csv.gz"
    visit_source_path = ctx.support_dir / "dim_hackle_visit_30m_v2.csv.gz"
    visit_output = ctx.output_dir / f"{visit_name}_clean.csv.gz"
    daily_output = ctx.output_dir / "mart_hackle_user_daily_24d_v2_clean.csv.gz"

    # RAW_ID_ATTRIBUTE is 11.4M high-cardinality values and is supplied by an
    # event-aligned bridge. Only the small event/item/page code maps stay in RAM.
    small_text_parts = []
    for chunk in read_support(ctx, "dim_hackle_event_text_attribute_v2", chunksize=CHUNK_SIZE):
        keep = ~chunk["attribute_type"].eq("RAW_ID_ATTRIBUTE")
        if keep.any():
            small_text_parts.append(chunk.loc[keep, ["text_attribute_sk", "attribute_value_raw"]])
    small_text = pd.concat(small_text_parts, ignore_index=True)
    to_number(small_text, ["text_attribute_sk"])
    text_map = small_text.set_index("text_attribute_sk")["attribute_value_raw"]

    session = read_support(ctx, "dim_hackle_session_resolved_v2")
    users = read_support(ctx, "dim_hackle_user_resolved_v2")
    devices = read_support(ctx, "dim_hackle_device_resolved_v2")
    to_number(session, ["session_sk", "resolved_hackle_user_sk", "resolved_device_sk"])
    to_number(users, ["hackle_user_sk"])
    to_number(devices, ["device_sk"])
    session_lookup = session.merge(
        users, how="left", left_on="resolved_hackle_user_sk", right_on="hackle_user_sk",
        validate="many_to_one", suffixes=("", "_user"),
    ).merge(
        devices, how="left", left_on="resolved_device_sk", right_on="device_sk",
        validate="many_to_one", suffixes=("", "_device"),
    )
    if session_lookup["session_sk"].duplicated().any():
        raise AssertionError("Hackle session_lookup의 session_sk가 중복되었습니다.")

    fact_reader = read_source(ctx, fact_name, chunksize=CHUNK_SIZE)
    visit_bridge_reader = read_support(ctx, "bridge_hackle_event_visit_assignment_v2", chunksize=CHUNK_SIZE)
    raw_id_reader = read_support(ctx, "bridge_hackle_event_raw_id_v2", chunksize=CHUNK_SIZE)
    fact_daily_parts: list[pd.DataFrame] = []
    fact_visit_touch_parts: list[pd.DataFrame] = []
    fact_rows = missing_visit = missing_raw_id = outside_window = parse_fail = 0
    unidentified = ambiguous_forced = sequence_errors = 0
    previous_sk = 0
    fact_source_columns: list[str] = []
    fact_output_columns: list[str] = []

    numeric_fact = ["event_sk", "original_session_sk", "event_key_attribute_sk",
                    "raw_id_attribute_sk", "item_name_attribute_sk", "page_name_attribute_sk",
                    "friend_count", "votes_count", "heart_balance", "question_id"]
    numeric_bridge = ["event_sk", "original_session_sk", "session_partition_sk",
                      "event_order_in_original_session", "seconds_since_previous_event",
                      "is_break_15m", "is_break_30m", "is_break_60m", "visit_sequence_15m",
                      "visit_sequence_30m", "visit_sequence_60m", "event_order_in_visit_30m"]
    value_cols = ["hackle_event_count", "hackle_question_start_count",
                  "hackle_question_complete_count", "hackle_question_skip_count",
                  "hackle_ping_open_count", "hackle_shop_event_count",
                  "hackle_shop_view_count", "hackle_purchase_click_count",
                  "hackle_purchase_complete_count"]

    from itertools import zip_longest
    with gzip.open(fact_output, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        first = True
        streams = zip_longest(fact_reader, visit_bridge_reader, raw_id_reader)
        for chunk_no, triple in enumerate(streams, start=1):
            fact, bridge, raw_id = triple
            if fact is None or bridge is None or raw_id is None:
                raise AssertionError("Hackle fact·방문 배정·raw id 배정의 청크 수가 다릅니다.")
            fact_source_columns = list(fact.columns)
            to_number(fact, numeric_fact)
            to_number(bridge, numeric_bridge)
            to_number(raw_id, ["event_sk"])
            if not (len(fact) == len(bridge) == len(raw_id)):
                raise AssertionError(f"청크 {chunk_no}: 세 정렬 스트림의 행 수가 다릅니다.")
            fact_key = fact["event_sk"].reset_index(drop=True)
            if not fact_key.equals(bridge["event_sk"].reset_index(drop=True)):
                raise AssertionError(f"청크 {chunk_no}: 방문 배정 event_sk가 fact와 다릅니다.")
            if not fact_key.equals(raw_id["event_sk"].reset_index(drop=True)):
                raise AssertionError(f"청크 {chunk_no}: raw id 배정 event_sk가 fact와 다릅니다.")
            expected = np.arange(previous_sk + 1, previous_sk + len(fact) + 1, dtype="int64")
            sequence_errors += int((fact_key.to_numpy(dtype="int64") != expected).sum())
            previous_sk += len(fact)

            clean = fact.merge(
                bridge.drop(columns=["original_session_sk"]), on="event_sk", how="left", validate="one_to_one"
            ).merge(raw_id, on="event_sk", how="left", validate="one_to_one")
            clean["event_key"] = clean["event_key_attribute_sk"].map(text_map)
            clean["item_name_raw"] = clean["item_name_attribute_sk"].map(text_map)
            clean["page_name_raw"] = clean["page_name_attribute_sk"].map(text_map)
            clean = clean.merge(
                session_lookup, how="left", left_on="original_session_sk", right_on="session_sk",
                validate="many_to_one",
            )
            parsed = parse_datetime(clean["event_datetime_raw"])
            clean["event_datetime_parse_status"] = np.where(parsed.notna(), "PARSED", "INVALID_OR_MISSING")
            clean["event_date_recomputed"] = parsed.dt.strftime("%Y-%m-%d").astype("string")
            clean["hackle_24d_window_flag"] = date_window_flag(parsed)
            clean["canonical_visit_definition"] = "30_MIN_INACTIVITY"
            clean["time_zone_officially_known_flag"] = 0
            clean["applied_timestamp_offset_minutes"] = 0
            user_status = clean["user_resolution_status"]
            clean["identity_assignment_status"] = np.select(
                [nonblank(clean["service_user_id"]), user_status.eq("AMBIGUOUS_MULTIPLE_USER_IDS"),
                 user_status.eq("MISSING"), user_status.eq("NO_SESSION_PROPERTY")],
                ["IDENTIFIED_SERVICE_USER", "AMBIGUOUS_NOT_ASSIGNED", "MISSING_USER_ID", "NO_SESSION_PROPERTY"],
                default="UNMATCHED_OR_NONNUMERIC",
            )
            clean["current_profile_not_event_time_flag"] = 1

            missing_visit += int((~nonblank(clean["analytics_visit_session_id"])).sum())
            missing_raw_id += int((~nonblank(clean["raw_id_attribute"])).sum())
            outside_window += int(clean["hackle_24d_window_flag"].eq(0).sum())
            parse_fail += int(parsed.isna().sum())
            unidentified += int((~nonblank(clean["service_user_id"])).sum())
            ambiguous_forced += int((user_status.eq("AMBIGUOUS_MULTIPLE_USER_IDS") & nonblank(clean["service_user_id"])).sum())

            mask = nonblank(clean["service_user_id"]) & parsed.notna()
            if mask.any():
                part = pd.DataFrame({
                    "user_id": clean.loc[mask, "service_user_id"].astype("string"),
                    "activity_date": parsed.loc[mask].dt.strftime("%Y-%m-%d"),
                    "event_key": clean.loc[mask, "event_key"].astype("string"),
                })
                part["hackle_event_count"] = 1
                part["hackle_question_start_count"] = part["event_key"].eq("click_question_start").astype("int64")
                part["hackle_question_complete_count"] = part["event_key"].eq("complete_question").astype("int64")
                part["hackle_question_skip_count"] = part["event_key"].eq("skip_question").astype("int64")
                part["hackle_ping_open_count"] = part["event_key"].eq("open_ping").astype("int64")
                part["hackle_shop_event_count"] = part["event_key"].isin(["view_shop", "click_purchase", "complete_purchase"]).astype("int64")
                part["hackle_shop_view_count"] = part["event_key"].eq("view_shop").astype("int64")
                part["hackle_purchase_click_count"] = part["event_key"].eq("click_purchase").astype("int64")
                part["hackle_purchase_complete_count"] = part["event_key"].eq("complete_purchase").astype("int64")
                fact_daily_parts.append(part.groupby(["user_id", "activity_date"], as_index=False)[value_cols].sum())
                if len(fact_daily_parts) >= 8:
                    fact_daily_parts = [collapse_daily(fact_daily_parts, ["user_id", "activity_date"], value_cols)]
                visit_touch = pd.DataFrame({
                    "user_id": clean.loc[mask, "service_user_id"].astype("string"),
                    "activity_date": parsed.loc[mask].dt.strftime("%Y-%m-%d"),
                    "analytics_visit_session_id": clean.loc[mask, "analytics_visit_session_id"].astype("string"),
                }).drop_duplicates()
                fact_visit_touch_parts.append(visit_touch)
                if len(fact_visit_touch_parts) >= 8:
                    fact_visit_touch_parts = [
                        pd.concat(fact_visit_touch_parts, ignore_index=True).drop_duplicates(
                            ["user_id", "activity_date", "analytics_visit_session_id"]
                        )
                    ]

            clean.to_csv(handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d %H:%M:%S.%f")
            first = False
            fact_rows += len(clean)
            fact_output_columns = list(clean.columns)
            if chunk_no % 5 == 0:
                print(f"  Hackle 이벤트 {fact_rows:,}행 처리", flush=True)
            del fact, bridge, raw_id, clean
            gc.collect()

    fact_user_daily = collapse_daily(fact_daily_parts, ["user_id", "activity_date"], value_cols)
    visit_touch = pd.concat(fact_visit_touch_parts, ignore_index=True).drop_duplicates(
        ["user_id", "activity_date", "analytics_visit_session_id"]
    )
    visit_touch_daily = visit_touch.groupby(["user_id", "activity_date"], as_index=False).size().rename(
        columns={"size": "hackle_visit_count"}
    )
    visit_touch_daily["hackle_visit_count"] = visit_touch_daily["hackle_visit_count"].astype("Int64")
    fact_user_daily = fact_user_daily.merge(
        visit_touch_daily, on=["user_id", "activity_date"], how="outer", validate="one_to_one"
    )
    fact_user_daily["hackle_visit_count"] = pd.to_numeric(
        fact_user_daily["hackle_visit_count"], errors="coerce"
    ).fillna(0).astype("Int64")
    ctx.fact_user_daily = fact_user_daily
    register_log(ctx, fact_name, ctx.expected_rows[fact_name], fact_rows,
                 source_column_count(ctx, fact_name), len(fact_output_columns), fact_output,
                 fact_source_path, fact_rows == ctx.expected_rows[fact_name])
    add_qa(ctx, fact_name, "event_sk_contiguous", sequence_errors, 0,
           "PASS" if sequence_errors == 0 else "FAIL", "CRITICAL", "event_sk는 1부터 연속이어야 합니다.")
    add_qa(ctx, fact_name, "visit_assignment_missing_rows", missing_visit, 0,
           "PASS" if missing_visit == 0 else "FAIL", "CRITICAL", "모든 이벤트에 30분 방문 ID가 있어야 합니다.")
    add_qa(ctx, fact_name, "raw_id_decode_missing_rows", missing_raw_id, 0,
           "PASS" if missing_raw_id == 0 else "FAIL", "CRITICAL", "모든 이벤트의 원시 id 속성을 복원해야 합니다.")
    add_qa(ctx, fact_name, "timestamp_parse_fail_rows", parse_fail, 0,
           "PASS" if parse_fail == 0 else "FAIL", "CRITICAL", "원시 이벤트 시각은 파싱 가능해야 합니다.")
    add_qa(ctx, fact_name, "events_outside_hackle_24d_window", outside_window, 0,
           "PASS" if outside_window == 0 else "FAIL", "CRITICAL", "모든 이벤트는 공식 24일 범위 안에 있어야 합니다.")
    add_qa(ctx, fact_name, "ambiguous_session_forced_user_assignment", ambiguous_forced, 0,
           "PASS" if ambiguous_forced == 0 else "FAIL", "CRITICAL", "사용자 충돌 세션은 계정에 강제 귀속하지 않습니다.")
    add_qa(ctx, fact_name, "unidentified_event_rows", unidentified, "원천 상태 보존",
           "WARN" if unidentified else "PASS", "SOURCE_LIMITATION", "식별 불가능 이벤트를 미귀속 상태로 보존했습니다.")
    add_qa(ctx, fact_name, "official_timezone_unknown", 0, 1, "WARN", "SOURCE_LIMITATION",
           "공식 시간대가 없으므로 원시 시각을 보존하고 +9시간을 적용하지 않았습니다.")
    add_dictionary(ctx, fact_name, fact_source_columns, fact_output_columns, {
        "raw_id_attribute": "event_sk와 1:1인 지원표에서 복원한 원시 id 속성.",
        "event_key": "문자열 사전에서 복원한 Hackle 이벤트명.",
        "analytics_visit_session_id": "30분 비활동 기준 방문 ID.",
        "event_datetime_parse_status": "원시 시각 파싱 상태.",
        "event_date_recomputed": "원시 시각에서 다시 계산한 날짜.",
        "hackle_24d_window_flag": "Hackle 공식 24일 범위 포함 여부.",
        "canonical_visit_definition": "방문 기준: 30분 비활동.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "정제 중 적용한 시간 보정. 현재 0.",
        "identity_assignment_status": "서비스 사용자 귀속 상태.",
        "current_profile_not_event_time_flag": "현재 계정 프로필임을 표시.",
    })

    visit_daily_parts: list[pd.DataFrame] = []
    visit_rows = visit_event_sum = negative_duration = visit_outside = duplicate_visit = 0
    previous_visit_id: str | None = None
    visit_source_columns: list[str] = []
    visit_output_columns: list[str] = []
    visit_numeric = ["original_session_sk", "visit_duration_seconds_30m", "visit_event_count_30m",
                     "distinct_event_key_count", "session_start_event_count", "launch_app_count",
                     "question_start_count", "question_complete_count", "question_skip_count",
                     "ping_open_count", "shop_view_count", "purchase_click_count", "purchase_complete_count"]
    with gzip.open(visit_output, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        first = True
        for chunk in read_support(ctx, "dim_hackle_visit_30m_v2", chunksize=CHUNK_SIZE):
            visit_source_columns = list(chunk.columns)
            to_number(chunk, visit_numeric)
            duplicate_visit += int(chunk["analytics_visit_session_id"].duplicated().sum())
            if len(chunk) and previous_visit_id == str(chunk["analytics_visit_session_id"].iloc[0]):
                duplicate_visit += 1
            if len(chunk):
                previous_visit_id = str(chunk["analytics_visit_session_id"].iloc[-1])
            clean = chunk.merge(session_lookup, how="left", left_on="original_session_sk",
                                right_on="session_sk", validate="many_to_one")
            start = parse_datetime(clean["visit_start_at_30m"])
            end = parse_datetime(clean["visit_end_at_30m"])
            clean["visit_date"] = start.dt.strftime("%Y-%m-%d").astype("string")
            clean["visit_time_parse_status"] = np.where(start.notna() & end.notna(), "PARSED", "INVALID_OR_MISSING")
            clean["hackle_24d_window_flag"] = date_window_flag(start)
            clean["canonical_visit_definition"] = "30_MIN_INACTIVITY"
            clean["time_zone_officially_known_flag"] = 0
            clean["applied_timestamp_offset_minutes"] = 0
            status = clean["user_resolution_status"]
            clean["identity_assignment_status"] = np.select(
                [nonblank(clean["service_user_id"]), status.eq("AMBIGUOUS_MULTIPLE_USER_IDS"),
                 status.eq("MISSING"), status.eq("NO_SESSION_PROPERTY")],
                ["IDENTIFIED_SERVICE_USER", "AMBIGUOUS_NOT_ASSIGNED", "MISSING_USER_ID", "NO_SESSION_PROPERTY"],
                default="UNMATCHED_OR_NONNUMERIC",
            )
            clean["current_profile_not_event_time_flag"] = 1
            visit_event_sum += int(clean["visit_event_count_30m"].fillna(0).sum())
            negative_duration += int((clean["visit_duration_seconds_30m"] < 0).fillna(False).sum())
            visit_outside += int(clean["hackle_24d_window_flag"].eq(0).sum())
            mask = nonblank(clean["service_user_id"]) & start.notna()
            if mask.any():
                part = pd.DataFrame({"user_id": clean.loc[mask, "service_user_id"].astype("string"),
                                     "activity_date": start.loc[mask].dt.strftime("%Y-%m-%d"),
                                     "hackle_visit_started_count": 1})
                visit_daily_parts.append(part.groupby(["user_id", "activity_date"], as_index=False)["hackle_visit_started_count"].sum())
                if len(visit_daily_parts) >= 8:
                    visit_daily_parts = [collapse_daily(visit_daily_parts, ["user_id", "activity_date"], ["hackle_visit_started_count"])]
            clean.to_csv(handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d %H:%M:%S.%f")
            first = False
            visit_rows += len(clean)
            visit_output_columns = list(clean.columns)
            del chunk, clean
            gc.collect()

    visit_user_daily = collapse_daily(visit_daily_parts, ["user_id", "activity_date"], ["hackle_visit_started_count"])
    ctx.visit_user_daily = visit_user_daily
    hackle_user_daily = fact_user_daily.merge(visit_user_daily, on=["user_id", "activity_date"],
                                              how="outer", validate="one_to_one")
    for col in [c for c in hackle_user_daily.columns if c not in ["user_id", "activity_date"]]:
        hackle_user_daily[col] = pd.to_numeric(hackle_user_daily[col], errors="coerce").fillna(0).astype("Int64")
    hackle_user_daily["hackle_observation_scope"] = "HACKLE_24D_IDENTIFIED_USER"
    hackle_user_daily["canonical_visit_definition"] = "30_MIN_INACTIVITY"
    hackle_user_daily["time_zone_officially_known_flag"] = 0
    hackle_user_daily["applied_timestamp_offset_minutes"] = 0
    hackle_user_daily = hackle_user_daily.sort_values(["user_id", "activity_date"], kind="stable").reset_index(drop=True)
    write_frame(hackle_user_daily, daily_output)
    ctx.hackle_user_daily = hackle_user_daily

    register_log(ctx, visit_name, visit_rows, visit_rows, len(visit_source_columns), len(visit_output_columns),
                 visit_output, visit_source_path, True)
    register_log(ctx, "mart_hackle_user_daily_24d_v2", fact_rows, len(hackle_user_daily),
                 len(fact_output_columns), len(hackle_user_daily.columns), daily_output, fact_output, "DERIVED_GRAIN")
    add_qa(ctx, visit_name, "visit_id_unique", duplicate_visit, 0,
           "PASS" if duplicate_visit == 0 else "FAIL", "CRITICAL", "30분 방문 ID는 고유해야 합니다.")
    add_qa(ctx, visit_name, "visit_event_sum_equals_fact_rows", visit_event_sum, fact_rows,
           "PASS" if visit_event_sum == fact_rows else "FAIL", "CRITICAL", "모든 이벤트는 정확히 한 방문에 포함되어야 합니다.")
    add_qa(ctx, visit_name, "negative_visit_duration_rows", negative_duration, 0,
           "PASS" if negative_duration == 0 else "FAIL", "CRITICAL", "방문 종료가 시작보다 빠를 수 없습니다.")
    add_qa(ctx, visit_name, "visits_outside_hackle_24d_window", visit_outside, 0,
           "PASS" if visit_outside == 0 else "FAIL", "CRITICAL", "방문 시작은 24일 범위 안이어야 합니다.")
    touch_total = int(hackle_user_daily["hackle_visit_count"].sum())
    started_total = int(hackle_user_daily["hackle_visit_started_count"].sum())
    add_qa(ctx, visit_name, "daily_visit_touch_vs_started_definition", touch_total - started_total, ">=0", "PASS", "INFO",
           "자정을 넘긴 방문은 이벤트 날짜별 접촉 방문 수에는 양일에 잡히고 시작 방문 수에는 첫날에만 잡힙니다.")
    add_dictionary(ctx, visit_name, visit_source_columns, visit_output_columns, {
        "visit_date": "방문 시작 원시 시각의 날짜.",
        "visit_time_parse_status": "방문 시작·종료 시각 파싱 상태.",
        "hackle_24d_window_flag": "방문 시작의 24일 범위 포함 여부.",
        "canonical_visit_definition": "30분 비활동 방문 기준.",
        "time_zone_officially_known_flag": "공식 시간대 확인 여부. 현재 0.",
        "applied_timestamp_offset_minutes": "정제 중 적용한 시간 보정. 현재 0.",
        "identity_assignment_status": "서비스 사용자 귀속 상태.",
        "current_profile_not_event_time_flag": "현재 프로필임을 표시.",
    })
    add_dictionary(ctx, "mart_hackle_user_daily_24d_v2", [], list(hackle_user_daily.columns), {
        col: "식별 가능한 Hackle 이벤트와 30분 방문의 사용자×원시 날짜 집계."
        for col in hackle_user_daily.columns
    })
    return {"fact_rows": fact_rows, "visit_rows": visit_rows,
            "identified_event_rows": fact_rows - unidentified,
            "unidentified_event_rows": unidentified,
            "identified_user_days": len(hackle_user_daily),
            "identified_users": int(hackle_user_daily["user_id"].nunique())}


def process_value(ctx: Context) -> dict[str, Any]:
    name = "mart_value_event_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    row_count = 0
    hackle_rows = outside_rows = missing_visit_rows = 0
    event_counts: dict[str, int] = {}
    daily_parts: list[pd.DataFrame] = []
    source_columns: list[str] = []
    output_columns: list[str] = []
    with gzip.open(output_path, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        first = True
        for chunk in read_source(ctx, name, chunksize=CHUNK_SIZE):
            source_columns = list(chunk.columns)
            parsed = parse_datetime(chunk["event_at_raw"])
            is_hackle = chunk["source_system"].eq("HACKLE")
            chunk["event_at_parse_status"] = np.where(parsed.notna(), "PARSED", "INVALID_OR_MISSING")
            chunk["event_date_recomputed"] = parsed.dt.strftime("%Y-%m-%d").astype("string")
            chunk["event_family"] = np.select(
                [chunk["event_type"].str.startswith("POINT_", na=False),
                 chunk["event_type"].isin(["PAYMENT_SUCCESS", "PAYMENT_FAIL"]),
                 chunk["event_type"].eq("PROMO_POINT_RECEIPT"),
                 chunk["event_type"].str.startswith("HACKLE_", na=False)],
                ["POINT", "DB_PAYMENT", "PROMO", "HACKLE_PURCHASE_UX"], default="OTHER",
            )
            chunk["source_observation_scope"] = np.where(is_hackle, "HACKLE_24D", chunk["source_scope"])
            chunk["hackle_24d_window_flag"] = pd.Series(pd.NA, index=chunk.index, dtype="Int8")
            chunk.loc[is_hackle, "hackle_24d_window_flag"] = date_window_flag(parsed.loc[is_hackle])
            chunk["identity_assignment_status"] = np.where(
                nonblank(chunk["service_user_id"]), "IDENTIFIED_SERVICE_USER",
                np.where(is_hackle, "HACKLE_UNIDENTIFIED", "DB_USER_NOT_LINKED"),
            )
            chunk["canonical_visit_definition"] = np.where(is_hackle, "30_MIN_INACTIVITY", "NOT_APPLICABLE")
            chunk["do_not_sum_with_db_payment_flag"] = chunk["event_type"].eq("HACKLE_PURCHASE_COMPLETE").astype("Int8")
            chunk["recommended_measurement_role"] = np.select(
                [chunk["event_type"].eq("PAYMENT_SUCCESS"), chunk["event_type"].eq("PAYMENT_FAIL"),
                 chunk["event_type"].str.startswith("HACKLE_", na=False)],
                ["SERVER_TRANSACTION_TRUTH", "INCOMPLETE_PERIOD_FAILURE_RECORD", "CLIENT_PURCHASE_UX"],
                default="SOURCE_SPECIFIC_EVENT",
            )
            chunk["mixed_source_time_comparison_caution_flag"] = 1

            hackle_rows += int(is_hackle.sum())
            outside_rows += int(chunk.loc[is_hackle, "hackle_24d_window_flag"].eq(0).sum())
            missing_visit_rows += int((is_hackle & ~nonblank(chunk["analytics_visit_session_id"])).sum())
            for key, value in chunk["event_type"].value_counts(dropna=False).items():
                event_counts[str(key)] = event_counts.get(str(key), 0) + int(value)
            mask = is_hackle & nonblank(chunk["service_user_id"]) & parsed.notna()
            if mask.any():
                part = pd.DataFrame({
                    "user_id": chunk.loc[mask, "service_user_id"].astype("string"),
                    "activity_date": parsed.loc[mask].dt.strftime("%Y-%m-%d"),
                    "value_hackle_shop_event_count": 1,
                })
                daily_parts.append(part.groupby(["user_id", "activity_date"], as_index=False)["value_hackle_shop_event_count"].sum())
                if len(daily_parts) >= 8:
                    daily_parts = [collapse_daily(daily_parts, ["user_id", "activity_date"], ["value_hackle_shop_event_count"])]
            chunk.to_csv(handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d %H:%M:%S.%f")
            first = False
            row_count += len(chunk)
            output_columns = list(chunk.columns)
            del chunk
            gc.collect()

    ctx.value_hackle_daily = collapse_daily(daily_parts, ["user_id", "activity_date"], ["value_hackle_shop_event_count"])
    register_log(ctx, name, ctx.expected_rows[name], row_count, source_column_count(ctx, name),
                 len(output_columns), output_path, source_path, row_count == ctx.expected_rows[name])
    add_qa(ctx, name, "hackle_value_rows", hackle_rows, 41847,
           "PASS" if hackle_rows == 41847 else "FAIL", "CRITICAL", "Hackle 구매 UX 행 수를 보존합니다.")
    add_qa(ctx, name, "hackle_rows_outside_24d", outside_rows, 0,
           "PASS" if outside_rows == 0 else "FAIL", "CRITICAL", "Hackle 구매 UX는 24일 범위 안이어야 합니다.")
    add_qa(ctx, name, "hackle_rows_missing_30m_visit", missing_visit_rows, 0,
           "PASS" if missing_visit_rows == 0 else "FAIL", "CRITICAL", "Hackle 구매 UX에 30분 방문 ID가 있어야 합니다.")
    add_qa(ctx, name, "db_and_hackle_purchase_are_not_additive", "분리 라벨 적용", "합산 금지", "PASS", "CRITICAL",
           "DB 결제는 서버 거래, Hackle 구매완료는 클라이언트 UX로 분리했습니다.")
    add_dictionary(ctx, name, source_columns, output_columns, {
        "event_at_parse_status": "원시 이벤트 시각 파싱 상태.",
        "event_date_recomputed": "원시 시각에서 다시 계산한 날짜.",
        "event_family": "POINT, DB_PAYMENT, PROMO, HACKLE_PURCHASE_UX 분석 계열.",
        "source_observation_scope": "행별 실제 관측 모집단·기간.",
        "hackle_24d_window_flag": "Hackle 행의 24일 범위 포함 여부.",
        "identity_assignment_status": "서비스 사용자 식별 상태.",
        "canonical_visit_definition": "Hackle 행의 30분 방문 기준.",
        "do_not_sum_with_db_payment_flag": "DB 결제와 합산 금지인 Hackle 구매완료.",
        "recommended_measurement_role": "서버 거래·실패 기록·클라이언트 UX 권장 용도.",
        "mixed_source_time_comparison_caution_flag": "공식 시간대 미확정 원천 간 직접 시각 비교 주의.",
    })
    return {"rows": row_count, "hackle_rows": hackle_rows, "event_counts": event_counts}


def process_activity(ctx: Context) -> dict[str, Any]:
    if ctx.hackle_user_daily is None:
        raise RuntimeError("Hackle 사용자 일별 기준표가 먼저 필요합니다.")
    name = "mart_user_activity_daily_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    hackle_cols = ["hackle_event_count", "hackle_visit_count", "hackle_question_start_count",
                   "hackle_question_complete_count", "hackle_shop_event_count"]
    db_cols = [
        "signup_record_count", "attendance_raw_element_count", "attendance_record_count",
        "friend_request_sent_count", "friend_request_received_count", "friend_request_sent_final_a_count",
        "friend_request_sent_final_p_count", "friend_request_sent_final_r_count",
        "db_question_set_created_count", "db_vote_record_created_count", "db_ping_received_record_count",
        "ping_current_read_record_count", "ping_current_answered_record_count", "point_earn_event_count",
        "point_spend_event_count", "db_payment_success_count", "db_payment_fail_count", "promo_receipt_count",
    ]
    row_count = duplicate_rows = sort_errors = 0
    invalid_activity_date_rows = missing_days_since_signup_rows = 0
    pre_signup_activity_rows = 0
    pre_signup_user_ids: set[int] = set()
    friend_request_status_sum_mismatch_rows = 0
    user_initiated_proxy_mismatch_rows = 0
    any_record_flag_mismatch_rows = 0
    context_only_flag_mismatch_rows = 0
    previous_key: tuple[int, str] | None = None
    positive_parts: list[pd.DataFrame] = []
    source_columns: list[str] = []
    output_columns: list[str] = []
    with gzip.open(output_path, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as handle:
        first = True
        for chunk in read_source(ctx, name, chunksize=CHUNK_SIZE):
            source_columns = list(chunk.columns)
            to_number(chunk, ["user_id", "days_since_signup"] + hackle_cols + db_cols)
            parsed_date = pd.to_datetime(chunk["activity_date"], errors="coerce")
            invalid_activity_date_rows += int(parsed_date.isna().sum())
            days_since_signup = pd.to_numeric(chunk["days_since_signup"], errors="coerce")
            missing_days_since_signup_rows += int(days_since_signup.isna().sum())
            pre_signup_mask = days_since_signup.lt(0)
            pre_signup_activity_rows += int(pre_signup_mask.sum())
            if pre_signup_mask.any():
                pre_signup_user_ids.update(
                    pd.to_numeric(chunk.loc[pre_signup_mask, "user_id"], errors="coerce")
                    .dropna().astype("int64").tolist()
                )
            keys = pd.MultiIndex.from_arrays([chunk["user_id"], chunk["activity_date"]])
            duplicate_rows += int(keys.duplicated().sum())
            if not keys.is_monotonic_increasing:
                sort_errors += 1
            if len(keys):
                first_key = (int(chunk["user_id"].iloc[0]), str(chunk["activity_date"].iloc[0]))
                last_key = (int(chunk["user_id"].iloc[-1]), str(chunk["activity_date"].iloc[-1]))
                if previous_key is not None:
                    if first_key == previous_key:
                        duplicate_rows += 1
                    elif first_key < previous_key:
                        sort_errors += 1
                previous_key = last_key

            hackle_sum = chunk[hackle_cols].fillna(0).sum(axis=1)
            db_sum = chunk[db_cols].fillna(0).sum(axis=1)
            hackle_positive = hackle_sum.gt(0)
            db_positive = db_sum.gt(0)
            in_window = (parsed_date >= HACKLE_START.normalize()) & (parsed_date <= HACKLE_END.normalize())
            chunk["row_storage_type"] = "SPARSE_OBSERVED_DATE_ONLY"
            chunk["hackle_date_in_observation_window_flag"] = in_window.astype("Int8")
            chunk["hackle_identified_activity_observed_flag"] = hackle_positive.astype("Int8")
            chunk["db_record_observed_flag"] = db_positive.astype("Int8")
            chunk["source_composition"] = np.select(
                [hackle_positive & db_positive, hackle_positive, db_positive],
                ["HACKLE_AND_DB", "HACKLE_ONLY", "DB_ONLY"], default="CONTEXT_OR_SYSTEM_ONLY",
            )
            chunk["zero_hackle_interpretation"] = np.where(
                in_window & ~hackle_positive, "NO_IDENTIFIED_HACKLE_EVENT_ON_STORED_DATE",
                np.where(~in_window, "OUTSIDE_HACKLE_OBSERVATION_WINDOW", "HACKLE_ACTIVITY_PRESENT"),
            )
            chunk["canonical_visit_definition"] = np.where(in_window, "30_MIN_INACTIVITY", "NOT_APPLICABLE")
            chunk["time_zone_officially_known_flag"] = pd.Series(pd.NA, index=chunk.index, dtype="Int8")
            chunk.loc[in_window, "time_zone_officially_known_flag"] = 0
            chunk["absence_of_row_is_inactivity_flag"] = 0
            chunk["pre_signup_activity_record_flag"] = pre_signup_mask.astype("Int8")
            chunk["pre_signup_activity_interpretation"] = np.select(
                [days_since_signup.isna(), pre_signup_mask],
                ["SIGNUP_DATE_UNAVAILABLE", "SOURCE_EVENT_PRECEDES_ACCOUNT_SIGNUP_DATE"],
                default="ON_OR_AFTER_SIGNUP",
            )

            friend_status_sum = (
                chunk["friend_request_sent_final_a_count"].fillna(0)
                + chunk["friend_request_sent_final_p_count"].fillna(0)
                + chunk["friend_request_sent_final_r_count"].fillna(0)
            )
            friend_request_status_sum_mismatch_rows += int(
                chunk["friend_request_sent_count"].fillna(0).ne(friend_status_sum).sum()
            )
            user_initiated_expected = (
                chunk["attendance_raw_element_count"].fillna(0)
                + chunk["friend_request_sent_count"].fillna(0)
                + chunk["db_vote_record_created_count"].fillna(0)
                + chunk["db_payment_success_count"].fillna(0)
                + chunk["db_payment_fail_count"].fillna(0)
                + chunk["hackle_event_count"].fillna(0)
            ).gt(0)
            user_initiated_proxy_mismatch_rows += int(
                pd.to_numeric(chunk["user_initiated_activity_proxy_flag"], errors="coerce")
                .fillna(0).astype("int64").ne(user_initiated_expected.astype("int64")).sum()
            )
            any_record_expected = (
                chunk["signup_record_count"].fillna(0)
                + chunk["attendance_raw_element_count"].fillna(0)
                + chunk["friend_request_sent_count"].fillna(0)
                + chunk["friend_request_received_count"].fillna(0)
                + chunk["db_question_set_created_count"].fillna(0)
                + chunk["db_vote_record_created_count"].fillna(0)
                + chunk["db_ping_received_record_count"].fillna(0)
                + chunk["point_earn_event_count"].fillna(0)
                + chunk["point_spend_event_count"].fillna(0)
                + chunk["db_payment_success_count"].fillna(0)
                + chunk["db_payment_fail_count"].fillna(0)
                + chunk["promo_receipt_count"].fillna(0)
                + chunk["hackle_event_count"].fillna(0)
            ).gt(0)
            any_record_flag_mismatch_rows += int(
                pd.to_numeric(chunk["any_record_observed_flag"], errors="coerce")
                .fillna(0).astype("int64").ne(any_record_expected.astype("int64")).sum()
            )
            context_only_expected = (
                (
                    chunk["signup_record_count"].fillna(0)
                    + chunk["friend_request_received_count"].fillna(0)
                    + chunk["db_question_set_created_count"].fillna(0)
                    + chunk["db_ping_received_record_count"].fillna(0)
                    + chunk["point_earn_event_count"].fillna(0)
                ).gt(0)
                & ~user_initiated_expected
            )
            context_only_flag_mismatch_rows += int(
                pd.to_numeric(chunk["context_or_system_only_day_flag"], errors="coerce")
                .fillna(0).astype("int64").ne(context_only_expected.astype("int64")).sum()
            )
            if hackle_positive.any():
                positive_parts.append(chunk.loc[hackle_positive, ["user_id", "activity_date"] + hackle_cols].copy())
            chunk.to_csv(handle, index=False, header=first, na_rep="", date_format="%Y-%m-%d")
            first = False
            row_count += len(chunk)
            output_columns = list(chunk.columns)
            del chunk
            gc.collect()

    activity_hackle = pd.concat(positive_parts, ignore_index=True)
    activity_hackle["user_id"] = activity_hackle["user_id"].astype("string")
    for col in hackle_cols:
        activity_hackle[col] = pd.to_numeric(activity_hackle[col], errors="coerce").fillna(0).astype("Int64")
    ctx.activity_hackle = activity_hackle
    register_log(ctx, name, ctx.expected_rows[name], row_count, source_column_count(ctx, name),
                 len(output_columns), output_path, source_path, row_count == ctx.expected_rows[name])
    add_qa(ctx, name, "user_date_duplicate_rows", duplicate_rows, 0,
           "PASS" if duplicate_rows == 0 else "FAIL", "CRITICAL", "사용자×일자 키는 고유해야 합니다.")
    add_qa(ctx, name, "user_date_sort_errors", sort_errors, 0,
           "PASS" if sort_errors == 0 else "FAIL", "CRITICAL", "사용자×일자 순서를 검증합니다.")
    add_qa(ctx, name, "invalid_activity_date_rows", invalid_activity_date_rows, 0,
           "PASS" if invalid_activity_date_rows == 0 else "FAIL", "CRITICAL", "모든 활동일은 날짜로 파싱 가능해야 합니다.")
    add_qa(ctx, name, "days_since_signup_missing_rows", missing_days_since_signup_rows, 0,
           "PASS" if missing_days_since_signup_rows == 0 else "FAIL", "CRITICAL", "가입일 연결 후 가입 경과일이 비어 있지 않아야 합니다.")
    add_qa(ctx, name, "pre_signup_activity_rows", pre_signup_activity_rows, "원천 상태 보존 및 플래그",
           "WARN" if pre_signup_activity_rows else "PASS", "SOURCE_LIMITATION",
           f"가입일보다 앞선 활동 {pre_signup_activity_rows}행({len(pre_signup_user_ids)}명)은 삭제·보정하지 않고 원천 이상 플래그로 보존했습니다.")
    add_qa(ctx, name, "friend_request_status_sum_mismatch_rows", friend_request_status_sum_mismatch_rows, 0,
           "PASS" if friend_request_status_sum_mismatch_rows == 0 else "FAIL", "CRITICAL",
           "친구요청 발송 수는 최종 A/P/R 상태 합계와 같아야 합니다.")
    add_qa(ctx, name, "user_initiated_activity_proxy_definition_mismatch_rows", user_initiated_proxy_mismatch_rows, 0,
           "PASS" if user_initiated_proxy_mismatch_rows == 0 else "FAIL", "CRITICAL",
           "사용자 주도 활동 proxy 플래그가 생성 SQL 정의와 같아야 합니다.")
    add_qa(ctx, name, "any_record_observed_definition_mismatch_rows", any_record_flag_mismatch_rows, 0,
           "PASS" if any_record_flag_mismatch_rows == 0 else "FAIL", "CRITICAL",
           "기록 관측 플래그가 생성 SQL 정의와 같아야 합니다.")
    add_qa(ctx, name, "context_or_system_only_definition_mismatch_rows", context_only_flag_mismatch_rows, 0,
           "PASS" if context_only_flag_mismatch_rows == 0 else "FAIL", "CRITICAL",
           "맥락·시스템 기록일 플래그가 생성 SQL 정의와 같아야 합니다.")
    add_qa(ctx, name, "sparse_panel_semantics_documented", "행 없는 날짜 미생성", "명시", "PASS", "CRITICAL",
           "행 부재를 비활성·이탈로 처리하지 않도록 플래그를 추가했습니다.")

    recon = ctx.hackle_user_daily.merge(activity_hackle, on=["user_id", "activity_date"], how="outer",
                                        suffixes=("_fact", "_activity"), indicator=True)
    for metric in hackle_cols:
        left = pd.to_numeric(recon[f"{metric}_fact"], errors="coerce").fillna(0)
        right = pd.to_numeric(recon[f"{metric}_activity"], errors="coerce").fillna(0)
        mismatch = int((left != right).sum())
        add_qa(ctx, name, f"{metric}_reconciles_to_hackle_fact", mismatch, 0,
               "PASS" if mismatch == 0 else "FAIL", "CRITICAL", "원시 Hackle·30분 방문 재집계와 일별 값이 같아야 합니다.")
    add_dictionary(ctx, name, source_columns, output_columns, {
        "row_storage_type": "전체 날짜 패널이 아닌 기록일만 저장하는 희소 마트.",
        "hackle_date_in_observation_window_flag": "일자가 Hackle 24일 범위에 속하는지.",
        "hackle_identified_activity_observed_flag": "식별 가능한 Hackle 이벤트/방문 존재 여부.",
        "db_record_observed_flag": "DB 원천 기록 존재 여부.",
        "source_composition": "Hackle·DB 기록 조합.",
        "zero_hackle_interpretation": "Hackle 0의 올바른 해석.",
        "canonical_visit_definition": "Hackle 방문 집계 기준.",
        "time_zone_officially_known_flag": "Hackle 공식 시간대 확인 여부.",
        "absence_of_row_is_inactivity_flag": "행 부재를 비활성으로 볼 수 있는지. 현재 0.",
        "pre_signup_activity_record_flag": "활동일이 연결된 계정 가입일보다 앞서는 원천 이상 여부.",
        "pre_signup_activity_interpretation": "가입일 비교 결과. 음수는 삭제하지 않고 원천 시각·계정 연결 검토 대상으로 보존.",
    })
    return {"rows": row_count, "hackle_positive_user_days": len(activity_hackle),
            "pre_signup_activity_rows": pre_signup_activity_rows,
            "pre_signup_users": len(pre_signup_user_ids)}


def process_cumulative(ctx: Context) -> dict[str, Any]:
    if ctx.hackle_user_daily is None or ctx.value_hackle_daily is None:
        raise RuntimeError("Hackle 기준표와 가치 원장 처리가 먼저 필요합니다.")
    name = "mart_user_cumulative_state_1y_v2"
    source_path = ctx.source_dir / f"{name}.csv.gz"
    output_path = ctx.output_dir / f"{name}_clean.csv.gz"
    frame = read_source(ctx, name)
    source_columns = list(frame.columns)
    hackle_cols = ["hackle_24d_event_count", "hackle_24d_visit_count",
                   "hackle_24d_question_start_count", "hackle_24d_question_complete_count"]
    to_number(frame, hackle_cols + ["hackle_identified_user_observed_flag"])
    frame["user_id"] = frame["user_id"].astype("string")
    frame["hackle_observation_scope"] = "IDENTIFIED_USER_EVENTS_IN_2023_07_18_TO_2023_08_10"
    frame["hackle_observation_status"] = np.where(
        frame["hackle_identified_user_observed_flag"].fillna(0).eq(1),
        "IDENTIFIED_HACKLE_USER_OBSERVED", "NO_IDENTIFIED_HACKLE_EVENT_IN_24D",
    )
    frame["zero_hackle_is_annual_inactivity_flag"] = 0
    frame["canonical_visit_definition"] = "30_MIN_INACTIVITY"
    frame["time_zone_officially_known_flag"] = 0
    frame["current_profile_not_historical_flag"] = 1
    frame["mixed_population_caution"] = "QUESTION_POINT_TOP10; PAYMENT_SAFETY_GLOBAL; HACKLE_IDENTIFIED_24D"
    write_frame(frame, output_path)
    register_log(ctx, name, ctx.expected_rows[name], len(frame), source_column_count(ctx, name),
                 len(frame.columns), output_path, source_path, len(frame) == ctx.expected_rows[name])
    duplicate_users = int(frame["user_id"].duplicated().sum())
    add_qa(ctx, name, "user_id_duplicate_rows", duplicate_users, 0,
           "PASS" if duplicate_users == 0 else "FAIL", "CRITICAL", "누적상태는 사용자 1명당 1행이어야 합니다.")
    add_qa(ctx, name, "zero_not_interpreted_as_annual_inactivity", 0, 0, "PASS", "CRITICAL",
           "Hackle 0은 24일 동안 식별 가능한 이벤트가 없었다는 뜻으로만 사용합니다.")

    # The daily activity mart counts every distinct visit touched on each event
    # date.  A visit crossing midnight is therefore present on both dates.
    # The cumulative mart is built from dim_hackle_visit_30m_v2 and counts each
    # visit exactly once.  Reconcile its visit metric with visit-start counts,
    # while the other metrics remain additive event-date counts.
    daily = ctx.hackle_user_daily.groupby("user_id", as_index=False)[
        ["hackle_event_count", "hackle_visit_started_count", "hackle_question_start_count", "hackle_question_complete_count"]
    ].sum().rename(columns={
        "hackle_event_count": "hackle_24d_event_count_recalc",
        "hackle_visit_started_count": "hackle_24d_visit_count_recalc",
        "hackle_question_start_count": "hackle_24d_question_start_count_recalc",
        "hackle_question_complete_count": "hackle_24d_question_complete_count_recalc",
    })
    recon = frame[["user_id"] + hackle_cols].merge(daily, on="user_id", how="left", validate="one_to_one")
    for source_col in hackle_cols:
        left = pd.to_numeric(recon[source_col], errors="coerce").fillna(0)
        right = pd.to_numeric(recon[f"{source_col}_recalc"], errors="coerce").fillna(0)
        mismatch = int((left != right).sum())
        explanation = (
            "30분 방문 원장의 방문 시작일 집계와 24일 고유 방문 수가 같아야 합니다."
            if source_col == "hackle_24d_visit_count"
            else "Hackle 일별 이벤트 합계와 누적상태 값이 같아야 합니다."
        )
        add_qa(ctx, name, f"{source_col}_reconciles_to_user_daily", mismatch, 0,
               "PASS" if mismatch == 0 else "FAIL", "CRITICAL", explanation)

    value_recon = ctx.hackle_user_daily.merge(ctx.value_hackle_daily, on=["user_id", "activity_date"], how="outer")
    left_shop = pd.to_numeric(value_recon["hackle_shop_event_count"], errors="coerce").fillna(0)
    right_shop = pd.to_numeric(value_recon["value_hackle_shop_event_count"], errors="coerce").fillna(0)
    mismatch = int((left_shop != right_shop).sum())
    add_qa(ctx, "mart_value_event_v2", "identified_hackle_shop_counts_reconcile_to_fact", mismatch, 0,
           "PASS" if mismatch == 0 else "FAIL", "CRITICAL", "가치 원장의 Hackle 구매 UX와 원시 fact 재집계가 같아야 합니다.")
    add_dictionary(ctx, name, source_columns, list(frame.columns), {
        "hackle_observation_scope": "Hackle 누적값의 모집단과 기간.",
        "hackle_observation_status": "24일 동안 식별 가능한 Hackle 사용자 관측 여부.",
        "zero_hackle_is_annual_inactivity_flag": "Hackle 0을 1년 미활동으로 해석 가능한지. 현재 0.",
        "canonical_visit_definition": "Hackle 방문 수의 30분 비활동 기준.",
        "time_zone_officially_known_flag": "Hackle 공식 시간대 확인 여부. 현재 0.",
        "current_profile_not_historical_flag": "학교·학년·반 등이 현재 스냅샷임을 표시.",
        "mixed_population_caution": "한 행 안에 섞인 원천별 모집단 범위 요약.",
    })
    return {"rows": len(frame), "users_with_hackle": int(frame["hackle_identified_user_observed_flag"].fillna(0).eq(1).sum())}


def finalize(ctx: Context) -> dict[str, Any]:
    qa = pd.DataFrame(ctx.qa_rows)
    processing = pd.DataFrame(ctx.processing_log)
    dictionary = pd.DataFrame(ctx.dictionary_rows).drop_duplicates(["mart", "column_name"], keep="last")
    scope = pd.DataFrame([
        {"object": "fact_hackle_event_24d_v2", "population": "Hackle 전체 이벤트", "period": "2023-07-18~2023-08-10", "time_basis": "원시 시각, 공식 시간대 미확정", "grain": "event_id 1행"},
        {"object": "mart_hackle_visit_30m_v2", "population": "Hackle 전체 이벤트에서 만든 방문", "period": "2023-07-18~2023-08-10", "time_basis": "원시 시각, 30분 비활동", "grain": "visit_id 1행"},
        {"object": "mart_hackle_user_daily_24d_v2", "population": "안전하게 식별된 Hackle 사용자", "period": "2023-07-18~2023-08-10", "time_basis": "원시 시각 날짜", "grain": "user_id×activity_date"},
        {"object": "mart_value_event_v2", "population": "DB 이벤트별 범위 + Hackle 구매 UX", "period": "원천별 상이; Hackle만 24일", "time_basis": "원천별 공식 시간대 미확정", "grain": "source_table×source_row_id"},
        {"object": "mart_user_activity_daily_v2", "population": "식별 가능한 사용자 기록일", "period": "원천별 상이; Hackle만 24일", "time_basis": "희소 활동일", "grain": "user_id×activity_date"},
        {"object": "mart_user_cumulative_state_1y_v2", "population": "전체 계정 677,085명", "period": "원천별 상이; Hackle 누적만 24일", "time_basis": "현재 프로필 + 기간별 누적", "grain": "user_id 1행"},
    ])
    qa_path = ctx.report_dir / "hackle_mixed_qa_summary.csv"
    log_path = ctx.report_dir / "hackle_mixed_processing_log.csv"
    dict_path = ctx.report_dir / "hackle_mixed_column_dictionary.csv"
    scope_path = ctx.report_dir / "hackle_mixed_scope_summary.csv"
    report_path = ctx.report_dir / "hackle_mixed_preprocessing_report.md"
    qa.to_csv(qa_path, index=False, encoding="utf-8-sig")
    processing.to_csv(log_path, index=False, encoding="utf-8-sig")
    dictionary.to_csv(dict_path, index=False, encoding="utf-8-sig")
    scope.to_csv(scope_path, index=False, encoding="utf-8-sig")
    counts = qa["status"].value_counts().to_dict()
    passed, warned, failed = int(counts.get("PASS", 0)), int(counts.get("WARN", 0)), int(counts.get("FAIL", 0))
    lines = [
        "# Hackle 및 혼합 마트 통합 전처리 결과", "", "## 완료 판정", "",
        f"- PASS: {passed}건", f"- WARN: {warned}건", f"- FAIL: {failed}건",
        "- 최종 판정: " + ("전처리 완료" if failed == 0 else "재검토 필요"), "",
        "## 공통 기준", "",
        "- Hackle 관측기간: 2023-07-18~2023-08-10, 원시 시각 기준 24일",
        "- 방문 세션: 원래 session_id가 아닌 30분 비활동 기준 방문 ID",
        "- 사용자: 하나의 서비스 계정으로 안전하게 확인된 경우만 귀속",
        "- 시간대: 공식 확인 전까지 원시 시각 보존, +9시간 미적용",
        "- DB 결제와 Hackle 구매완료: 서버 거래와 클라이언트 UX로 분리, 합산 금지",
        "- 활동 마트: 전체 날짜 패널이 아닌 기록일 기반 희소 마트", "",
        "## 정제 산출물", "",
    ]
    for row in processing.to_dict("records"):
        lines.append(f"- `{Path(str(row['output_path'])).name}`: {int(row['output_rows']):,}행, {int(row['output_columns']):,}열")
    lines += ["", "## 주의사항", "",
              "- Hackle과 DB의 상대적 9시간 차이는 공식 시간대 증명이 아니므로 정제값에는 반영하지 않았습니다.",
              "- 사용자 미식별·충돌 이벤트는 삭제하지 않고 미귀속 상태로 보존했습니다.",
              "- 누적상태의 Hackle 0은 1년 미활동이 아니라 24일 내 식별 이벤트 없음입니다.",
              "- 가입일보다 앞선 Hackle 활동은 삭제하거나 0일로 덮지 않고 원천 시각·계정 연결 이상 플래그로 보존했습니다.",
              "- 현재 학교·학년·반·차단·기기 프로필은 이벤트 당시 상태로 사용하지 않습니다.",
              "- 안전·가입·출석·프로모션·라이프사이클 원장은 Hackle 원천이 아니므로 이번 묶음에서 변경하지 않았습니다."]
    report_path.write_text("\n".join(lines), encoding="utf-8")
    if failed:
        raise AssertionError(f"Hackle 통합 전처리 QA 실패 {failed}건")
    return {"qa": qa, "processing": processing, "dictionary": dictionary, "scope": scope,
            "pass": passed, "warn": warned, "fail": failed, "report_path": report_path}
