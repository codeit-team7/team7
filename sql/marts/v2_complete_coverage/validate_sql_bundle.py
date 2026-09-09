"""Lightweight offline checks for the v2 MySQL build bundle.

This does not replace execution against MySQL.  It catches truncated comments,
unbalanced parentheses, missing SOURCE files, and accidental writes to the seven
legacy mart names before a long database build is started.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LEGACY_NAMES = {
    "mart_user_network_snapshot",
    "mart_user_journey_funnel_1y",
    "mart_user_activity_daily",
    "mart_safety_event",
    "mart_question_exposure",
    "mart_point_payment_event",
    "mart_funnel_session",
}

RAW_COLUMNS = {
    "accounts_attendance": {"id", "attendance_date_list", "user_id"},
    "accounts_blockrecord": {"id", "reason", "created_at", "block_user_id", "user_id"},
    "accounts_failpaymenthistory": {"id", "productId", "phone_type", "created_at", "user_id"},
    "accounts_friendrequest": {"id", "status", "created_at", "updated_at", "receive_user_id", "send_user_id"},
    "accounts_group": {"id", "grade", "class_num", "school_id"},
    "accounts_nearbyschool": {"id", "distance", "nearby_school_id", "school_id"},
    "accounts_paymenthistory": {"id", "productId", "phone_type", "created_at", "user_id"},
    "accounts_user_contacts": {"id", "contacts_count", "invite_user_id_list", "user_id"},
    "accounts_pointhistory": {"id", "delta_point", "created_at", "user_id", "user_question_record_id"},
    "accounts_school": {"id", "address", "student_count", "school_type"},
    "accounts_timelinereport": {"id", "reason", "created_at", "reported_user_id", "user_id", "user_question_record_id"},
    "accounts_user": {"id", "is_superuser", "is_staff", "gender", "point", "friend_id_list", "is_push_on", "created_at", "block_user_id_list", "hide_user_id_list", "ban_status", "report_count", "alarm_count", "pending_chat", "pending_votes", "group_id"},
    "accounts_userquestionrecord": {"id", "status", "created_at", "chosen_user_id", "question_id", "user_id", "question_piece_id", "has_read", "answer_status", "answer_updated_at", "report_count", "opened_times"},
    "accounts_userwithdraw": {"id", "reason", "created_at"},
    "event_receipts": {"id", "created_at", "event_id", "user_id", "plus_point"},
    "events": {"id", "title", "plus_point", "event_type", "is_expired", "created_at"},
    "polls_question": {"id", "question_text", "created_at"},
    "polls_questionpiece": {"id", "is_voted", "created_at", "question_id", "is_skipped"},
    "polls_questionreport": {"id", "reason", "created_at", "question_id", "user_id"},
    "polls_questionset": {"id", "question_piece_id_list", "opening_time", "status", "created_at", "user_id"},
    "polls_usercandidate": {"id", "created_at", "question_piece_id", "user_id"},
    "hackle_properties": {"id", "session_id", "user_id", "language", "osname", "osversion", "versionname", "device_id"},
    "device_properties": {"id", "device_id", "device_model", "device_vendor"},
    "hackle_events": {"event_id", "event_datetime", "event_key", "session_id", "id", "item_name", "page_name", "friend_count", "votes_count", "heart_balance", "question_id"},
    "user_properties": {"user_id", "class", "gender", "grade", "school_id"},
}


def strip_non_code(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    return sql


def check_parentheses(path: Path) -> list[str]:
    sql = strip_non_code(path.read_text(encoding="utf-8"))
    balance = 0
    errors: list[str] = []
    for offset, char in enumerate(sql):
        if char == "(":
            balance += 1
        elif char == ")":
            balance -= 1
            if balance < 0:
                errors.append(f"unexpected ')' near character {offset}")
                balance = 0
    if balance:
        errors.append(f"unclosed parentheses: {balance}")
    return errors


def check_legacy_writes(path: Path) -> list[str]:
    sql = strip_non_code(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    pattern = re.compile(
        r"(?:DROP\s+TABLE|CREATE\s+TABLE|ALTER\s+TABLE|INSERT\s+INTO)\s+"
        r"(?:`?votes_mart`?\.)?`?([A-Za-z0-9_가-힣()]+)`?",
        flags=re.I,
    )
    for match in pattern.finditer(sql):
        if match.group(1) in LEGACY_NAMES:
            errors.append(f"legacy mart write found: {match.group(1)}")
    return errors


def check_raw_column_references(path: Path) -> list[str]:
    """Validate aliases that directly read final.<table> within each statement."""
    sql = strip_non_code(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    for statement_number, statement in enumerate(sql.split(";"), start=1):
        aliases = re.findall(
            r"(?:FROM|JOIN)\s+final\.([A-Za-z_][A-Za-z0-9_]*)"
            r"\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)",
            statement,
            flags=re.I,
        )
        for table, alias in aliases:
            expected = RAW_COLUMNS.get(table.lower())
            if expected is None:
                errors.append(f"unknown raw table final.{table} in statement {statement_number}")
                continue
            used = set(re.findall(rf"\b{re.escape(alias)}\.([A-Za-z_][A-Za-z0-9_]*)", statement))
            for column in sorted(used - expected):
                errors.append(
                    f"final.{table} alias {alias} references unknown column "
                    f"{column} in statement {statement_number}"
                )
    return errors


def check_source_manifests() -> list[str]:
    errors = []
    expected_source_counts = {
        "08_build_all_v2.sql": 7,
        "08d_build_stage_3_recover_v2.sql": 4,
        "08e_build_stage_3_activity_recover_v2.sql": 3,
        "08f_build_stage_3_activity_resume_after5_v2.sql": 3,
        "08g_build_stage_3_activity_resume_after7_v2.sql": 4,
    }
    for build_name, expected_count in expected_source_counts.items():
        build = (ROOT / build_name).read_text(encoding="utf-8")
        referenced = re.findall(r"SOURCE\s+([^;]+);", strip_non_code(build), flags=re.I)
        for raw in referenced:
            name = Path(raw.strip().replace("/", "\\")).name
            if not (ROOT / name).exists():
                errors.append(f"{build_name}: missing SOURCE file: {name}")
        if len(referenced) != expected_count:
            errors.append(
                f"{build_name}: expected {expected_count} SOURCE statements, "
                f"found {len(referenced)}"
            )
    return errors


def main() -> None:
    failures: list[str] = []
    sql_files = sorted(
        set(ROOT.glob("[0-9][0-9]_*.sql"))
        | set(ROOT.glob("02z_*.sql"))
        | set(ROOT.glob("04[bc]_*.sql"))
        | set(ROOT.glob("05[cd]_*.sql"))
        | set(ROOT.glob("06[defgh]_*.sql"))
        | set(ROOT.glob("08[abcdefg]*_*.sql"))
    )
    for path in sql_files:
        errors = (
            check_parentheses(path)
            + check_legacy_writes(path)
            + check_raw_column_references(path)
        )
        if errors:
            failures.extend(f"{path.name}: {error}" for error in errors)
        else:
            print(f"PASS  {path.name}")
    failures.extend(check_source_manifests())
    if failures:
        print("\nFAIL")
        for failure in failures:
            print(f"- {failure}")
        raise SystemExit(1)
    print("\nAll offline bundle checks passed.")


if __name__ == "__main__":
    main()
