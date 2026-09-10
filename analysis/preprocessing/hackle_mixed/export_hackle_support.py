from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


EXPORTS = {
    "dim_hackle_event_text_attribute_v2": """
        SELECT
            text_attribute_sk, attribute_type, attribute_value_raw,
            attribute_value_normalized, raw_value_state
        FROM votes_mart.dim_hackle_event_text_attribute_v2
        ORDER BY text_attribute_sk
    """,
    "dim_hackle_device_resolved_v2": """
        SELECT *
        FROM votes_mart.dim_hackle_device_resolved_v2
        ORDER BY device_sk
    """,
    "dim_hackle_user_resolved_v2": """
        SELECT *
        FROM votes_mart.dim_hackle_user_resolved_v2
        ORDER BY hackle_user_sk
    """,
    "dim_hackle_session_resolved_v2": """
        SELECT *
        FROM votes_mart.dim_hackle_session_resolved_v2
        ORDER BY session_sk
    """,
    "bridge_hackle_event_visit_assignment_v2": """
        SELECT
            event_sk, original_session_sk, session_partition_sk,
            event_order_in_original_session,
            previous_event_at_in_original_session,
            seconds_since_previous_event,
            is_break_15m, is_break_30m, is_break_60m,
            visit_sequence_15m, visit_sequence_30m, visit_sequence_60m,
            LOWER(HEX(derived_visit_id_30m)) AS analytics_visit_session_id,
            event_order_in_visit_30m
        FROM votes_mart.bridge_hackle_event_visit_assignment_v2
        ORDER BY event_sk
    """,
    "bridge_hackle_event_raw_id_v2": """
        SELECT
            f.event_sk,
            rid.attribute_value_raw AS raw_id_attribute
        FROM votes_mart.fact_hackle_event_24d_v2 AS f
        JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS rid
          ON rid.text_attribute_sk=f.raw_id_attribute_sk
        ORDER BY f.event_sk
    """,
    "dim_hackle_visit_30m_v2": """
        SELECT
            LOWER(HEX(derived_visit_id_30m)) AS analytics_visit_session_id,
            session_partition_sk, original_session_sk, visit_sequence_30m,
            visit_start_at_30m, visit_end_at_30m,
            visit_duration_seconds_30m, visit_event_count_30m,
            distinct_event_key_count, session_start_event_count,
            launch_app_count, question_start_count, question_complete_count,
            question_skip_count, ping_open_count, shop_view_count,
            purchase_click_count, purchase_complete_count
        FROM votes_mart.dim_hackle_visit_30m_v2
        ORDER BY derived_visit_id_30m
    """,
}


def mysql_unescape(value: str) -> str | None:
    """Decode mysql --batch escaping without confusing SQL NULL with empty text."""
    if value == "NULL":
        return None
    out: list[str] = []
    i = 0
    mapping = {"0": "\0", "n": "\n", "r": "\r", "t": "\t", "\\": "\\"}
    while i < len(value):
        if value[i] == "\\" and i + 1 < len(value):
            nxt = value[i + 1]
            out.append(mapping.get(nxt, nxt))
            i += 2
        else:
            out.append(value[i])
            i += 1
    return "".join(out)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def export_one(mysql_exe: Path, args: argparse.Namespace, name: str, query: str) -> dict:
    output_path = args.output_dir / f"{name}.csv.gz"
    done_path = args.output_dir / f".{name}.done.json"
    if output_path.exists() and done_path.exists() and not args.force:
        previous = json.loads(done_path.read_text(encoding="utf-8"))
        if previous.get("status") == "PASS" and previous.get("sha256") == sha256_file(output_path):
            print(f"[재사용] {name}: {previous['row_count']:,}행", flush=True)
            return previous

    command = [
        str(mysql_exe),
        f"--host={args.host}",
        f"--port={args.port}",
        f"--user={args.user}",
        "--batch",
        "--quick",
        "--default-character-set=utf8mb4",
        "--database=votes_mart",
        "--execute",
        " ".join(query.split()),
    ]
    started = time.time()
    print(f"[추출 시작] {name}", flush=True)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=os.environ.copy(),
        bufsize=1024 * 1024,
    )
    assert process.stdout is not None
    reader = csv.reader(process.stdout, delimiter="\t", quoting=csv.QUOTE_NONE)
    row_count = 0
    column_count = 0
    try:
        with gzip.open(output_path, "wt", encoding="utf-8-sig", newline="", compresslevel=3) as gz:
            writer = csv.writer(gz, lineterminator="\n")
            header = next(reader, None)
            if header is None:
                stderr_text = process.stderr.read() if process.stderr is not None else ""
                return_code = process.wait()
                raise RuntimeError(
                    f"{name}: MySQL이 CSV 헤더를 반환하지 않았습니다 "
                    f"(종료코드 {return_code}).\n{stderr_text.strip()}"
                )
            column_count = len(header)
            writer.writerow(header)
            for raw_row in reader:
                if len(raw_row) != column_count:
                    raise RuntimeError(
                        f"{name}: 열 수 불일치 ({len(raw_row)} != {column_count}), row={row_count + 1}"
                    )
                writer.writerow([mysql_unescape(v) for v in raw_row])
                row_count += 1
                if row_count % 1_000_000 == 0:
                    print(f"  {name}: {row_count:,}행", flush=True)
    except Exception:
        process.kill()
        output_path.unlink(missing_ok=True)
        raise

    stderr_text = process.stderr.read() if process.stderr is not None else ""
    return_code = process.wait()
    if return_code != 0:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(f"MySQL 추출 실패: {name}\n{stderr_text.strip()}")

    result = {
        "object_name": name,
        "row_count": row_count,
        "column_count": column_count,
        "compressed_bytes": output_path.stat().st_size,
        "sha256": sha256_file(output_path),
        "elapsed_seconds": round(time.time() - started, 3),
        "status": "PASS",
        "output_path": str(output_path),
    }
    done_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[추출 완료] {name}: {row_count:,}행, {column_count}열, "
        f"{result['compressed_bytes'] / 1024**2:,.1f}MB",
        flush=True,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql-exe", type=Path, required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not os.environ.get("MYSQL_PWD"):
        raise RuntimeError("MYSQL_PWD가 현재 프로세스에만 설정되어 있어야 합니다.")
    if not args.mysql_exe.exists():
        raise FileNotFoundError(args.mysql_exe)

    results = []
    for name, query in EXPORTS.items():
        results.append(export_one(args.mysql_exe, args, name, query))

    manifest_path = args.output_dir / "hackle_support_export_manifest.csv"
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f"지원 테이블 추출 검증표: {manifest_path}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[실패] {exc}", file=sys.stderr, flush=True)
        raise
