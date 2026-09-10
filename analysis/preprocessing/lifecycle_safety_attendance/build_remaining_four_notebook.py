from pathlib import Path

import nbformat as nbf


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "promo_safety_lifecycle_attendance_4marts_preprocessing.ipynb"


def md(text: str):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text: str):
    return nbf.v4.new_code_cell(text.strip())


cells = [
    md(
        """
# 프로모션·안전·생애주기·출석 4개 마트 전처리

18개 신규 분석 객체 중 마지막 네 개 마트를 기존 전처리 기준과 동일하게 정제한다.
원본 행과 원문은 삭제하지 않고, 공통 사용자 677,085명·기존 가치 원장·질문 원장·사용자 일별·누적상태와 교차대사한다.

## 처리 대상

| 마트 | 한 행의 의미 | 핵심 기준 |
|---|---|---|
| `mart_lifecycle_event_v2` | 가입 또는 탈퇴 원본 1건 | 탈퇴 user_id 부재를 강제 연결하지 않음 |
| `mart_promo_event_receipt_v2` | 프로모션 지급 원본 1건 | 지급과 노출·참여를 구분 |
| `mart_safety_event_v2` | 안전·피드백 원본 또는 누적 스냅샷 1건 | 원행 수와 report_count 분리 |
| `mart_attendance_record_v2` | 사용자별 출석 날짜 JSON 원행 1건 | 원행은 출석 하루가 아님 |

출석 날짜별 분석을 위해 `bridge_attendance_day_v2`를 파생 기준표로 함께 생성한다.
"""
    ),
    md(
        """
## 공통 전처리 원칙

1. SQL NULL 문자열 `\\N`을 실제 결측으로 변환한다.
2. 원본 ID·원문·시각·JSON·행 수를 보존한다.
3. 공통 사용자 기준은 전처리 완료된 677,085명 계정 원장이다.
4. 이상행은 임의 삭제하거나 다른 사용자·날짜로 수정하지 않고 검토 플래그를 추가한다.
5. 서로 다른 모집단·기간·측정 단위를 한 비율의 분자와 분모로 합치지 않는다.
6. 이전 전처리 산출물과 사용자별·사건별·날짜별 합계를 교차검증한다.
"""
    ),
    code(
        r"""
from pathlib import Path
import sys
import pandas as pd
from IPython.display import display, Markdown

def find_repo_root(start: Path | None = None) -> Path:
    start = (start or Path.cwd()).resolve()
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    raise FileNotFoundError("Git 저장소 루트를 찾지 못했습니다.")


HERE = find_repo_root() / "analysis" / "preprocessing" / "lifecycle_safety_attendance"
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from remaining_four_core import (
    prepare_context,
    process_lifecycle,
    process_promo,
    process_safety,
    process_attendance,
    finalize,
)

pd.set_option("display.max_columns", 120)
pd.set_option("display.max_rows", 160)
pd.set_option("display.width", 260)

ctx = prepare_context()
print(f"원본 폴더: {ctx.source_dir}")
print(f"정제 폴더: {ctx.output_dir}")
display(ctx.source_manifest[["object_name", "exported_rows", "column_count", "status", "sha256"]].reset_index(drop=True))
"""
    ),
    md(
        """
## 1. 가입·탈퇴 생애주기

가입 677,085건은 공통 사용자 원장과 사용자 집합·가입시각을 대조한다.
탈퇴 70,764건은 원천에 사용자 ID가 없으므로 개인 가입자에게 연결하지 않고, 집계 추세와 사유 분석 용도로만 표시한다.
"""
    ),
    code(
        """
lifecycle_result = process_lifecycle(ctx)
display(pd.DataFrame([lifecycle_result]))
"""
    ),
    md(
        """
## 2. 프로모션 포인트 지급

지급 309건을 공통 사용자 원장·가치 행동 통합 원장·사용자 누적상태와 대사한다.
동일 사용자의 다회 지급은 삭제하지 않고 횟수와 순위를 제공한다. 지급 기록에는 노출·참여 분모가 없으므로 전환율 계산 불가를 명시한다.
"""
    ),
    code(
        """
promo_result = process_promo(ctx)
display(pd.DataFrame([promo_result]))
"""
    ),
    md(
        """
## 3. 안전·피드백 원장

질문 피드백, 차단, 타임라인 신고, Ping 신고 누적 스냅샷을 구분한다.
원문 사유를 보존하면서 규칙 기반 분석 범주를 추가하되 실제 피해 발생을 확정하지 않는다.
Ping 스냅샷은 원행 수 169건과 누적 report_count 215건을 별도 측정값으로 유지한다.
"""
    ),
    code(
        """
safety_result = process_safety(ctx)
display(pd.DataFrame([safety_result]))
"""
    ),
    md(
        """
## 4. 출석 원행과 날짜 bridge

사용자별 JSON 원행 349,637개를 보존하고 JSON 유효성·배열 여부·원소 수를 재계산한다.
배열 안의 날짜 원소는 순서를 유지한 `bridge_attendance_day_v2`로 전개하고,
사용자 일별 활동 및 누적상태의 출석 집계와 다시 대조한다.
"""
    ),
    code(
        """
attendance_result = process_attendance(ctx)
display(pd.DataFrame([attendance_result]))
"""
    ),
    md(
        """
## 5. 최종 QA·컬럼 명세·모집단 범위

`FAIL=0`이면 현재 원천으로 수행 가능한 전처리가 완료된 것이다.
`WARN`은 삭제하거나 추정값으로 덮지 않은 원천 한계와 검토 후보이며, 해당 행에 분석 통제 플래그가 포함되어 있다.
"""
    ),
    code(
        """
final_result = finalize(ctx)
display(Markdown(f"### QA 결과: PASS {final_result['pass']} / WARN {final_result['warn']} / FAIL {final_result['fail']}"))
display(final_result["qa"][["mart", "test_name", "actual", "expected", "status", "severity"]])
display(final_result["processing"])
display(final_result["scope"])
print(f"최종 보고서: {final_result['report_path']}")
"""
    ),
]

notebook = nbf.v4.new_notebook()
notebook["cells"] = cells
notebook["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3"},
}
nbf.write(notebook, NOTEBOOK_PATH)
print(f"노트북 생성: {NOTEBOOK_PATH}")
