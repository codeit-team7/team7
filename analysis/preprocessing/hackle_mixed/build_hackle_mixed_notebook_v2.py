from pathlib import Path

import nbformat as nbf


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "hackle_and_mixed_4marts_preprocessing.ipynb"


def md(text: str):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text: str):
    return nbf.v4.new_code_cell(text.strip())


cells = [
    md(
        """
# Hackle 및 혼합 마트 4개 통합 전처리

Hackle 24일 이벤트 원장과 Hackle 집계가 섞인 가치·활동·누적상태 마트를
같은 관측기간, 사용자 식별, 30분 방문 기준으로 정제하고 서로 대사한다.

## 처리 대상

| 마트 | 한 행의 의미 | Hackle 포함 방식 |
|---|---|---|
| `fact_hackle_event_24d_v2` | Hackle 원시 이벤트 1건 | 전체가 Hackle |
| `mart_value_event_v2` | 포인트·결제·프로모션·구매 UX 이벤트 1건 | 구매 UX만 Hackle |
| `mart_user_activity_daily_v2` | 관측 기록이 존재하는 사용자×일자 1행 | Hackle 일별 건수 포함 |
| `mart_user_cumulative_state_1y_v2` | 전체 계정 사용자 1명 1행 | Hackle 24일 누적 건수 포함 |

이 네 개를 함께 처리해야 `원시 이벤트 → 30분 방문 → 사용자 일별 → 사용자 누적`의 숫자가
동일한 기준으로 이어지는지 검증할 수 있다.
"""
    ),
    md(
        """
## 공통 전처리 기준

1. Hackle 원시 관측기간은 `2023-07-18~2023-08-10` 24일이다.
2. 공식 시간대는 확인되지 않았으므로 원시 시각을 보존하고 `+9시간`을 적용하지 않는다.
3. 원래 `session_id`는 며칠씩 이어질 수 있어 방문으로 쓰지 않고 30분 비활동 기준 방문 ID를 사용한다.
4. 세션에서 사용자 ID가 하나로 안전하게 확정된 경우만 서비스 사용자에게 귀속한다.
5. DB 결제 성공과 Hackle 구매 완료는 서버 거래/클라이언트 UX로 분리하고 합산하지 않는다.
6. 일별 활동 마트는 전체 날짜 패널이 아니라 기록이 존재하는 날짜만 담은 희소 마트다.
7. 원본 행과 컬럼은 삭제하지 않고, 해석 플래그와 범위 라벨을 추가한다.
8. 가입일보다 앞선 활동은 임의 삭제·날짜 보정하지 않고 원천 시각·계정 연결 이상으로 표시한다.
"""
    ),
    code(
        """
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


HERE = find_repo_root() / "analysis" / "preprocessing" / "hackle_mixed"
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from hackle_mixed_core import (
    prepare_context,
    process_hackle_base_streaming as process_hackle_base,
    process_value,
    process_activity,
    process_cumulative,
    finalize,
)

pd.set_option("display.max_columns", 180)
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
## 1. Hackle 이벤트·30분 방문·사용자 일별 기준표

숫자 대체키로 저장된 이벤트명을 복원하고, 원시 이벤트 1,144만 건에 30분 방문 ID와
충돌 없는 사용자 ID를 붙인다. 원시 이벤트는 삭제하지 않으며 식별 불가능한 이벤트도
미귀속 상태로 보존한다. 이후 같은 원천에서 방문 1행 마트와 사용자×일자 기준표를 만든다.
"""
    ),
    code(
        """
hackle_result = process_hackle_base(ctx)
display(pd.DataFrame([hackle_result]))
print("Hackle 원시 이벤트, 30분 방문, 사용자 일별 기준표 생성 완료")
"""
    ),
    md(
        """
## 2. 가치 행동 통합 원장

DB 포인트·결제·프로모션은 원래 기간과 모집단을 유지한다. Hackle 구매 UX 행에만
24일 범위와 30분 방문 기준을 적용한다. DB 결제 성공은 거래 기준, Hackle 구매완료는
화면 행동 기준으로 구분해 두 값을 더하지 않도록 표시한다.
"""
    ),
    code(
        """
value_result = process_value(ctx)
display(pd.DataFrame([{
    "전체 행": value_result["rows"],
    "Hackle 구매 UX 행": value_result["hackle_rows"],
}]))
display(pd.Series(value_result["event_counts"]).sort_values(ascending=False).rename_axis("event_type").to_frame("행 수"))
"""
    ),
    md(
        """
## 3. 사용자 일별 활동

Hackle이 찍힌 날, DB 기록만 있는 날, 두 원천이 모두 있는 날을 분리한다.
행이 없는 날짜를 비활성 또는 이탈로 간주하지 않도록 희소 마트라는 사실을 명시한다.
Hackle 관련 다섯 개 집계는 원시 이벤트·30분 방문에서 다시 계산한 값과 대사한다.
가입일보다 앞선 기록은 별도 플래그로 보존하고, 친구요청 상태합·활동 proxy·기록 관측·맥락/시스템 기록일
플래그가 원본 생성 SQL 정의와 같은지도 전 행 검증한다.
"""
    ),
    code(
        """
activity_result = process_activity(ctx)
display(pd.DataFrame([activity_result]))
print("사용자 일별 Hackle 건수와 원시 이벤트·방문 기준표 대사 완료")
"""
    ),
    md(
        """
## 4. 사용자 누적상태와 최종 교차 검증

Hackle 이벤트 누적값은 사용자 일별 이벤트 합계와 비교한다. 방문 누적값은 자정을 넘긴 한 방문을
두 번 세지 않도록 방문 시작일 기준 합계와 비교한다. 누적값이 0인 사용자는 1년 동안 미활동한 것이 아니라,
Hackle 24일 동안 서비스 계정으로 식별 가능한 이벤트가 없었던 사용자로만 해석한다.
가치 원장의 Hackle 구매 UX 건수도 원시 이벤트와 최종 대사한다.
"""
    ),
    code(
        """
cumulative_result = process_cumulative(ctx)
display(pd.DataFrame([cumulative_result]))
"""
    ),
    md(
        """
## 5. 최종 QA·컬럼 명세·범위표

`FAIL=0`이면 전처리 완료다. `WARN`은 숨기지 않은 원천 한계로, 미식별 사용자와 공식 시간대 미확정을
각 행의 라벨과 보고서에 남긴다.
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


nb = nbf.v4.new_notebook()
nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3"},
}
nbf.write(nb, NOTEBOOK_PATH)
print(f"노트북 생성: {NOTEBOOK_PATH}")
