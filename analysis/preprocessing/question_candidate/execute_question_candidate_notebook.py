from pathlib import Path

import nbformat
from nbclient import NotebookClient


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "question_candidate_4marts_preprocessing.ipynb"


def on_cell_start(cell, cell_index, **kwargs):
    first_line = cell.source.strip().splitlines()[0] if cell.source.strip() else "(empty)"
    print(f"[CELL {cell_index + 1}] 시작: {first_line[:100]}", flush=True)


def on_cell_complete(cell, cell_index, **kwargs):
    print(f"[CELL {cell_index + 1}] 커널 전달 완료, 계산 결과 대기", flush=True)


def on_cell_executed(cell, cell_index, execute_reply, **kwargs):
    print(f"[CELL {cell_index + 1}] 실제 실행 완료", flush=True)


nb = nbformat.read(NOTEBOOK_PATH, as_version=4)
client = NotebookClient(
    nb,
    timeout=7200,
    kernel_name="python3",
    resources={"metadata": {"path": str(HERE)}},
    on_cell_start=on_cell_start,
    on_cell_complete=on_cell_complete,
    on_cell_executed=on_cell_executed,
)
client.execute()
nbformat.write(nb, NOTEBOOK_PATH)
print(f"실행 완료 노트북 저장: {NOTEBOOK_PATH}", flush=True)
