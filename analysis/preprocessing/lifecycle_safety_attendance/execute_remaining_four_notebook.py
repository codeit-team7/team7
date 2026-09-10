from pathlib import Path

import nbformat
from nbclient import NotebookClient


HERE = Path(__file__).resolve().parent
NOTEBOOK_PATH = HERE / "promo_safety_lifecycle_attendance_4marts_preprocessing.ipynb"

notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
client = NotebookClient(
    notebook,
    timeout=None,
    kernel_name="python3",
    resources={"metadata": {"path": str(HERE)}},
    allow_errors=False,
)
client.execute()
nbformat.write(notebook, NOTEBOOK_PATH)
print(f"실행 결과 저장: {NOTEBOOK_PATH}")
