from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

README = Path(__file__).resolve().parents[1] / "README.md"

_PYTHON_BLOCK = re.compile(
    r"^```python\n(.*?)^```\s*$",
    flags=re.MULTILINE | re.DOTALL,
)


def _python_examples() -> list[str]:
    return [str(match) for match in _PYTHON_BLOCK.findall(README.read_text(encoding="utf-8"))]


EXAMPLES = list(enumerate(_python_examples()))


def _is_pytest_module(source: str) -> bool:
    return any(line.startswith("def test_") for line in source.splitlines())


@pytest.mark.parametrize(("index", "source"), EXAMPLES, ids=[str(i) for i, _ in EXAMPLES])
def test_readme_python_example_runs(index: int, source: str, tmp_path: Path) -> None:
    """Every README Python block must run exactly as written."""
    if _is_pytest_module(source):
        module = tmp_path / f"test_readme_{index}.py"
        module.write_text(source, encoding="utf-8")
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            str(module),
        ]
    else:
        script = tmp_path / f"readme_{index}.py"
        script.write_text(source, encoding="utf-8")
        command = [sys.executable, str(script)]

    completed = subprocess.run(
        command,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, (
        "README example failed:\n"
        f"--- stdout ---\n{completed.stdout}\n"
        f"--- stderr ---\n{completed.stderr}"
    )
