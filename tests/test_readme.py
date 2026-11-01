"""Every ```python block of README.md runs, in order, in one namespace (`target` is a live test server)."""

import re
from pathlib import Path

README = Path(__file__).resolve().parent.parent / "README.md"


def test_readme_examples_run(target, capsys):
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), flags=re.S)
    assert len(blocks) >= 5
    namespace = {"target": target, "__name__": "readme"}
    for number, block in enumerate(blocks, 1):
        exec(compile(block, f"README.md block {number}", "exec"), namespace)
    out = capsys.readouterr().out
    assert "-> /echo.Echo/Echo hi metadata: {'x-client': 'readme'}" in out
    assert "<- /echo.Echo/Echo HI" in out
    assert "initial metadata: {'x-server-version': '1.4.2'}" in out
    assert "trailing metadata: {'x-request-cost': '3'}" in out
    assert f"[4bf92f35] {target}/echo.Echo/Echo OK" in out
