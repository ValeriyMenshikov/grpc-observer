"""Every file in examples/ runs as a script, and the observers work inside pytest."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import grpc
import pytest
from helpers import stubs, sv

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

SCRIPTS = {
    "logging_observer": ['"type": "end", "status_code": "OK"', '"status_code": "INVALID_ARGUMENT"'],
    "allure_observer": ["gRPC /example.Echo/Chat → OK", "gRPC /example.Echo/Echo → INVALID_ARGUMENT"],
}
EXTRA_DEPS = {"allure_observer": "allure"}


@pytest.mark.parametrize("name", sorted(SCRIPTS))
def test_example_script_runs(name):
    if name in EXTRA_DEPS:
        pytest.importorskip(EXTRA_DEPS[name])
    proc = subprocess.run(
        [sys.executable, str(EXAMPLES / f"{name}.py")], capture_output=True, text=True, timeout=60, check=False
    )
    assert proc.returncode == 0, proc.stderr
    output = proc.stdout + proc.stderr
    for expected in SCRIPTS[name]:
        assert expected in output


def test_gif_demo_runs():
    """docs/demo/demo.py is what docs/demo/demo.gif shows."""
    proc = subprocess.run(
        [sys.executable, str(EXAMPLES.parent / "docs" / "demo" / "demo.py")],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert 'trailing_metadata={"x-request-cost": "3"}' in proc.stdout
    assert 'details="boom"' in proc.stdout


def _load(name):
    spec = importlib.util.spec_from_file_location(f"examples_{name}", EXAMPLES / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_logging_observer_records(target, caplog):
    observer = _load("logging_observer").LoggingObserver()
    with caplog.at_level("INFO", logger="grpc"), grpc.insecure_channel(target) as raw:
        uu, us, *_ = stubs(grpc.intercept_channel(raw, observer.interceptor()))
        uu(sv("a"))
        list(us(sv("b")))
        with pytest.raises(grpc.RpcError):
            uu(sv("fail"))
    events = [r.grpc for r in caplog.records]
    assert [e["type"] for e in events if e["method"].endswith("/UU")] == [
        "request",
        "response",
        "end",
        "request",
        "error",
    ]
    assert events[-1]["status_code"] == "INVALID_ARGUMENT"
    assert {e["type"] for e in events if e["method"].endswith("/US")} >= {"stream_message", "end"}


def test_allure_observer_in_pytest(target):
    pytest.importorskip("allure")
    observer = _load("allure_observer").AllureObserver(stream_tail=2)
    with grpc.insecure_channel(target) as raw:
        uu, us, su, ss = stubs(grpc.intercept_channel(raw, observer.interceptor()))
        uu(sv("a"))
        list(us(sv("b")))
        su(iter([sv("c")]))
        list(ss(iter([sv("d")])))
        with pytest.raises(grpc.RpcError):
            uu(sv("fail"))
