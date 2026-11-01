"""Own GrpcCall subclasses, `target` and `wait_for_ready` reach the hooks in sync and aio channels."""

import contextvars
from dataclasses import dataclass

import grpc
import pytest
from helpers import run, stub, stubs, sv

from grpc_observer import AsyncGrpcObserver, GrpcCall, GrpcObserver, GrpcResult

trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="-")


@dataclass
class TracedCall(GrpcCall):
    trace_id: str = ""
    messages: int = 0


class TracedObserver(GrpcObserver[TracedCall]):
    def __init__(self):
        self.calls: list[TracedCall] = []

    def on_call_start(self, call: TracedCall) -> None:
        call.trace_id = trace_id.get()

    def on_response_message(self, call: TracedCall, message, index: int) -> None:
        call.messages += 1

    def on_call_end(self, call: TracedCall, result: GrpcResult) -> None:
        self.calls.append(call)


class AsyncTracedObserver(AsyncGrpcObserver[TracedCall]):
    def __init__(self):
        self.calls: list[TracedCall] = []

    async def on_call_start(self, call: TracedCall) -> None:
        call.trace_id = trace_id.get()

    async def on_response_message(self, call: TracedCall, message, index: int) -> None:
        call.messages += 1

    async def on_call_end(self, call: TracedCall, result: GrpcResult) -> None:
        self.calls.append(call)


def test_call_class_from_generic_parameter():
    assert TracedObserver.call_class is TracedCall
    assert AsyncTracedObserver.call_class is TracedCall
    assert GrpcObserver.call_class is GrpcCall

    class Child(TracedObserver):  # inherited, not reset to GrpcCall
        pass

    assert Child.call_class is TracedCall


def test_explicit_call_class():
    class Explicit(GrpcObserver):
        call_class = TracedCall

    assert Explicit.call_class is TracedCall


def test_wrong_call_class_fails_at_class_definition():
    with pytest.raises(TypeError, match="must be a subclass of GrpcCall"):

        class Wrong(GrpcObserver):
            call_class = dict


def test_sync_custom_call(target):
    observer = TracedObserver()
    token = trace_id.set("t-sync")
    try:
        with grpc.insecure_channel(target) as raw:
            uu, us, *_ = stubs(grpc.intercept_channel(raw, observer.interceptor(target=target)))
            uu(sv("a"), wait_for_ready=True)
            list(us(sv("b")))
    finally:
        trace_id.reset(token)
    first, second = observer.calls
    assert all(isinstance(call, TracedCall) for call in observer.calls)
    assert (first.trace_id, first.target, first.wait_for_ready, first.messages) == ("t-sync", target, True, 0)
    assert (second.trace_id, second.wait_for_ready, second.messages) == ("t-sync", None, 3)


@pytest.mark.parametrize("observer_cls", [TracedObserver, AsyncTracedObserver])
def test_aio_custom_call(target, observer_cls):
    observer = observer_cls()

    async def scenario():
        trace_id.set("t-aio")
        async with grpc.aio.insecure_channel(target, interceptors=observer.aio_interceptors(target=target)) as ch:
            await stub(ch, "unary_unary", "UU")(sv("a"), wait_for_ready=True)
            async for _ in stub(ch, "unary_stream", "US")(sv("b")):
                pass

    run(scenario())
    first, second = observer.calls
    assert (first.trace_id, first.target, first.wait_for_ready, first.messages) == ("t-aio", target, True, 0)
    assert (second.trace_id, second.target, second.messages) == ("t-aio", target, 3)


def test_target_defaults_to_none(target):
    observer = TracedObserver()
    with grpc.insecure_channel(target) as raw:
        stubs(grpc.intercept_channel(raw, observer.interceptor()))[0](sv("a"))
    assert observer.calls[0].target is None
