"""Cancellation, deadlines, async request iterators, several observers, broken async hooks.

Every test checks that `on_call_end` fired exactly once and with the right status code.
"""

import asyncio

import grpc
import pytest
from helpers import AsyncRecorder, Recorder, run, stub, stubs, sv

from grpc_observer import AsyncGrpcObserver, GrpcObserver

# Before grpcio 1.83 closing a grpc.aio channel does not end the calls still running on it.
CLOSE_ENDS_CALLS = tuple(int(part) for part in grpc.__version__.split(".")[:2]) >= (1, 83)


def _endless(channel):
    return stub(channel, "unary_stream", "Endless")


def _slow(channel):
    return stub(channel, "unary_unary", "Slow")


async def _settle():
    for _ in range(20):
        await asyncio.sleep(0.01)


def _single_end(rec, code):
    ends = rec.ends()
    assert len(ends) == 1, rec.events
    assert ends[0][1] == code
    return ends[0]


# ---------- cancel() ----------


def test_sync_response_stream_cancel(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        call = _endless(grpc.intercept_channel(raw, rec.interceptor()))(sv("c"))
        assert next(call).value == "c-0"
        assert call.cancel()
        with pytest.raises(grpc.RpcError):
            list(call)
    assert _single_end(rec, "CANCELLED") == ("end", "CANCELLED", 0, 1)


def test_sync_bidi_stream_cancel(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        *_, ss = stubs(grpc.intercept_channel(raw, rec.interceptor()))
        call = ss(iter([sv("a"), sv("b"), sv("c")]))
        assert next(call).value == "A"
        call.cancel()
        with pytest.raises(grpc.RpcError):
            list(call)
    _single_end(rec, "CANCELLED")


def test_sync_unary_future_cancel(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        future = _slow(grpc.intercept_channel(raw, rec.interceptor())).future(sv("s"))
        assert future.cancel()
        with pytest.raises(grpc.FutureCancelledError):
            future.result()
    _single_end(rec, "CANCELLED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_response_stream_cancel(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            call = _endless(channel)(sv("c"))
            assert (await call.read()).value == "c-0"
            assert call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call.read()
            await _settle()
        return rec

    _single_end(run(scenario()), "CANCELLED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_bidi_stream_cancel(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            *_, ss = stubs(channel)
            call = ss()
            await call.write(sv("a"))
            assert (await call.read()).value == "A"
            call.cancel()
            await _settle()
        return rec

    _single_end(run(scenario()), "CANCELLED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_stream_unary_cancel(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            _, _, su, _ = stubs(channel)
            call = su()
            await call.write(sv("a"))
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
            await _settle()
        return rec

    _single_end(run(scenario()), "CANCELLED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_unary_task_cancel(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            task = asyncio.ensure_future(_slow(channel)(sv("s")))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await _settle()
        return rec

    _single_end(run(scenario()), "CANCELLED")


# ---------- break out of `async for` ----------


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_break_then_cancel(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            call = _endless(channel)(sv("b"))
            async for message in call:
                assert message.value == "b-0"
                break
            call.cancel()
            await _settle()
            assert _single_end(rec, "CANCELLED") == ("end", "CANCELLED", 0, 1)
        return rec

    _single_end(run(scenario()), "CANCELLED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_break_then_channel_close(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            async for _ in _endless(channel)(sv("b")):
                break
            await _settle()
            assert rec.ends() == [], "the RPC is still running on the server, grpc does not cancel it on break"
        await _settle()
        if CLOSE_ENDS_CALLS:
            assert _single_end(rec, "CANCELLED") == ("end", "CANCELLED", 0, 1)
        return rec

    # On older grpcio the stream is finished when the event loop shuts down.
    assert _single_end(run(scenario()), "CANCELLED") == ("end", "CANCELLED", 0, 1)


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_break_from_finite_stream(target, recorder_cls):
    """The server already sent everything, but grpc reads the status only when the client reads on."""

    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            _, us, *_ = stubs(channel)
            async for _ in us(sv("b")):
                break
            await _settle()
            assert rec.ends() == []
        await _settle()
        return rec

    assert _single_end(run(scenario()), "CANCELLED") == ("end", "CANCELLED", 0, 1)


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_unclosed_stream_at_loop_shutdown(target, recorder_cls):
    """Stream abandoned and channel never closed: the loop closes the wrapper with GeneratorExit."""
    rec = recorder_cls()
    keep = []

    async def scenario():
        channel = grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors())
        call = _endless(channel)(sv("g"))
        async for _ in call:
            break
        await _settle()
        assert rec.ends() == []
        keep.extend((channel, call))

    run(scenario())
    assert _single_end(rec, "CANCELLED") == ("end", "CANCELLED", 0, 1)
    assert rec.last_result.details == "response stream was closed before the end"


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_stream_unary_cancel_without_await(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            _, _, su, _ = stubs(channel)
            call = su()
            await call.write(sv("a"))
            call.cancel()
            await _settle()
            _single_end(rec, "CANCELLED")
        return rec

    _single_end(run(scenario()), "CANCELLED")


# ---------- deadline exceeded ----------


def test_sync_deadline_exceeded(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        channel = grpc.intercept_channel(raw, rec.interceptor())
        with pytest.raises(grpc.RpcError) as err:
            _slow(channel)(sv("s"), timeout=0.1)
        assert err.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert rec.events[0][0] == "start"
        _single_end(rec, "DEADLINE_EXCEEDED")
        assert ("error", "DEADLINE_EXCEEDED", "Deadline Exceeded") in rec.events

        rec.events.clear()
        with pytest.raises(grpc.RpcError):
            list(_endless(channel)(sv("e"), timeout=0.1))
        _single_end(rec, "DEADLINE_EXCEEDED")


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_deadline_exceeded(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await _slow(channel)(sv("s"), timeout=0.1)
            assert err.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            _single_end(rec, "DEADLINE_EXCEEDED")

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                [m async for m in _endless(channel)(sv("e"), timeout=0.1)]
            _single_end(rec, "DEADLINE_EXCEEDED")

    run(scenario())


def test_timeout_is_visible_to_observer(target):
    seen = []

    class TimeoutObserver(GrpcObserver):
        def on_call_start(self, call):
            seen.append(call.timeout)

    with grpc.insecure_channel(target) as raw:
        uu, *_ = stubs(grpc.intercept_channel(raw, TimeoutObserver().interceptor()))
        uu(sv("a"), timeout=5)
    assert seen == [5]


# ---------- async request iterator ----------


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_async_request_iterator(target, recorder_cls):
    async def requests(*values):
        for value in values:
            await asyncio.sleep(0)
            yield sv(value)

    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            _, _, su, ss = stubs(channel)
            assert (await su(requests("x", "y"))).value == "x,y"
            assert rec.events[-1] == ("end", "OK", 2, 0)
            assert [e for e in rec.events if e[0] == "request_message"] == [
                ("request_message", 0, "x"),
                ("request_message", 1, "y"),
            ]

            rec.events.clear()
            assert [m.value async for m in ss(requests("p", "q", "r"))] == ["P", "Q", "R"]
            assert rec.ends() == [("end", "OK", 3, 3)]

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                await su(requests("ok", "fail"))
            assert rec.ends() == [("end", "INVALID_ARGUMENT", 2, 0)]

    run(scenario())


# ---------- several observers on one channel ----------


def test_sync_two_observers(target):
    first, second = Recorder(), Recorder()
    with grpc.insecure_channel(target) as raw:
        uu, us, *_ = stubs(grpc.intercept_channel(raw, first.interceptor(), second.interceptor()))
        assert uu(sv("a")).value == "A"
        assert [m.value for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
        with pytest.raises(grpc.RpcError):
            uu(sv("fail"))

    for rec in (first, second):
        assert rec.ends() == [("end", "OK", 0, 0), ("end", "OK", 0, 3), ("end", "INVALID_ARGUMENT", 0, 0)]
    assert first.events == second.events
    assert all(a is not b for a, b in zip(first.results, second.results, strict=True))


def test_aio_two_observers_sync_and_async(target):
    async def scenario():
        sync_rec, async_rec = Recorder(), AsyncRecorder()
        interceptors = [*sync_rec.aio_interceptors(), *async_rec.aio_interceptors()]
        async with grpc.aio.insecure_channel(target, interceptors=interceptors) as channel:
            uu, us, su, ss = stubs(channel)
            assert (await uu(sv("a"))).value == "A"
            assert [m.value async for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
            assert (await su(iter([sv("x"), sv("y")]))).value == "x,y"
            assert [m.value async for m in ss(iter([sv("p")]))] == ["P"]
            with pytest.raises(grpc.aio.AioRpcError):
                await uu(sv("fail"))
        return sync_rec, async_rec

    sync_rec, async_rec = run(scenario())
    expected = [
        ("end", "OK", 0, 0),
        ("end", "OK", 0, 3),
        ("end", "OK", 2, 0),
        ("end", "OK", 1, 1),
        ("end", "INVALID_ARGUMENT", 0, 0),
    ]
    assert sync_rec.ends() == expected
    assert async_rec.ends() == expected
    assert sync_rec.events == async_rec.events


def test_observer_sees_metadata_of_earlier_interceptor(target):
    class AddMetadata(grpc.aio.UnaryUnaryClientInterceptor):
        async def intercept_unary_unary(self, continuation, details, request):
            metadata = [*(details.metadata or ()), ("x-added", "1")]
            return await continuation(details._replace(metadata=metadata), request)

    seen = {}

    class MetadataObserver(GrpcObserver):
        def on_call_start(self, call):
            seen.setdefault("calls", []).append(call.metadata.get("x-added"))

    async def scenario():
        before, after = MetadataObserver(), MetadataObserver()
        interceptors = [*before.aio_interceptors(), AddMetadata(), *after.aio_interceptors()]
        async with grpc.aio.insecure_channel(target, interceptors=interceptors) as channel:
            uu, *_ = stubs(channel)
            await uu(sv("a"))

    run(scenario())
    assert seen["calls"] == [None, "1"]


# ---------- broken hooks ----------


def _broken_async_observer():
    class BrokenAsync(AsyncGrpcObserver):
        def __init__(self):
            self.ends = []

        async def on_call_start(self, call):
            raise RuntimeError("start bug")

        async def on_request(self, call, request):
            raise RuntimeError("request bug")

        async def on_request_message(self, call, message, index):
            raise RuntimeError("request message bug")

        async def on_response(self, call, response):
            await asyncio.sleep(0)
            raise RuntimeError("response bug")

        async def on_response_message(self, call, message, index):
            raise RuntimeError("response message bug")

        async def on_error(self, call, result):
            raise RuntimeError("error bug")

        async def on_call_end(self, call, result):
            self.ends.append(result.code.name)
            raise RuntimeError("end bug")

    return BrokenAsync()


def test_aio_broken_async_hooks_do_not_break_calls(target, caplog):
    async def scenario():
        observer = _broken_async_observer()
        async with grpc.aio.insecure_channel(target, interceptors=observer.aio_interceptors()) as channel:
            uu, us, su, ss = stubs(channel)
            assert (await uu(sv("a"))).value == "A"
            assert [m.value async for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
            assert (await su(iter([sv("x"), sv("y")]))).value == "x,y"
            assert [m.value async for m in ss(iter([sv("p")]))] == ["P"]
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await uu(sv("fail"))
            assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        return observer

    observer = run(scenario())
    assert observer.ends == ["OK", "OK", "OK", "OK", "INVALID_ARGUMENT"]
    assert "gRPC observer hook" in caplog.text
    assert "BrokenAsync.on_response" in caplog.text


def test_sync_broken_hooks_in_every_kind(target, caplog):
    class Broken(GrpcObserver):
        def __init__(self):
            self.ends = []

        def on_request_message(self, call, message, index):
            raise RuntimeError("request message bug")

        def on_response_message(self, call, message, index):
            raise RuntimeError("response message bug")

        def on_call_end(self, call, result):
            self.ends.append(result.code.name)
            raise RuntimeError("end bug")

    observer = Broken()
    with grpc.insecure_channel(target) as raw:
        uu, us, su, ss = stubs(grpc.intercept_channel(raw, observer.interceptor()))
        assert uu(sv("a")).value == "A"
        assert [m.value for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
        assert su(iter([sv("x")])).value == "x"
        assert [m.value for m in ss(iter([sv("p")]))] == ["P"]
    assert observer.ends == ["OK", "OK", "OK", "OK"]
    assert "Broken.on_request_message" in caplog.text
    # README promises this logger name to silence or route hook errors.
    assert {r.name for r in caplog.records if "gRPC observer hook" in r.getMessage()} == {"grpc_observer"}
