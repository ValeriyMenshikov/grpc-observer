"""Ported reference checks: every call kind, sync and aio, success and error paths."""

import grpc
import pytest
from helpers import HAS_STATUS, AsyncRecorder, Recorder, run, stubs, sv

from grpc_observer import GrpcObserver

UU_METHOD = "/test.Echo/UU"
EXPECTED_ERROR = ("error", "INVALID_ARGUMENT", "boom")


def test_sync_success(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        uu, us, su, ss = stubs(grpc.intercept_channel(raw, rec.interceptor()))

        assert uu(sv("a")).value == "A"
        assert rec.events == [
            ("start", "unary_unary", UU_METHOD),
            ("request", "a"),
            ("response", "A"),
            ("end", "OK", 0, 0),
        ]
        assert rec.last_result.trailing_metadata == {"x-trailer": "t"}

        rec.events.clear()
        assert [m.value for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
        assert rec.events[-1] == ("end", "OK", 0, 3)

        rec.events.clear()
        assert su(iter([sv("x"), sv("y")])).value == "x,y"
        assert ("request_message", 1, "y") in rec.events
        assert rec.events[-2:] == [("response", "x,y"), ("end", "OK", 2, 0)]

        rec.events.clear()
        assert [m.value for m in ss(iter([sv("p"), sv("q")]))] == ["P", "Q"]
        assert rec.events[-1] == ("end", "OK", 2, 2)

        rec.events.clear()
        response, call = uu.with_call(sv("w"))
        assert response.value == "W" and call.code() == grpc.StatusCode.OK
        assert rec.events[-1] == ("end", "OK", 0, 0)

        rec.events.clear()
        assert uu.future(sv("f")).result().value == "F"
        assert rec.events[-1] == ("end", "OK", 0, 0)


def test_sync_errors(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        uu, us, su, ss = stubs(grpc.intercept_channel(raw, rec.interceptor()))

        with pytest.raises(grpc.RpcError) as err:
            uu(sv("fail"))
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 0, 0)]

        rec.events.clear()
        with pytest.raises(grpc.RpcError):
            list(us(sv("fail")))
        assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 0, 3)]

        rec.events.clear()
        with pytest.raises(grpc.RpcError):
            su(iter([sv("fail")]))
        assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 1, 0)]

        rec.events.clear()
        with pytest.raises(grpc.RpcError):
            list(ss(iter([sv("ok"), sv("fail")])))
        assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 2, 1)]


def test_broken_observer_does_not_break_calls(target):
    class Broken(GrpcObserver):
        def on_response(self, call, response):
            raise RuntimeError("observer bug")

    with grpc.insecure_channel(target) as raw:
        uu, *_ = stubs(grpc.intercept_channel(raw, Broken().interceptor()))
        assert uu(sv("a")).value == "A"


@pytest.mark.parametrize("recorder_cls", [Recorder, AsyncRecorder])
def test_aio_success_and_errors(target, recorder_cls):
    async def scenario():
        rec = recorder_cls()
        async with grpc.aio.insecure_channel(target, interceptors=rec.aio_interceptors()) as channel:
            uu, us, su, ss = stubs(channel)

            assert (await uu(sv("a"))).value == "A"
            assert rec.events == [
                ("start", "unary_unary", UU_METHOD),
                ("request", "a"),
                ("response", "A"),
                ("end", "OK", 0, 0),
            ]
            assert rec.last_result.trailing_metadata == {"x-trailer": "t"}

            rec.events.clear()
            assert [m.value async for m in us(sv("b"))] == ["b-0", "b-1", "b-2"]
            assert rec.events[-1] == ("end", "OK", 0, 3)

            # read() style on a response stream
            rec.events.clear()
            call = us(sv("r"))
            got = []
            while (msg := await call.read()) != grpc.aio.EOF:
                got.append(msg.value)
            assert got == ["r-0", "r-1", "r-2"]
            assert ("response_message", 2, "r-2") in rec.events

            # iterator style request stream
            rec.events.clear()
            assert (await su(iter([sv("x"), sv("y")]))).value == "x,y"
            assert rec.events[-2:] == [("response", "x,y"), ("end", "OK", 2, 0)]

            # write() style request stream
            rec.events.clear()
            call = su()
            await call.write(sv("m"))
            await call.write(sv("n"))
            await call.done_writing()
            assert (await call).value == "m,n"
            assert rec.events[-1] == ("end", "OK", 2, 0)

            # write() style bidi
            rec.events.clear()
            call = ss()
            await call.write(sv("p"))
            assert (await call.read()).value == "P"
            await call.done_writing()
            assert await call.read() == grpc.aio.EOF
            assert rec.events[-1] == ("end", "OK", 1, 1)

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                await uu(sv("fail"))
            assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 0, 0)]

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                [m async for m in us(sv("fail"))]
            assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 0, 3)]

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                await su(iter([sv("fail")]))
            assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 1, 0)]

            rec.events.clear()
            with pytest.raises(grpc.aio.AioRpcError):
                [m async for m in ss(iter([sv("ok"), sv("fail")]))]
            assert rec.events[-2:] == [EXPECTED_ERROR, ("end", "INVALID_ARGUMENT", 2, 1)]

    run(scenario())


@pytest.mark.skipif(not HAS_STATUS, reason="needs grpc-observer[status]")
def test_rich_status_is_decoded(target):
    rec = Recorder()
    with grpc.insecure_channel(target) as raw:
        uu, *_ = stubs(grpc.intercept_channel(raw, rec.interceptor()))
        with pytest.raises(grpc.RpcError):
            uu(sv("fail"))
    result = rec.last_result
    assert result.status["message"] == "boom"
    assert result.status["details"][0]["value"] == "bad"
    assert "grpc-status-details-bin" not in result.trailing_metadata

    async def scenario():
        arec = AsyncRecorder()
        async with grpc.aio.insecure_channel(target, interceptors=arec.aio_interceptors()) as channel:
            uu, *_ = stubs(channel)
            with pytest.raises(grpc.aio.AioRpcError):
                await uu(sv("fail"))
        return arec.last_result

    assert run(scenario()).status["details"][0]["value"] == "bad"


def test_wrong_observer_kind_fails_fast():
    with pytest.raises(TypeError, match="grpc.aio channels only"):
        AsyncRecorder().interceptor()

    class AsyncHookInSyncObserver(GrpcObserver):
        async def on_response(self, call, response): ...

    with pytest.raises(TypeError, match="async hooks"):
        AsyncHookInSyncObserver().interceptor()
