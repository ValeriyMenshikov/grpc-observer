"""In-process echo server on `StringValue` and recording observers shared by the tests."""

import asyncio
import time
from concurrent import futures

import grpc
from google.protobuf.any_pb2 import Any as AnyPb
from google.protobuf.wrappers_pb2 import StringValue

from grpc_observer import AsyncGrpcObserver, GrpcObserver
from grpc_observer.status import GRPC_STATUS_DETAILS_KEY

try:
    from google.rpc import code_pb2, status_pb2
except ImportError:
    status_pb2 = None

SERVICE = "test.Echo"
HAS_STATUS = status_pb2 is not None


class _Status(grpc.Status):
    def __init__(self, code, details, trailing_metadata):
        self.code = code
        self.details = details
        self.trailing_metadata = trailing_metadata


def _fail(context):
    """Abort with INVALID_ARGUMENT "boom"; with googleapis-common-protos also send a rich status."""
    if status_pb2 is None:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")
    detail = AnyPb()
    detail.Pack(StringValue(value="bad"))
    status = status_pb2.Status(code=code_pb2.INVALID_ARGUMENT, message="boom", details=[detail])
    context.abort_with_status(
        _Status(
            grpc.StatusCode.INVALID_ARGUMENT,
            status.message,
            ((GRPC_STATUS_DETAILS_KEY, status.SerializeToString()),),
        )
    )


def _unary_unary(request, context):
    if request.value == "fail":
        _fail(context)
    context.set_trailing_metadata((("x-trailer", "t"),))
    return StringValue(value=request.value.upper())


def _readme_echo(request, context):
    context.send_initial_metadata((("x-server-version", "1.4.2"),))
    context.set_trailing_metadata((("x-request-cost", "3"),))
    return StringValue(value=request.value.upper())


def _unary_stream(request, context):
    for i in range(3):
        yield StringValue(value=f"{request.value}-{i}")
    if request.value == "fail":
        _fail(context)


def _stream_unary(request_iterator, context):
    values = [r.value for r in request_iterator]
    if "fail" in values:
        _fail(context)
    return StringValue(value=",".join(values))


def _stream_stream(request_iterator, context):
    for r in request_iterator:
        if r.value == "fail":
            _fail(context)
        yield StringValue(value=r.value.upper())


def _slow(request, context):
    time.sleep(1)
    return request


def _endless(request, context):
    i = 0
    while context.is_active():
        yield StringValue(value=f"{request.value}-{i}")
        i += 1
        time.sleep(0.01)


def _handler(kind, fn):
    factory = getattr(grpc, f"{kind}_rpc_method_handler")
    return factory(fn, request_deserializer=StringValue.FromString, response_serializer=StringValue.SerializeToString)


def start_server() -> tuple[grpc.Server, str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                SERVICE,
                {
                    "UU": _handler("unary_unary", _unary_unary),
                    "US": _handler("unary_stream", _unary_stream),
                    "SU": _handler("stream_unary", _stream_unary),
                    "SS": _handler("stream_stream", _stream_stream),
                    "Slow": _handler("unary_unary", _slow),
                    "Endless": _handler("unary_stream", _endless),
                },
            ),
            # Service used by the README examples.
            grpc.method_handlers_generic_handler(
                "echo.Echo",
                {
                    "Echo": _handler("unary_unary", _readme_echo),
                    "Repeat": _handler("unary_stream", _unary_stream),
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}"


def stub(channel, kind: str, name: str):
    return getattr(channel, kind)(
        f"/{SERVICE}/{name}",
        request_serializer=StringValue.SerializeToString,
        response_deserializer=StringValue.FromString,
    )


def stubs(channel):
    return (
        stub(channel, "unary_unary", "UU"),
        stub(channel, "unary_stream", "US"),
        stub(channel, "stream_unary", "SU"),
        stub(channel, "stream_stream", "SS"),
    )


def sv(value: str) -> StringValue:
    return StringValue(value=value)


class Recorder(GrpcObserver):
    def __init__(self):
        self.events = []
        self.results = []

    def on_call_start(self, call):
        self.events.append(("start", call.kind.value, call.method))

    def on_request(self, call, request):
        self.events.append(("request", request.value))

    def on_request_message(self, call, message, index):
        self.events.append(("request_message", index, message.value))

    def on_response(self, call, response):
        self.events.append(("response", response.value))

    def on_response_message(self, call, message, index):
        self.events.append(("response_message", index, message.value))

    def on_error(self, call, result):
        self.events.append(("error", result.code.name, result.details))

    def on_call_end(self, call, result):
        self.events.append(("end", result.code.name, result.request_count, result.response_count))
        self.results.append(result)

    @property
    def last_result(self):
        return self.results[-1]

    def ends(self):
        return [e for e in self.events if e[0] == "end"]


class AsyncRecorder(AsyncGrpcObserver):
    """Same events as Recorder, but every hook really suspends."""

    def __init__(self):
        self._sync = Recorder()
        self.events = self._sync.events
        self.results = self._sync.results

    @property
    def last_result(self):
        return self._sync.last_result

    def ends(self):
        return self._sync.ends()


def _make_async_hook(name):
    async def hook(self, *args):
        await asyncio.sleep(0)
        getattr(self._sync, name)(*args)

    hook.__name__ = name
    return hook


for _name in (
    "on_call_start",
    "on_request",
    "on_request_message",
    "on_response",
    "on_response_message",
    "on_error",
    "on_call_end",
):
    setattr(AsyncRecorder, _name, _make_async_hook(_name))


def run(coro, timeout: float = 20):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))
