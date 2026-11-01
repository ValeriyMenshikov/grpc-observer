"""Tiny local echo server on `StringValue` so the examples run without code generation."""

from concurrent import futures

import grpc
from google.protobuf.wrappers_pb2 import StringValue

SERVICE = "example.Echo"


def _echo(request, context):
    context.send_initial_metadata((("x-server-version", "1.4.2"),))
    context.set_trailing_metadata((("x-request-cost", "3"),))
    if request.value == "fail":
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")
    return StringValue(value=request.value.upper())


def _repeat(request, context):
    for i in range(3):
        yield StringValue(value=f"{request.value}-{i}")


def _join(request_iterator, context):
    return StringValue(value=",".join(r.value for r in request_iterator))


def _chat(request_iterator, context):
    for r in request_iterator:
        yield StringValue(value=r.value.upper())


def start_server() -> tuple[grpc.Server, str]:
    def handler(kind, fn):
        return getattr(grpc, f"{kind}_rpc_method_handler")(
            fn, request_deserializer=StringValue.FromString, response_serializer=StringValue.SerializeToString
        )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                SERVICE,
                {
                    "Echo": handler("unary_unary", _echo),
                    "Repeat": handler("unary_stream", _repeat),
                    "Join": handler("stream_unary", _join),
                    "Chat": handler("stream_stream", _chat),
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}"


def call_everything(channel: grpc.Channel) -> None:
    """One call of every kind plus one failing call, through a sync channel."""

    def method(kind, name):
        return getattr(channel, kind)(
            f"/{SERVICE}/{name}",
            request_serializer=StringValue.SerializeToString,
            response_deserializer=StringValue.FromString,
        )

    method("unary_unary", "Echo")(StringValue(value="hello"), metadata=(("x-client", "demo"),))
    list(method("unary_stream", "Repeat")(StringValue(value="tick")))
    method("stream_unary", "Join")(iter([StringValue(value="a"), StringValue(value="b")]))
    list(method("stream_stream", "Chat")(iter([StringValue(value="hi")])))
    try:
        method("unary_unary", "Echo")(StringValue(value="fail"), metadata=(("x-client", "demo"),))
    except grpc.RpcError:
        pass
