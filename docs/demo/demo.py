"""The script recorded in docs/demo/demo.gif: LoggingObserver from examples/ with a human-friendly formatter.

Run: python docs/demo/demo.py
"""

import json
import logging
import sys
from pathlib import Path

import grpc
from google.protobuf.wrappers_pb2 import StringValue

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "examples"))

from _echo import start_server  # noqa: E402
from logging_observer import LoggingObserver  # noqa: E402

from grpc_observer import json_default  # noqa: E402

WIDTH = 96
SKIP = {"request_id", "kind", "type", "method"}


class DemoFormatter(logging.Formatter):
    """`LEVEL type method` and the event fields: on one line if they fit, else one per line."""

    def format(self, record: logging.LogRecord) -> str:
        event = record.grpc
        head = f"{record.levelname:<7} {event['type']:<14} {event['method']}"
        fields = [f"{k}={json.dumps(v, default=json_default)}" for k, v in event.items() if k not in SKIP]
        line = "  ".join([head, *fields])
        if len(line) <= WIDTH:
            return line
        return "\n".join([head, *(f"{'':8}{field}" for field in fields)])


def main() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(DemoFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    server, target = start_server()
    with grpc.insecure_channel(target) as raw:
        channel = grpc.intercept_channel(raw, LoggingObserver().interceptor())

        echo = channel.unary_unary(
            "/example.Echo/Echo",
            request_serializer=StringValue.SerializeToString,
            response_deserializer=StringValue.FromString,
        )
        repeat = channel.unary_stream(
            "/example.Echo/Repeat",
            request_serializer=StringValue.SerializeToString,
            response_deserializer=StringValue.FromString,
        )

        echo(StringValue(value="hello"), metadata=(("x-client", "demo"),))
        list(repeat(StringValue(value="tick")))
        try:
            echo(StringValue(value="fail"), metadata=(("x-client", "demo"),))
        except grpc.RpcError:
            pass
    server.stop(None)


if __name__ == "__main__":
    main()
