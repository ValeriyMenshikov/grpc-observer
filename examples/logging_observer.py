"""Log every gRPC request, response, stream message and call result with stdlib `logging`.

Run: python examples/logging_observer.py
"""

import json
import logging

from google.protobuf.message import Message

from grpc_observer import GrpcCall, GrpcObserver, GrpcResult, json_default, message_to_dict


class LoggingObserver(GrpcObserver):
    """One log record per event. Fields go to `extra` and, as JSON, to the message itself."""

    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger("grpc")

    def _emit(self, level: int, call: GrpcCall, **fields) -> None:
        fields = {"request_id": call.request_id, "method": call.method, "kind": call.kind.value, **fields}
        message = json.dumps(fields, ensure_ascii=False, default=json_default)
        self.log.log(level, "grpc %s", message, extra={"grpc": fields})

    def on_call_start(self, call: GrpcCall) -> None:
        if call.kind.request_streaming:
            self._emit(logging.INFO, call, type="stream_request_start", metadata=call.metadata)

    def on_request(self, call: GrpcCall, request: Message) -> None:
        self._emit(logging.INFO, call, type="request", request=message_to_dict(request), metadata=call.metadata)

    # In sync channels this hook runs in a gRPC worker thread: logging is thread-safe.
    def on_request_message(self, call: GrpcCall, message: Message, index: int) -> None:
        self._emit(logging.INFO, call, type="stream_request_message", index=index, message=message_to_dict(message))

    def on_response(self, call: GrpcCall, response: Message) -> None:
        self._emit(logging.INFO, call, type="response", response=message_to_dict(response))

    def on_response_message(self, call: GrpcCall, message: Message, index: int) -> None:
        self._emit(logging.INFO, call, type="stream_message", index=index, message=message_to_dict(message))

    def on_call_end(self, call: GrpcCall, result: GrpcResult) -> None:
        event = {
            "type": "end" if result.ok else "error",
            "status_code": result.code.name,
            "duration_ms": round(call.elapsed_ms, 1),
            "initial_metadata": result.initial_metadata,
            "trailing_metadata": result.trailing_metadata,
        }
        if result.details:
            event["details"] = result.details
        if result.status:
            event["status"] = result.status
        if call.kind.request_streaming:
            event["requests_count"] = result.request_count
        if call.kind.response_streaming:
            event["messages_count"] = result.response_count
        self._emit(logging.INFO if result.ok else logging.WARNING, call, **event)


if __name__ == "__main__":
    import grpc
    from _echo import call_everything, start_server

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    server, target = start_server()
    with grpc.insecure_channel(target) as channel:
        call_everything(grpc.intercept_channel(channel, LoggingObserver().interceptor()))
    server.stop(None)
