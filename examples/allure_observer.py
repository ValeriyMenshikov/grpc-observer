"""One Allure step per gRPC call with request / response / error attachments.

Use it in pytest with allure-pytest: put `AllureObserver().interceptor()` on the channel of your client fixture.
Run standalone: python examples/allure_observer.py   (needs `pip install allure-pytest`; prints the steps)
"""

import json
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import allure
from google.protobuf.message import Message

from grpc_observer import (
    GrpcCall,
    GrpcObserver,
    GrpcResult,
    json_default,
    message_to_dict,
)


@dataclass
class _AllureCallData:
    request: dict | None = None
    response: dict | None = None
    request_messages: deque = field(default_factory=deque)
    response_messages: deque = field(default_factory=deque)


class AllureObserver(GrpcObserver):
    """Adds one Allure step per call with request / response / error attachments.

    The step is written when the call ends (for streams — when the stream is fully consumed).
    This keeps the report correct for streams and concurrent async calls. For streams only
    the last `stream_tail` messages are attached.

    Override `step_title` or `attachments` to change what ends up in the report.
    """

    def __init__(self, *, stream_tail: int = 20):
        self.stream_tail = stream_tail

    @staticmethod
    def _data(call: GrpcCall) -> _AllureCallData:
        return call.state["allure"]

    def on_call_start(self, call: GrpcCall) -> None:
        call.state["allure"] = _AllureCallData(
            request_messages=deque(maxlen=self.stream_tail),
            response_messages=deque(maxlen=self.stream_tail),
        )

    def on_request(self, call: GrpcCall, request: Message) -> None:
        self._data(call).request = message_to_dict(request)

    def on_request_message(self, call: GrpcCall, message: Message, index: int) -> None:
        self._data(call).request_messages.append({"index": index, "message": message_to_dict(message)})

    def on_response(self, call: GrpcCall, response: Message) -> None:
        self._data(call).response = message_to_dict(response)

    def on_response_message(self, call: GrpcCall, message: Message, index: int) -> None:
        self._data(call).response_messages.append({"index": index, "message": message_to_dict(message)})

    def on_call_end(self, call: GrpcCall, result: GrpcResult) -> None:
        with allure.step(self.step_title(call, result)):
            for name, payload in self.attachments(call, result):
                allure.attach(
                    json.dumps(payload, indent=4, ensure_ascii=False, default=json_default),
                    name,
                    attachment_type=allure.attachment_type.JSON,
                )

    # ---------- override points ----------

    def step_title(self, call: GrpcCall, result: GrpcResult) -> str:
        return f"gRPC {call.method} → {result.code.name} ({call.elapsed_ms:.0f} ms)"

    def attachments(self, call: GrpcCall, result: GrpcResult) -> list[tuple[str, Any]]:
        data = self._data(call)
        items: list[tuple[str, Any]] = []

        request: dict[str, Any] = {"request_id": call.request_id, "metadata": call.metadata}
        if call.kind.request_streaming:
            request["messages_total"] = result.request_count
            request["messages"] = list(data.request_messages)
        else:
            request["request"] = data.request
        items.append(("request", request))

        response: dict[str, Any] = {
            "initial_metadata": result.initial_metadata,
            "trailing_metadata": result.trailing_metadata,
        }
        if call.kind.response_streaming:
            response["messages_total"] = result.response_count
            response["messages"] = list(data.response_messages)
        elif result.ok:
            response["response"] = data.response
        items.append(("response", response))

        if not result.ok:
            items.append(
                ("error", {"status_code": result.code.name, "details": result.details, "status": result.status})
            )
        return items


if __name__ == "__main__":
    import grpc
    from _echo import call_everything, start_server

    class PrintingAllureObserver(AllureObserver):
        """Outside pytest there is no Allure report, so also print what would be attached."""

        def on_call_end(self, call: GrpcCall, result: GrpcResult) -> None:
            super().on_call_end(call, result)
            print(self.step_title(call, result))
            for name, payload in self.attachments(call, result):
                print(f"  {name}: {json.dumps(payload, ensure_ascii=False, default=json_default)}")

    server, target = start_server()
    with grpc.insecure_channel(target) as channel:
        call_everything(grpc.intercept_channel(channel, PrintingAllureObserver().interceptor()))
    server.stop(None)
