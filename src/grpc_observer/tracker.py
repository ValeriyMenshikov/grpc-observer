"""Fire observer hooks for one call (sync channels) so that a broken hook never breaks the call."""

import logging

from google.protobuf.message import Message

from grpc_observer.models import GrpcCall, GrpcResult
from grpc_observer.observer import GrpcObserver

# One stable name, whatever module the failing hook is called from.
_log = logging.getLogger("grpc_observer")


def log_hook_error(hook) -> None:
    _log.exception("gRPC observer hook %s failed", getattr(hook, "__qualname__", hook))


def safe_hook(hook, *args) -> None:
    try:
        hook(*args)
    except Exception:
        log_hook_error(hook)


class CallTracker:
    """Fires observer hooks for one call and guarantees `on_call_end` runs once."""

    def __init__(self, observer: GrpcObserver, call: GrpcCall):
        self.observer = observer
        self.call = call
        self.request_count = 0
        self.response_count = 0
        self._finished = False
        safe_hook(observer.on_call_start, call)

    def request(self, request: Message) -> None:
        safe_hook(self.observer.on_request, self.call, request)

    def request_message(self, message: Message) -> None:
        index = self.request_count
        self.request_count += 1
        safe_hook(self.observer.on_request_message, self.call, message, index)

    def response(self, response: Message) -> None:
        safe_hook(self.observer.on_response, self.call, response)

    def response_message(self, message: Message) -> None:
        index = self.response_count
        self.response_count += 1
        safe_hook(self.observer.on_response_message, self.call, message, index)

    @property
    def finished(self) -> bool:
        return self._finished

    def finish(self, result: GrpcResult) -> None:
        if self._finished:
            return
        self._finished = True
        result.request_count = self.request_count
        result.response_count = self.response_count
        if not result.ok:
            safe_hook(self.observer.on_error, self.call, result)
        safe_hook(self.observer.on_call_end, self.call, result)
