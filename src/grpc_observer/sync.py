"""Interceptor for sync channels: `grpc.intercept_channel(channel, ObserverInterceptor(observer))`."""

import inspect
from collections.abc import Callable, Iterator
from typing import Any

import grpc
from google.protobuf.message import Message

from grpc_observer.models import CallKind, GrpcResult, new_call, to_metadata
from grpc_observer.observer import HOOK_NAMES, AsyncGrpcObserver, GrpcObserver
from grpc_observer.status import build_result
from grpc_observer.tracker import CallTracker


def _result_from_call(call: Any) -> GrpcResult:
    return build_result(
        code=call.code(),
        details=call.details(),
        initial_metadata=to_metadata(call.initial_metadata()),
        trailing_metadata=to_metadata(call.trailing_metadata()),
    )


def _result_from_exception(exc: BaseException) -> GrpcResult:
    # gRPC errors are also `grpc.Call` and carry the real status and metadata.
    if isinstance(exc, grpc.Call):
        result = _result_from_call(exc)
        result.exception = exc
        return result
    return build_result(
        code=grpc.StatusCode.UNKNOWN,
        details=repr(exc),
        initial_metadata={},
        trailing_metadata={},
        exception=exc,
    )


def _cancelled_result(details: str) -> GrpcResult:
    return build_result(
        code=grpc.StatusCode.CANCELLED,
        details=details,
        initial_metadata={},
        trailing_metadata={},
    )


def _finish_unary_response(tracker: CallTracker, future: grpc.Future) -> None:
    if future.cancelled():
        tracker.finish(_cancelled_result("call was cancelled"))
        return
    exc = future.exception()
    if exc is not None:
        tracker.finish(_result_from_exception(exc))
        return
    tracker.response(future.result())
    tracker.finish(_result_from_call(future))


def _observe_requests(request_iterator: Iterator[Message], tracker: CallTracker) -> Iterator[Message]:
    for message in request_iterator:
        tracker.request_message(message)
        yield message


class _ObservedResponseStream:
    """Iterator over responses that is still a `grpc.Call` / `grpc.Future` for the caller."""

    def __init__(self, call: Any, tracker: CallTracker):
        self._call = call
        self._tracker = tracker

    def __iter__(self) -> "_ObservedResponseStream":
        return self

    def __next__(self) -> Message:
        try:
            message = next(self._call)
        except StopIteration:
            self._tracker.finish(_result_from_call(self._call))
            raise
        except BaseException as exc:
            self._tracker.finish(_result_from_exception(exc))
            raise
        self._tracker.response_message(message)
        return message

    def cancel(self) -> bool:
        cancelled = self._call.cancel()
        self._tracker.finish(_cancelled_result("call was cancelled by the client"))
        return cancelled

    def __getattr__(self, item: str) -> Any:
        return getattr(self._call, item)


class ObserverInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, observer: GrpcObserver | AsyncGrpcObserver, target: str | None = None):
        if isinstance(observer, AsyncGrpcObserver):
            raise TypeError(
                f"{type(observer).__name__} is an AsyncGrpcObserver and works with grpc.aio channels only. "
                "Use observer.aio_interceptors() with a grpc.aio channel, or subclass GrpcObserver for sync channels."
            )
        async_hooks = [name for name in HOOK_NAMES if inspect.iscoroutinefunction(getattr(observer, name))]
        if async_hooks:
            raise TypeError(
                f"{type(observer).__name__} defines async hooks {async_hooks} but sync channels can't await them. "
                "Subclass AsyncGrpcObserver and use it with a grpc.aio channel."
            )
        self.observer = observer
        self.target = target

    def _start(self, details: grpc.ClientCallDetails, kind: CallKind) -> CallTracker:
        return CallTracker(self.observer, new_call(self.observer.call_class, details, kind, self.target))

    @staticmethod
    def _invoke(tracker: CallTracker, continuation: Callable, details: grpc.ClientCallDetails, request: Any) -> Any:
        try:
            return continuation(details, request)
        except BaseException as exc:
            tracker.finish(_result_from_exception(exc))
            raise

    def intercept_unary_unary(self, continuation, client_call_details, request):
        tracker = self._start(client_call_details, CallKind.UNARY_UNARY)
        tracker.request(request)
        future = self._invoke(tracker, continuation, client_call_details, request)
        future.add_done_callback(lambda f: _finish_unary_response(tracker, f))
        return future

    def intercept_unary_stream(self, continuation, client_call_details, request):
        tracker = self._start(client_call_details, CallKind.UNARY_STREAM)
        tracker.request(request)
        call = self._invoke(tracker, continuation, client_call_details, request)
        return _ObservedResponseStream(call, tracker)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        tracker = self._start(client_call_details, CallKind.STREAM_UNARY)
        requests = _observe_requests(request_iterator, tracker)
        future = self._invoke(tracker, continuation, client_call_details, requests)
        future.add_done_callback(lambda f: _finish_unary_response(tracker, f))
        return future

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        tracker = self._start(client_call_details, CallKind.STREAM_STREAM)
        requests = _observe_requests(request_iterator, tracker)
        call = self._invoke(tracker, continuation, client_call_details, requests)
        return _ObservedResponseStream(call, tracker)
