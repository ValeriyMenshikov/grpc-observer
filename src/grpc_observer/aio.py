"""Interceptors for grpc.aio channels: `grpc.aio.insecure_channel(target, interceptors=aio_interceptors(observer))`.

Notes on grpc.aio internals that shape this code:
- The aio channel registers each interceptor object for the first call type it implements (if/elif),
  so there is one interceptor class per call type.
- For streaming requests the caller's `call.write()` is fed to the request iterator only after all
  interceptors have returned, so stream-request interceptors must not await the response themselves.
  `stream_unary` therefore returns a proxy call that fires hooks when the caller awaits it.
- Response streams are always consumed by grpc via `async for`, so wrapping them in an async generator
  also covers `call.read()`.
- Request streams are always re-wrapped into an async generator, so async hooks can be awaited there.

Both `GrpcObserver` (sync hooks) and `AsyncGrpcObserver` (async hooks) are supported.
"""

import asyncio
import inspect
from collections.abc import AsyncIterator
from typing import Any

import grpc
from google.protobuf.message import Message
from grpc.aio import AioRpcError

from grpc_observer.models import CallKind, GrpcCall, GrpcResult, new_call, to_metadata
from grpc_observer.observer import AsyncGrpcObserver, GrpcObserver
from grpc_observer.status import build_result
from grpc_observer.tracker import log_hook_error

AnyObserver = GrpcObserver | AsyncGrpcObserver


async def _call_hook(hook, *args) -> None:
    try:
        result = hook(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:
        log_hook_error(hook)


class CallTracker:
    """Async twin of `tracker.CallTracker`: awaits hooks of `AsyncGrpcObserver`, calls hooks of `GrpcObserver`."""

    def __init__(self, observer: AnyObserver, call: GrpcCall):
        self.observer = observer
        self.call = call
        self.request_count = 0
        self.response_count = 0
        self._finished = False
        # Keeps the task started by a done callback alive until it finishes the call.
        self.background: asyncio.Task | None = None

    async def start(self) -> None:
        await _call_hook(self.observer.on_call_start, self.call)

    async def request(self, request: Message) -> None:
        await _call_hook(self.observer.on_request, self.call, request)

    async def request_message(self, message: Message) -> None:
        index = self.request_count
        self.request_count += 1
        await _call_hook(self.observer.on_request_message, self.call, message, index)

    async def response(self, response: Message) -> None:
        await _call_hook(self.observer.on_response, self.call, response)

    async def response_message(self, message: Message) -> None:
        index = self.response_count
        self.response_count += 1
        await _call_hook(self.observer.on_response_message, self.call, message, index)

    @property
    def finished(self) -> bool:
        return self._finished

    async def finish(self, result: GrpcResult) -> None:
        if self._finished:
            return
        self._finished = True
        result.request_count = self.request_count
        result.response_count = self.response_count
        if not result.ok:
            await _call_hook(self.observer.on_error, self.call, result)
        await _call_hook(self.observer.on_call_end, self.call, result)


async def _result_from_call(call: Any) -> GrpcResult:
    return build_result(
        code=await call.code(),
        details=await call.details(),
        initial_metadata=to_metadata(await call.initial_metadata()),
        trailing_metadata=to_metadata(await call.trailing_metadata()),
    )


def _result_from_exception(exc: BaseException) -> GrpcResult:
    if isinstance(exc, AioRpcError):
        return build_result(
            code=exc.code(),
            details=exc.details(),
            initial_metadata=to_metadata(exc.initial_metadata()),
            trailing_metadata=to_metadata(exc.trailing_metadata()),
            exception=exc,
        )
    if isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
        details = (
            "response stream was closed before the end" if isinstance(exc, GeneratorExit) else "call was cancelled"
        )
        return build_result(
            code=grpc.StatusCode.CANCELLED,
            details=details,
            initial_metadata={},
            trailing_metadata={},
        )
    return build_result(
        code=grpc.StatusCode.UNKNOWN,
        details=repr(exc),
        initial_metadata={},
        trailing_metadata={},
        exception=exc,
    )


async def _observe_requests(request_iterator: Any, tracker: CallTracker) -> AsyncIterator[Message]:
    if hasattr(request_iterator, "__aiter__"):
        async for message in request_iterator:
            await tracker.request_message(message)
            yield message
    else:
        for message in request_iterator:
            await tracker.request_message(message)
            yield message


async def _finish_cancelled(tracker: CallTracker, idle: asyncio.Event | None) -> None:
    if idle is not None:
        await idle.wait()
    await tracker.finish(_result_from_exception(asyncio.CancelledError()))


def _finish_on_cancel(call: Any, tracker: CallTracker, idle: asyncio.Event | None = None) -> None:
    """Report a cancelled RPC from its done callback.

    After `break` out of `async for` (or a never-awaited stream-unary call) nothing resumes our wrapper,
    so without this `on_call_end` would wait for garbage collection or event loop shutdown.
    `idle` is set while the response wrapper is not running hooks, so `on_call_end` stays the last hook.
    """

    def on_done(done_call: Any) -> None:
        if tracker.finished or not done_call.cancelled():
            return
        try:
            tracker.background = asyncio.get_running_loop().create_task(_finish_cancelled(tracker, idle))
        except RuntimeError:  # event loop is already gone
            pass

    call.add_done_callback(on_done)


def _observe_responses(call: Any, tracker: CallTracker) -> AsyncIterator[Message]:
    idle = asyncio.Event()
    idle.set()
    _finish_on_cancel(call, tracker, idle)
    return _iterate_responses(call, tracker, idle)


async def _iterate_responses(call: Any, tracker: CallTracker, idle: asyncio.Event) -> AsyncIterator[Message]:
    idle.clear()
    try:
        async for message in call:
            await tracker.response_message(message)
            idle.set()
            yield message
            idle.clear()
    except BaseException as exc:
        await tracker.finish(_result_from_exception(exc))
        raise
    finally:
        idle.set()
    await tracker.finish(await _result_from_call(call))


class _ObservedStreamUnaryCall(grpc.aio.StreamUnaryCall):
    """Delegates to the real call; fires response hooks when the caller awaits the result."""

    def __init__(self, call: Any, tracker: CallTracker):
        self._call = call
        self._tracker = tracker
        _finish_on_cancel(call, tracker)

    async def _wait(self) -> Message:
        try:
            response = await self._call
        except BaseException as exc:
            await self._tracker.finish(_result_from_exception(exc))
            raise
        if not self._tracker.finished:
            await self._tracker.response(response)
            await self._tracker.finish(await _result_from_call(self._call))
        return response

    def __await__(self):
        return self._wait().__await__()

    # --- plain delegation ---
    def cancelled(self) -> bool:
        return self._call.cancelled()

    def done(self) -> bool:
        return self._call.done()

    def time_remaining(self) -> float | None:
        return self._call.time_remaining()

    def cancel(self) -> bool:
        return self._call.cancel()

    def add_done_callback(self, callback) -> None:
        self._call.add_done_callback(callback)

    async def initial_metadata(self):
        return await self._call.initial_metadata()

    async def trailing_metadata(self):
        return await self._call.trailing_metadata()

    async def code(self) -> grpc.StatusCode:
        return await self._call.code()

    async def details(self) -> str:
        return await self._call.details()

    async def wait_for_connection(self) -> None:
        await self._call.wait_for_connection()

    async def write(self, request: Message) -> None:
        await self._call.write(request)

    async def done_writing(self) -> None:
        await self._call.done_writing()

    def __getattr__(self, item: str) -> Any:
        # grpc internals read private attributes like `_done_writing_flag`.
        return getattr(self._call, item)


class _AsyncObserverInterceptorBase:
    def __init__(self, observer: AnyObserver, target: str | None = None):
        self.observer = observer
        self.target = target

    async def _start(self, details: grpc.aio.ClientCallDetails, kind: CallKind) -> CallTracker:
        tracker = CallTracker(self.observer, new_call(self.observer.call_class, details, kind, self.target))
        await tracker.start()
        return tracker

    @staticmethod
    async def _invoke(tracker: CallTracker, continuation, details, request: Any) -> Any:
        try:
            return await continuation(details, request)
        except BaseException as exc:
            await tracker.finish(_result_from_exception(exc))
            raise


class AsyncUnaryUnaryObserverInterceptor(_AsyncObserverInterceptorBase, grpc.aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        tracker = await self._start(client_call_details, CallKind.UNARY_UNARY)
        await tracker.request(request)
        call = await self._invoke(tracker, continuation, client_call_details, request)
        try:
            response = await call
        except asyncio.CancelledError as exc:
            await tracker.finish(_result_from_exception(exc))
            raise
        except BaseException as exc:
            # The caller gets the same error when awaiting the returned call.
            await tracker.finish(_result_from_exception(exc))
            return call
        await tracker.response(response)
        await tracker.finish(await _result_from_call(call))
        return call


class AsyncUnaryStreamObserverInterceptor(_AsyncObserverInterceptorBase, grpc.aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(self, continuation, client_call_details, request):
        tracker = await self._start(client_call_details, CallKind.UNARY_STREAM)
        await tracker.request(request)
        call = await self._invoke(tracker, continuation, client_call_details, request)
        return _observe_responses(call, tracker)


class AsyncStreamUnaryObserverInterceptor(_AsyncObserverInterceptorBase, grpc.aio.StreamUnaryClientInterceptor):
    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        tracker = await self._start(client_call_details, CallKind.STREAM_UNARY)
        requests = _observe_requests(request_iterator, tracker)
        call = await self._invoke(tracker, continuation, client_call_details, requests)
        return _ObservedStreamUnaryCall(call, tracker)


class AsyncStreamStreamObserverInterceptor(_AsyncObserverInterceptorBase, grpc.aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        tracker = await self._start(client_call_details, CallKind.STREAM_STREAM)
        requests = _observe_requests(request_iterator, tracker)
        call = await self._invoke(tracker, continuation, client_call_details, requests)
        return _observe_responses(call, tracker)


def aio_interceptors(observer: AnyObserver, target: str | None = None) -> list[grpc.aio.ClientInterceptor]:
    """grpc.aio registers an interceptor object for one call type only, so we need four of them."""
    return [
        AsyncUnaryUnaryObserverInterceptor(observer, target),
        AsyncUnaryStreamObserverInterceptor(observer, target),
        AsyncStreamUnaryObserverInterceptor(observer, target),
        AsyncStreamStreamObserverInterceptor(observer, target),
    ]
