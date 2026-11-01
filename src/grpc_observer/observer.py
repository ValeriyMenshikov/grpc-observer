"""Base classes to subclass: `GrpcObserver` (sync hooks) and `AsyncGrpcObserver` (async hooks).

Subclass one, override the hooks you need and plug it into a channel:

    class MyObserver(GrpcObserver):
        def on_response(self, call, response):
            print(call.method, response)

    # sync
    channel = grpc.intercept_channel(grpc.insecure_channel(target), MyObserver().interceptor())
    # async
    channel = grpc.aio.insecure_channel(target, interceptors=MyObserver().aio_interceptors())

The same observer works for sync and async channels and for all four call types.

To carry your own typed fields through the hooks, subclass `GrpcCall` and parametrize the observer with it:

    @dataclass
    class MyCall(GrpcCall):
        trace_id: str = ""

    class MyObserver(GrpcObserver[MyCall]):
        def on_call_start(self, call: MyCall) -> None:
            call.trace_id = current_trace_id()
"""

from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_args

import grpc
from google.protobuf.message import Message

from grpc_observer.models import GrpcCall, GrpcResult

if TYPE_CHECKING:
    from grpc_observer.sync import ObserverInterceptor

CallT = TypeVar("CallT", bound=GrpcCall)

_HOOK_ORDER_DOC = """
    Hook order for a call:
        on_call_start
        on_request                  (unary request)   | on_request_message * N  (request stream)
        on_response                 (unary response)  | on_response_message * N (response stream)
        on_error                    (only if status != OK)
        on_call_end                 (always, exactly once)

    For bidi streams request and response messages may interleave.
    Exceptions raised in hooks are logged and swallowed, so a broken observer never breaks a call.
"""

HOOK_NAMES = (
    "on_call_start",
    "on_request",
    "on_request_message",
    "on_response",
    "on_response_message",
    "on_error",
    "on_call_end",
)


class _ObserverBase(Generic[CallT]):
    # The class the interceptors instantiate for every call. Taken from `GrpcObserver[MyCall]`,
    # or set it explicitly: `call_class = MyCall`.
    call_class: type[GrpcCall] = GrpcCall

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "call_class" not in cls.__dict__:
            for base in cls.__dict__.get("__orig_bases__", ()):
                args = get_args(base)
                if args and isinstance(args[0], type) and issubclass(args[0], GrpcCall):
                    cls.call_class = args[0]
        if not (isinstance(cls.call_class, type) and issubclass(cls.call_class, GrpcCall)):
            raise TypeError(f"{cls.__name__}.call_class must be a subclass of GrpcCall, got {cls.call_class!r}")


class GrpcObserver(_ObserverBase[CallT]):
    """Observer with sync hooks. Works with sync channels and with grpc.aio channels.

    Override any hook, all hooks are no-ops by default. In sync channels request stream hooks
    may run in a gRPC worker thread — keep them thread-safe.
    """

    __doc__ = (__doc__ or "") + _HOOK_ORDER_DOC

    def on_call_start(self, call: CallT) -> None: ...

    def on_request(self, call: CallT, request: Message) -> None: ...

    def on_request_message(self, call: CallT, message: Message, index: int) -> None: ...

    def on_response(self, call: CallT, response: Message) -> None: ...

    def on_response_message(self, call: CallT, message: Message, index: int) -> None: ...

    def on_error(self, call: CallT, result: GrpcResult) -> None: ...

    def on_call_end(self, call: CallT, result: GrpcResult) -> None: ...

    def interceptor(self, target: str | None = None) -> "ObserverInterceptor":
        """Pass to `grpc.intercept_channel(channel, observer.interceptor())`.

        `target` is only reported as `call.target`: grpc does not tell interceptors which channel they are on.
        """
        from grpc_observer.sync import ObserverInterceptor

        return ObserverInterceptor(self, target=target)

    def aio_interceptors(self, target: str | None = None) -> list[grpc.aio.ClientInterceptor]:
        """Pass as `interceptors=` to a grpc.aio channel (several observers: `[*a.aio_interceptors(), ...]`)."""
        from grpc_observer.aio import aio_interceptors

        return aio_interceptors(self, target=target)


class AsyncGrpcObserver(_ObserverBase[CallT]):
    """Observer with `async def` hooks. Works with grpc.aio channels only.

    Use it when a hook needs to await something (async logger, queue, DB). Hooks are awaited
    inline, so a slow hook slows down the call — offload heavy work to a background task.
    """

    __doc__ = (__doc__ or "") + _HOOK_ORDER_DOC

    async def on_call_start(self, call: CallT) -> None: ...

    async def on_request(self, call: CallT, request: Message) -> None: ...

    async def on_request_message(self, call: CallT, message: Message, index: int) -> None: ...

    async def on_response(self, call: CallT, response: Message) -> None: ...

    async def on_response_message(self, call: CallT, message: Message, index: int) -> None: ...

    async def on_error(self, call: CallT, result: GrpcResult) -> None: ...

    async def on_call_end(self, call: CallT, result: GrpcResult) -> None: ...

    def interceptor(self, target: str | None = None) -> "ObserverInterceptor":
        """Always fails: sync channels can't await hooks. Kept to give a clear error message."""
        from grpc_observer.sync import ObserverInterceptor

        return ObserverInterceptor(self, target=target)  # raises TypeError

    def aio_interceptors(self, target: str | None = None) -> list[grpc.aio.ClientInterceptor]:
        """Pass as `interceptors=` to a grpc.aio channel (several observers: `[*a.aio_interceptors(), ...]`).

        `target` is only reported as `call.target`: grpc does not tell interceptors which channel they are on.
        """
        from grpc_observer.aio import aio_interceptors

        return aio_interceptors(self, target=target)
