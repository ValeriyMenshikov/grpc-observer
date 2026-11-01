"""What observers receive: the call (`GrpcCall`), its kind (`CallKind`) and its final status (`GrpcResult`)."""

import enum
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import grpc

Metadata = dict[str, str | bytes]


class CallKind(enum.StrEnum):
    UNARY_UNARY = "unary_unary"
    UNARY_STREAM = "unary_stream"
    STREAM_UNARY = "stream_unary"
    STREAM_STREAM = "stream_stream"

    @property
    def request_streaming(self) -> bool:
        return self in (CallKind.STREAM_UNARY, CallKind.STREAM_STREAM)

    @property
    def response_streaming(self) -> bool:
        return self in (CallKind.UNARY_STREAM, CallKind.STREAM_STREAM)


@dataclass
class GrpcCall:
    """One RPC as seen by an observer. Created per call, passed to every hook."""

    method: str
    kind: CallKind
    metadata: Metadata
    timeout: float | None
    wait_for_ready: bool | None = None
    # Channel address, if passed to `observer.interceptor(target=...)` / `aio_interceptors(target=...)`.
    target: str | None = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: float = field(default_factory=time.monotonic)
    # Free-form per-call storage for observers (collected messages, timers, etc).
    state: dict[str, Any] = field(default_factory=dict)

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started_at) * 1000


@dataclass
class GrpcResult:
    """Final status of a call. Passed to `on_error` and `on_call_end`."""

    code: grpc.StatusCode
    details: str | None = None
    initial_metadata: Metadata = field(default_factory=dict)
    trailing_metadata: Metadata = field(default_factory=dict)
    # Decoded google.rpc.Status from `grpc-status-details-bin`, if the server sent one.
    status: dict | None = None
    exception: BaseException | None = None
    # Number of messages streamed (requests / responses). 0 for unary sides.
    request_count: int = 0
    response_count: int = 0

    @property
    def ok(self) -> bool:
        return self.code == grpc.StatusCode.OK


def to_metadata(raw: Iterable[tuple[str, str | bytes]] | None) -> Metadata:
    if not raw:
        return {}
    return {key: value for key, value in raw}


def decode_method(method: str | bytes) -> str:
    return method.decode() if isinstance(method, bytes) else method


def new_call(call_class: type[GrpcCall], details: Any, kind: CallKind, target: str | None) -> GrpcCall:
    """Create the observer's call object from `grpc.ClientCallDetails` / `grpc.aio.ClientCallDetails`."""
    return call_class(
        method=decode_method(details.method),
        kind=kind,
        metadata=to_metadata(details.metadata),
        timeout=details.timeout,
        wait_for_ready=getattr(details, "wait_for_ready", None),
        target=target,
    )
