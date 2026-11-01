"""Build `GrpcResult` from what grpc returns, decoding the rich error status (`grpc-status-details-bin`)."""

import base64

import grpc
from google.protobuf.json_format import MessageToDict

from grpc_observer.models import GrpcResult, Metadata

try:
    from google.rpc import status_pb2
except ImportError:  # optional extra: grpc-observer[status]
    status_pb2 = None

GRPC_STATUS_DETAILS_KEY = "grpc-status-details-bin"


def _decode_status(raw: bytes) -> dict:
    if status_pb2 is not None:
        try:
            return MessageToDict(status_pb2.Status.FromString(raw))
        except Exception:  # malformed bytes, unknown Any types in details etc.
            pass
    return {"raw": base64.b64encode(raw).decode()}


def build_result(
    *,
    code: grpc.StatusCode | None,
    details: str | None,
    initial_metadata: Metadata,
    trailing_metadata: Metadata,
    exception: BaseException | None = None,
) -> GrpcResult:
    trailing = dict(trailing_metadata)
    status = None
    raw_status = trailing.pop(GRPC_STATUS_DETAILS_KEY, None)
    if isinstance(raw_status, bytes):
        status = _decode_status(raw_status)
    return GrpcResult(
        code=code or (grpc.StatusCode.OK if exception is None else grpc.StatusCode.UNKNOWN),
        details=details or None,
        initial_metadata=initial_metadata,
        trailing_metadata=trailing,
        status=status,
        exception=exception,
    )
