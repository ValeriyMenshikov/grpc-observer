import base64
import json

import grpc
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from grpc_observer import CallKind, json_default, message_to_dict
from grpc_observer import status as status_module
from grpc_observer.status import GRPC_STATUS_DETAILS_KEY, build_result


def _result(trailing):
    return build_result(
        code=grpc.StatusCode.INVALID_ARGUMENT, details="boom", initial_metadata={}, trailing_metadata=trailing
    )


def test_malformed_status_details_fall_back_to_raw():
    result = _result({GRPC_STATUS_DETAILS_KEY: b"\xffnot a status", "x": "y"})
    assert result.status == {"raw": base64.b64encode(b"\xffnot a status").decode()}
    assert result.trailing_metadata == {"x": "y"}


def test_status_without_extra_is_raw(monkeypatch):
    monkeypatch.setattr(status_module, "status_pb2", None)
    result = _result({GRPC_STATUS_DETAILS_KEY: b"\x08\x03"})
    assert result.status == {"raw": base64.b64encode(b"\x08\x03").decode()}


def test_status_decoded_with_extra():
    status_pb2 = pytest.importorskip("google.rpc.status_pb2")
    raw = status_pb2.Status(code=3, message="boom").SerializeToString()
    assert _result({GRPC_STATUS_DETAILS_KEY: raw}).status == {"code": 3, "message": "boom"}


def test_ok_defaults():
    result = build_result(code=None, details="", initial_metadata={}, trailing_metadata={})
    assert result.ok and result.details is None and result.status is None


def test_call_kind_flags():
    assert [(k.request_streaming, k.response_streaming) for k in CallKind] == [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ]


def test_json_helpers():
    payload = {"bin": b"\x00", "msg": StringValue(value="v"), "kind": CallKind.UNARY_UNARY}
    assert json.loads(json.dumps(payload, default=json_default)) == {
        "bin": "AA==",
        "msg": "v",
        "kind": "unary_unary",
    }
    assert message_to_dict(StringValue()) == ""
