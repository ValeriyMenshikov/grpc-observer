"""Turn protobuf messages and metadata into JSON-friendly values for logs and reports."""

import base64
import enum
from typing import Any

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message


def message_to_dict(message: Message) -> dict:
    return MessageToDict(message, always_print_fields_with_no_presence=True)


def json_default(value: Any) -> Any:
    """`json.dumps(..., default=json_default)` — handles binary metadata and protobuf messages."""
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, Message):
        return message_to_dict(value)
    if isinstance(value, enum.Enum):
        return value.name
    return str(value)
