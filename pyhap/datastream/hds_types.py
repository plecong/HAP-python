from dataclasses import dataclass
from enum import Enum, StrEnum


class Protocol(StrEnum):
    CONTROL = "control"
    TARGET_CONTROL = "targetControl"
    DATA_SEND = "dataSend"


class Topic(StrEnum):
    HELLO = "hello"
    WHOAMI = "whoami"
    OPEN = "open"
    DATA = "data"
    ACK = "ack"
    CLOSE = "close"


@dataclass
class HDSFrame:
    header: bytearray
    ciphered_payload: bytearray
    auth_tag: bytearray
    plaintext_payload: bytearray = None


class HDSStatus(Enum):
    SUCCESS = 0
    OUT_OF_MEMORY = 1
    TIMEOUT = 2
    HEADER_ERROR = 3
    PAYLOAD_ERROR = 4
    MISSING_PROTOCOL = 5
    PROTOCOL_SPECIFIC_ERROR = 6


class HDSProtocolSpecificErrorReason(Enum):
    NORMAL = 0
    NOT_ALLOWED = 1
    BUSY = 2
    CANCELLED = 3
    UNSUPPORTED = 4
    UNEXPECTED_FAILURE = 5
    TIMEOUT = 6
    BAD_DATA = 7
    PROTOCOL_ERROR = 8
    INVALID_CONFIGURATION = 9
