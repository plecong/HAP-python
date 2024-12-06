from __future__ import annotations

import asyncio
import itertools
import logging
import random

from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, Tuple, TypeAlias, TypedDict

import pyhap
from pyhap.hap_connection import HAPConnection

from .datastream_parser import (
    DatastreamParser,
    DatastreamReader,
    DatastreamWriter,
    Int64,
)
from .hds_crypto import HDSCrypto
from .hds_types import HDSFrame, HDSStatus, Protocol, Topic


logger = logging.getLogger(__name__)


EventHandler: TypeAlias = Callable[[dict], None]

RequestHandler: TypeAlias = Callable[[int, dict], None]

ResponseHandler: TypeAlias = Callable[[Optional[Exception], dict, HDSStatus], None]

MessageHandler: TypeAlias = Callable[
    ["DatastreamConnection", "DatastreamMessage"], None
]


class DatastreamProtocolHandler(TypedDict):
    event_handler: Optional[dict[Topic, EventHandler]]
    request_handler: Optional[dict[Topic, RequestHandler]]


class MessageType(Enum):
    EVENT = 1
    REQUEST = 2
    RESPONSE = 3


@dataclass
class DatastreamMessage:
    type: MessageType
    protocol: Protocol
    topic: Topic
    message: dict
    id: Optional[int] = None  # for requests and responses
    status: Optional[HDSStatus] = None


class ConnectionState(Enum):
    UNIDENTIFIED = 0
    EXPECTING_HELLO = 1
    READY = 2
    CLOSING = 3
    CLOSED = 4


@dataclass
class DatastreamSession:
    connection: HAPConnection
    controller_key_salt: bytes
    crypto: HDSCrypto = None
    port: Optional[int] = None
    connection_task: Optional[asyncio.TimerHandle] = None

    def __post_init__(self):
        self.crypto = HDSCrypto(self.connection.shared_key, self.controller_key_salt)

    @property
    def accessory_key_salt(self):
        return self.crypto.accessory_key_salt


class DatastreamConnection(asyncio.Protocol):
    HEADER_LENGTH = 4
    AUTH_TAG_LENGTH = 16
    MAX_PAYLOAD_LENGTH = 0b11111111111111111111

    def __init__(
        self,
        server: "pyhap.datastream.datastream_server.DatastreamServer",
        message_callback: MessageHandler,
    ):
        self._server = server
        self._session: DatastreamSession = None
        self._state = ConnectionState.UNIDENTIFIED
        self._transport: asyncio.Transport = None
        self._buffer = bytearray()

        self.peername = None
        self.close_handlers = []
        self.protocol_handlers: DatastreamProtocolHandler = {}
        self._response_handlers: dict[
            int, Tuple[ResponseHandler, asyncio.TimerHandle]
        ] = {}
        self._message_callback = message_callback

        self.add_protocol_handler(
            Protocol.CONTROL, {"request_handler": {Topic.HELLO: self._handle_hello}}
        )

        # event loop for handling timeouts
        self._loop = asyncio.get_event_loop()
        self._hello_timer = self._loop.call_later(5, self._hello_timeout)

    def _handle_hello(self, id: int, message: dict):
        logger.debug(
            "[%s] Received HELLO message from controller: %s", self.peername, message
        )
        self._hello_timer.cancel()
        self._hello_timer = None
        self._state = ConnectionState.READY
        self.send_response(Protocol.CONTROL, Topic.HELLO, id)

    def _hello_timeout(self):
        logger.debug(
            "[%s] Timeout waiting for HELLO message from controller", self.peername
        )
        self.close()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info("peername")
        logger.info("[%s] New DataStream connection was established", peername)

        self._transport = transport
        self.peername = peername

    def close(self):
        if self._state >= ConnectionState.CLOSING:
            return
        self._state = ConnectionState.CLOSING
        self._transport.close()

    def connection_lost(self, exc: Exception | None) -> None:
        logger.info("[%s] DataStream connection was lost", self.peername)
        self._state = ConnectionState.CLOSED
        for handler in self.close_handlers:
            handler()
        self.close_handlers.clear()
        # TODO: handle listeners of HAP connection

    def eof_received(self) -> bool | None:
        logger.info("[%s] DataStream received EOF", self.peername)
        return super().eof_received()

    def data_received(self, data: bytes):
        logger.debug("[%s] Received %d bytes: %s", self.peername, len(data), data.hex())
        try:
            frames = self.decode_frames(data)
        except Exception as e:
            logger.error("[%s] Failed to decode HDS frames: %s", self.peername, e)
            self.close()

        if not frames:
            return

        # try to identify the session
        if self._state == ConnectionState.UNIDENTIFIED:
            session = self._server.identify_session(frames[0])

            if session is None:
                logger.debug(
                    "(HDS %s) Could not identify connection. Terminating.",
                    self.peername,
                )
                self.close()
                return

            # have a valid session
            self._session = session
            self._state = ConnectionState.EXPECTING_HELLO

            # TODO register close if HAP connection is closed

        # decrypt all frames
        for idx, frame in enumerate(frames):
            if not self._session.crypto.decrypt(frame):
                logger.debug("[%s] Failed to decrypt frame %d", self.peername, idx)
                self.close()
                return

        messages = [self.decode_payload(frame) for frame in frames]

        # confirm first message if expecting hello
        if self._state == ConnectionState.EXPECTING_HELLO:
            first = messages[0]
            if first.protocol == Protocol.CONTROL and first.topic == Topic.HELLO:
                self._state = ConnectionState.READY
            else:
                logger.debug(
                    "[%s] Expected HELLO message from controller, but got %s",
                    self.peername,
                    first,
                )
                self.close()
                return

        # process messages
        for message in messages:
            self.process_message(message)

    def process_message(self, message: DatastreamMessage):
        logger.debug("[%s] Processing message: %s", self.peername, message)

        if message is None:
            return

        if message.type == MessageType.RESPONSE:
            if message.id not in self._response_handlers:
                logger.warning(
                    "[%s] Received unexpected response with id %d: %s",
                    self.peername,
                    message.id,
                    message,
                )
                return

            handler, timer = self._response_handlers.pop(message.id)
            timer.cancel()
            handler(None, message.message, message.status)
            return

        if message.protocol not in self.protocol_handlers:
            self._message_callback(self, message)
            return

        # process message with protocol handlers
        handlers = self.protocol_handlers[message.protocol]
        if message.type == MessageType.EVENT:
            if message.topic not in handlers.get("event_handler", {}):
                logger.warning(
                    "[%s] No event handler was found for message: %s",
                    self.peername,
                    message,
                )
                return

            try:
                handlers["event_handler"][message.topic](message.message)
            except Exception as e:
                logger.error(
                    "[%s] Error while processing event handler for message (%s): %s",
                    self.peername,
                    e,
                    message,
                )

        elif message.type == MessageType.REQUEST:
            if message.topic not in handlers.get("request_handler", {}):
                logger.warning(
                    "[%s] No request handler was found for message: %s",
                    self.peername,
                    message,
                )
                return

            try:
                handlers["request_handler"][message.topic](message.id, message.message)
            except Exception as e:
                logger.error(
                    "[%s] Error while processing request handler for message (%s): %s",
                    self.peername,
                    e,
                    message,
                )

    def send_event(self, protocol: Protocol, topic: Topic, message: dict):
        header = {
            "protocol": protocol,
            "event": topic,
        }
        if self._state == ConnectionState.READY:
            self.send_frame(header, message)

    def send_request(
        self, protocol: Protocol, topic: Topic, message: dict, callback: ResponseHandler
    ):
        # generate a random id not in request_handlers using next generator
        request_id = next(
            x
            for x in (random.randint(1, 2**32) for _ in itertools.count())
            if x not in self.request_handlers
        )

        # register a response handler with a timeout
        self._response_handlers[request_id] = (
            callback,
            self._loop.call_later(10, lambda: self._request_timeout(request_id)),
        )

        header = {
            "protocol": protocol,
            "request": topic,
            "id": Int64(request_id),
        }
        self.send_frame(header, message)

    def _request_timeout(self, request_id: int):
        if request_id in self._response_handlers:
            handler, timer = self._response_handlers.pop(request_id)
            timer.cancel()
            handler(Exception("Request timed out"), None, HDSStatus.TIMEOUT)
            # did not receive a response in time, close the connection
            self.close()

    def send_response(
        self,
        protocol: Protocol,
        topic: Topic,
        request_id: int,
        status: HDSStatus = HDSStatus.SUCCESS,
        message: dict = {},
    ):
        header = {
            "protocol": protocol,
            "response": topic,
            "id": Int64(request_id),
            "status": Int64(status.value),
        }
        self.send_frame(header, message)

    def send_frame(self, header: dict, message: dict):
        logger.debug(
            "[%s] Sending frame with header: %s and message: %s",
            self.peername,
            header,
            message,
        )
        payload = self.encode_payload(header, message)
        frame = self.encode_frame(payload)
        data = frame.header + frame.ciphered_payload
        self._transport.write(data)

    def decode_frames(self, data: bytes) -> List[HDSFrame]:
        """
        Decode HDS frames from the received data.
        """
        self._buffer.extend(data)
        frames: List[HDSFrame] = []

        while len(self._buffer) >= 16:
            header = self._buffer[: self.HEADER_LENGTH]
            payload_tag = int.from_bytes(header[0:1])
            payload_length = int.from_bytes(header[1:4])

            # not enough data to read the full frame
            frame_length = self.HEADER_LENGTH + payload_length + self.AUTH_TAG_LENGTH
            payload_start = self.HEADER_LENGTH
            auth_tag_start = self.HEADER_LENGTH + payload_length
            if len(self._buffer) < frame_length:
                break

            ciphered_payload = self._buffer[
                payload_start : payload_start + payload_length
            ]
            auth_tag = self._buffer[
                auth_tag_start : auth_tag_start + self.AUTH_TAG_LENGTH
            ]
            self._buffer = self._buffer[frame_length:]

            if payload_tag == 1:
                frames.append(HDSFrame(header, ciphered_payload, auth_tag))
            else:
                logger.debug(
                    "[%s] Encountered unknown payload type %d for payload: %s",
                    self.peername,
                    payload_tag,
                    ciphered_payload.hex(),
                )

        return frames

    def encode_frame(self, payload: bytearray) -> HDSFrame:
        """
        Encode a HDS frame from the provided payload.
        """
        payload_length = len(payload)
        header = bytearray()
        header.append(1)
        header.extend(payload_length.to_bytes(3, "big"))

        frame = HDSFrame(header, None, None, payload)
        return self._session.crypto.encrypt(frame)

    def decode_payload(self, frame: HDSFrame):
        payload = frame.plaintext_payload

        header_length = payload[0]
        message_begin = 1 + header_length

        header_reader = DatastreamReader(payload[1:message_begin])
        message_reader = DatastreamReader(payload[message_begin:])

        try:
            header: dict = DatastreamParser.decode(header_reader)
            header_reader.finished()
        except Exception as e:
            logger.error("[%s] Failed to decode header: %s", self.peername, e)
            return

        try:
            message: dict = DatastreamParser.decode(message_reader)
            message_reader.finished()
        except Exception as e:
            logger.error("[%s] Failed to decode message: %s", self.peername, e)
            return

        if "event" in header:
            return DatastreamMessage(
                MessageType.EVENT,
                Protocol(header["protocol"]),
                Topic(header["event"]),
                message=message,
            )
        elif "request" in header:
            return DatastreamMessage(
                MessageType.REQUEST,
                Protocol(header["protocol"]),
                Topic(header["request"]),
                id=header["id"],
                message=message,
            )
        elif "response" in header:
            return DatastreamMessage(
                MessageType.RESPONSE,
                Protocol(header["protocol"]),
                Topic(header["response"]),
                id=header["id"],
                status=HDSStatus(header["status"]),
                message=message,
            )
        else:
            logger.error(
                "[%s] Encountered unknown paylaod header format: %s (message: %s)",
                self.peername,
                header,
                message,
            )
            return

    def encode_payload(self, header: dict, message: dict) -> bytearray:
        header_writer = DatastreamWriter()
        message_writer = DatastreamWriter()

        DatastreamParser.encode(header, header_writer)
        DatastreamParser.encode(message, message_writer)

        header_bytes = header_writer.get_bytes()
        message_bytes = message_writer.get_bytes()

        header_length = len(header_bytes)

        payload = bytearray()
        payload.append(header_length)
        payload.extend(header_bytes)
        payload.extend(message_bytes)

        if len(payload) > self.MAX_PAYLOAD_LENGTH:
            # TODO: return more specific error code
            raise ValueError(
                "Tried sending payload with length larger than the maximum allowed for data stream"
            )

        return payload

    def add_protocol_handler(
        self, protocol: Protocol, protocol_handler: DatastreamProtocolHandler
    ) -> bool:
        if protocol in self.protocol_handlers:
            return False

        self.protocol_handlers[protocol] = protocol_handler
        return True

    def remove_protocol_handler(
        self, protocol: Protocol, protocol_handler: DatastreamProtocolHandler
    ) -> None:
        current = self.protocol_handlers.get(protocol)

        if current == protocol_handler:
            del self.protocol_handlers[protocol]

    def add_close_handler(self, handler: Callable[[], None]):
        self.close_handlers.append(handler)
