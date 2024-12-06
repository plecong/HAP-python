import logging
import struct
from collections import defaultdict
from enum import Enum
from typing import Callable, List, Tuple, TypeAlias

from pyhap import tlv
from pyhap.const import HAP_SERVER_STATUS
from pyhap.hap_connection import HAPConnection
from pyhap.service import Service

from .datastream_connection import DatastreamConnection, DatastreamMessage
from .datastream_server import DatastreamServer
from .hds_types import Protocol, Topic

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DataStreamStatus:
    SUCCESS = b"\x00"
    GENERIC_ERROR = b"\x01"
    BUSY = b"\x02"


class TransportSessionConfiguration:
    TCP_LISTENING_PORT = b"\x01"


class TransportTypeTypes:
    TRANSPORT_TYPE = b"\x01"


class SessionCommandType(Enum):
    START_SESSION = b"\x00"


class TransportType(Enum):
    HOMEKIT_DATA_STREAM = b"\x00"


class TransferTransportConfigurationTypes:
    TRANSFER_TRANSPORT_CONFIGURATION = b"\x01"


class SetupDataStreamSessionTypes:
    SESSION_COMMAND_TYPE = b"\x01"
    TRANSPORT_TYPE = b"\x02"
    CONTROLLER_KEY_SALT = b"\x03"


class SetupDataStreamWriteResponseTypes:
    STATUS = b"\x01"
    TRANSPORT_TYPE_SESSION_PARAMETERS = b"\x02"
    ACCESSORY_KEY_SALT = b"\x03"


GlobalRequestHandler: TypeAlias = Callable[[DatastreamConnection, int, dict], None]


class DatastreamManagement:
    def __init__(self, service: Service):
        self.service = service
        self.server = DatastreamServer(message_callback=self._handle_message)
        self._setup_service()
        self._request_handlers: dict[
            Tuple[Protocol, Topic], List[GlobalRequestHandler]
        ] = defaultdict(list)

    def _setup_service(self):
        self.service.configure_char(
            "SetupDataStreamTransport",
            setter_callback=self._set_setup_data_stream_transport,
        )
        self.service.configure_char(
            "SupportedDataStreamTransportConfiguration",
            value=self._build_supported_data_stream_transport_configuration(),
        )
        self.service.configure_char("Version", value="1.0")

    def _build_supported_data_stream_transport_configuration(self):
        return tlv.encode(
            TransferTransportConfigurationTypes.TRANSFER_TRANSPORT_CONFIGURATION,
            tlv.encode(
                TransportTypeTypes.TRANSPORT_TYPE,
                TransportType.HOMEKIT_DATA_STREAM.value,
            ),
            to_base64=True,
        )

    async def _set_setup_data_stream_transport(self, value, connection: HAPConnection):
        logger.info("Setting up data stream transport %s", value)
        objs = tlv.decode(value, from_base64=True)

        session_command_type = SessionCommandType(
            objs.get(SetupDataStreamSessionTypes.SESSION_COMMAND_TYPE)
        )
        transport_type = TransportType(
            objs.get(SetupDataStreamSessionTypes.TRANSPORT_TYPE)
        )
        controller_key_salt = objs.get(SetupDataStreamSessionTypes.CONTROLLER_KEY_SALT)

        logger.debug(
            "Received setup data stream with command %s and transport type %s",
            session_command_type,
            transport_type,
        )

        if (
            session_command_type != SessionCommandType.START_SESSION
            or transport_type != TransportType.HOMEKIT_DATA_STREAM
        ):
            logger.error("Unsupported command or transport type")
            return HAP_SERVER_STATUS.INVALID_VALUE_IN_REQUEST

        session = await self.server.prepare_session(
            connection,
            controller_key_salt,
        )

        logger.debug("Responding with listening port: %s", session.port)

        listening_port = tlv.encode(
            TransportSessionConfiguration.TCP_LISTENING_PORT,
            struct.pack("<H", session.port),
        )
        response = tlv.encode(
            SetupDataStreamWriteResponseTypes.STATUS,
            DataStreamStatus.SUCCESS,
            SetupDataStreamWriteResponseTypes.TRANSPORT_TYPE_SESSION_PARAMETERS,
            listening_port,
            SetupDataStreamWriteResponseTypes.ACCESSORY_KEY_SALT,
            session.accessory_key_salt,
            to_base64=True,
        )
        return response

    def add_request_handler(
        self, Protocol: Protocol, Topic: Topic, handler: GlobalRequestHandler
    ):
        self._request_handlers[(Protocol, Topic)].append(handler)

    def remove_request_handler(
        self, Protocol: Protocol, Topic: Topic, handler: GlobalRequestHandler
    ):
        self._request_handlers[(Protocol, Topic)].remove(handler)

    def _handle_message(
        self,
        connection: DatastreamConnection,
        message: DatastreamMessage,
    ):
        for handler in self._request_handlers[(message.protocol, message.topic)]:
            handler(connection, message.id, message.message)
