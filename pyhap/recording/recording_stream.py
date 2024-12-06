import asyncio
import logging
import typing
from dataclasses import dataclass
from typing import AsyncGenerator

from pyhap.datastream import HDSStatus, Protocol, Topic
from pyhap.datastream.datastream_connection import DatastreamConnection

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PacketDataType:
    MEDIA_INITIALIZATION = "mediaInitialization"
    MEDIA_FRAGMENT = "mediaFragment"


@dataclass
class RecordingPacket:
    data: bytes
    last: bool


class CameraDelegate(typing.Protocol):
    async def handle_recording_stream(
        stream_id: int,
    ) -> AsyncGenerator[RecordingPacket, None]:
        pass


class RecordingStream:
    def __init__(
        self,
        connection: DatastreamConnection,
        delegate: CameraDelegate,
        request_id: int,
        stream_id: int,
    ):
        self.connection = connection
        self.stream_id = stream_id
        self._delegate = delegate
        self._request_id = request_id

        self.connection.add_protocol_handler(
            protocol=Protocol.DATA_SEND,
            protocol_handler={
                "event_handler": {
                    Topic.CLOSE: self._handle_data_send_close,
                    Topic.ACK: self._handle_data_send_ack,
                }
            },
        )

        self._generator: AsyncGenerator[RecordingPacket] = None

    def start(self):
        asyncio.create_task(self._async_start())

    async def _async_start(self):
        logger.debug(
            "(HDS %s) Sending DATA_SEND_OPEN response for stream_id %d",
            self.connection.peername,
            self.stream_id,
        )
        self.connection.send_response(
            Protocol.DATA_SEND,
            Topic.OPEN,
            self._request_id,
            HDSStatus.SUCCESS,
            {"status": HDSStatus.SUCCESS},
        )

        initializing = True
        data_sequence = 1
        marked_last = False
        max_chunk_size = 0x40000

        self._generator = self._delegate.handle_recording_stream(self.stream_id)
        async for packet in self._generator:
            # TODO: check closed?
            if marked_last:
                break

            fragment = packet.data
            offset = 0
            data_chunk_sequence = 1

            while offset < len(fragment):
                data = fragment[offset : offset + max_chunk_size]
                offset += len(data)

                metadata = {
                    "dataType": PacketDataType.MEDIA_INITIALIZATION
                    if initializing
                    else PacketDataType.MEDIA_FRAGMENT,
                    "dataSequenceNumber": data_sequence,
                    "dataChunkSequenceNumber": data_chunk_sequence,
                    "isLastDataChunk": offset >= len(fragment),
                    "dataTotalSize": len(fragment)
                    if data_chunk_sequence == 1
                    else None,
                }
                event = {
                    "streamId": self.stream_id,
                    "packets": [
                        {
                            "data": data,
                            "metadata": metadata,
                        }
                    ],
                    "endOfStream": packet.last if offset >= len(fragment) else None,
                }

                logger.debug(
                    "(HDS %s) Sending DATA_SEND DATA for stream %d with metadata: %s and length %d; EoS: %s",
                    self.connection.peername,
                    self.stream_id,
                    event["packets"][0]["metadata"],
                    len(data),
                    event["endOfStream"],
                )
                self.connection.send_event(
                    Protocol.DATA_SEND,
                    Topic.DATA,
                    event,
                )

                data_chunk_sequence += 1
                initializing = False

            marked_last = packet.last

            if packet.last:
                marked_last = True
                break

            data_sequence += 1

    def _handle_data_send_close(self, data):
        pass

    def _handle_data_send_ack(self, data):
        pass
