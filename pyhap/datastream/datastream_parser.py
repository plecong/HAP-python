import itertools
import logging
import struct

from enum import IntEnum, Enum
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)


class Magics:
    TERMINATOR = {type: "terminator"}


class ValueWrapper:
    def __init__(self, value: Any) -> None:
        self.value = value

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return str(f"<{self.__class__.__name__}: {self.value}>")


class Int8(ValueWrapper):
    pass


class Int16(ValueWrapper):
    pass


class Int32(ValueWrapper):
    pass


class Int64(ValueWrapper):
    pass


class Float32(ValueWrapper):
    pass


class Float64(ValueWrapper):
    pass


class SecondsSince2001(ValueWrapper):
    pass


class DataFormatTags(IntEnum):
    INVALID = 0x00
    TRUE = 0x01
    FALSE = 0x02
    TERMINATOR = 0x03
    NULL = 0x04
    UUID = 0x05
    DATE = 0x06
    INTEGER_MINUS_ONE = 0x07
    INTEGER_RANGE_START_0 = 0x08
    INTEGER_RANGE_STOP_39 = 0x2E
    INT8 = 0x30
    INT16LE = 0x31
    INT32LE = 0x32
    INT64LE = 0x33
    FLOAT32LE = 0x35
    FLOAT64LE = 0x36
    UTF8_LENGTH_START = 0x40
    UTF8_LENGTH_STOP = 0x60
    UTF8_LENGTH8 = 0x61
    UTF8_LENGTH16LE = 0x62
    UTF8_LENGTH32LE = 0x63
    UTF8_LENGTH64LE = 0x64
    UTF8_NULL_TERMINATED = 0x6F
    DATA_LENGTH_START = 0x70
    DATA_LENGTH_STOP = 0x90
    DATA_LENGTH8 = 0x91
    DATA_LENGTH16LE = 0x92
    DATA_LENGTH32LE = 0x93
    DATA_LENGTH64LE = 0x94
    DATA_TERMINATED = 0x9F
    COMPRESSION_START = 0xA0
    COMPRESSION_STOP = 0xCF
    ARRAY_LENGTH_START = 0xD0
    ARRAY_LENGTH_STOP = 0xDE
    ARRAY_TERMINATED = 0xDF
    DICTIONARY_LENGTH_START = 0xE0
    DICTIONARY_LENGTH_STOP = 0xEE
    DICTIONARY_TERMINATED = 0xEF


class DatastreamReader:
    def __init__(self, data: bytearray) -> None:
        self.data = data
        self.index = 0
        self.compressed = []

    def _track(self, data: Any) -> Any:
        self.compressed.append(data)
        return data

    def _ensure(self, length: int) -> None:
        if self.index + length > len(self.data):
            raise ValueError(
                f"""End of data stream. Tried reading {length} bytes but only {len(self.data) - self.index} bytes remaining"""
            )

    def finished(self) -> bool:
        if self.index < len(self.data):
            logger.warning(
                "Finished reading HDS frame, but there are still %d bytes left",
                len(self.data) - self.index,
            )
            return False
        return True

    def read_tag(self) -> int:
        self._ensure(1)
        tag = self.data[self.index]
        self.index += 1
        return tag

    def read_true(self) -> bool:
        return self._track(True)

    def read_false(self) -> bool:
        return self._track(False)

    def read_negative_one(self) -> int:
        return self._track(-1)

    def read_int_range(self, value: int) -> int:
        return self._track(value)

    def read_int8(self, track: bool = True) -> int:
        self._ensure(1)
        value = self.data[self.index]
        self.index += 1
        return self._track(value) if track else value

    def read_int16le(self, track: bool = True) -> int:
        self._ensure(2)
        value = int.from_bytes(self.data[self.index : self.index + 2], "little")
        self.index += 2
        return self._track(value) if track else value

    def read_int32le(self, track: bool = True) -> int:
        self._ensure(4)
        value = int.from_bytes(self.data[self.index : self.index + 4], "little")
        self.index += 4
        return self._track(value) if track else value

    def read_int64le(self, track: bool = True) -> int:
        self._ensure(8)
        value = int.from_bytes(self.data[self.index : self.index + 8], "little")
        self.index += 8
        return self._track(value) if track else value

    def read_float32le(self) -> float:
        self._ensure(4)
        value = struct.unpack("<f", self.data[self.index : self.index + 4])
        self.index += 4
        return self._track(value)

    def read_float64le(self) -> float:
        self._ensure(8)
        value = struct.unpack("<d", self.data[self.index : self.index + 8])
        self.index += 8
        return self._track(value)

    def read_utf8(self, length: int) -> str:
        self._ensure(length)
        value = self.data[self.index : self.index + length].decode("utf-8")
        self.index += length
        return self._track(value)

    def read_utf8_length8(self) -> str:
        length = self.read_int8(False)
        return self.read_utf8(length)

    def read_utf8_length16le(self) -> str:
        length = self.read_int16le(False)
        return self.read_utf8(length)

    def read_utf8_length32le(self) -> str:
        length = self.read_int32le(False)
        return self.read_utf8(length)

    def read_utf8_length64le(self) -> str:
        length = self.read_int64le(False)
        return self.read_utf8(length)

    def read_utf8_null_terminated(self) -> str:
        length = self.data[self.index :].index(0)
        return self.read_utf8(length)

    def read_data(self, length: int, track: bool = False) -> bytes:
        self._ensure(length)
        value = self.data[self.index : self.index + length]
        self.index += length
        return self._track(value) if track else value

    def read_data_length8(self) -> bytes:
        length = self.read_int8(False)
        return self.read_data(length)

    def read_data_length16le(self) -> bytes:
        length = self.read_int16le(False)
        return self.read_data(length)

    def read_data_length32le(self) -> bytes:
        length = self.read_int32le(False)
        return self.read_data(length)

    def read_data_length64le(self) -> bytes:
        length = self.read_int64le(False)
        return self.read_data(length)

    def read_data_terminated(self) -> bytes:
        length = self.data[self.index :].index(DataFormatTags.TERMINATOR.value)
        return self.read_data(length)

    def read_date(self) -> float:
        return self.read_float64le()

    def read_uuid(self) -> UUID:
        data = self.read_data(16, False)
        uuid = UUID(bytes=data)
        return self._track(uuid)

    def read_compressed(self, index: int) -> Any:
        return self.compressed[index]


class DatastreamWriter:
    CHUNK_SIZE = 128

    def __init__(self) -> None:
        self.data = bytearray(DatastreamWriter.CHUNK_SIZE)
        self.index = 0

    @property
    def length(self) -> int:
        return self.index

    def get_bytes(self) -> bytearray:
        return self.data[: self.index]

    def _ensure(self, length: int) -> None:
        needed = self.index + length - len(self.data)
        if needed > 0:
            chunks = (needed // DatastreamWriter.CHUNK_SIZE) + 1
            self.data.extend(bytearray(DatastreamWriter.CHUNK_SIZE * chunks))

    def write_tag(self, tag: int) -> None:
        self._ensure(1)
        self.data[self.index] = tag
        self.index += 1

    def write_true(self) -> None:
        self.write_tag(DataFormatTags.TRUE.value)

    def write_false(self) -> None:
        self.write_tag(DataFormatTags.FALSE.value)

    def write_number(self, value: int) -> None:
        if value == -1:
            self.write_tag(DataFormatTags.INTEGER_MINUS_ONE.value)
        elif value >= 0 and value <= 39:
            self.write_tag(DataFormatTags.INTEGER_RANGE_START_0.value + value)
        elif value >= -128 and value <= 127:
            self.write_tag(DataFormatTags.INT8.value)
            self.write_int(value, 1)
        elif value >= -32768 and value <= 32767:
            self.write_tag(DataFormatTags.INT16LE.value)
            self.write_int(value, 2)
        elif value >= -2147483648 and value <= 2147483647:
            self.write_tag(DataFormatTags.INT32LE.value)
            self.write_int(value, 4)
        else:
            self.write_tag(DataFormatTags.INT64LE.value)
            self.write_int(value, 8)

    def write_int(self, value: int, size: int) -> None:
        self._ensure(size)
        if size == 1:
            self.data[self.index] = value
        else:
            self.data[self.index : self.index + size] = value.to_bytes(size, "little")

        self.index += size

    def write_float32(self, value: Float32) -> None:
        self.write_tag(DataFormatTags.FLOAT32LE.value)
        self._ensure(4)
        self.data[self.index : self.index + 4] = struct.pack("<f", value)
        self.index += 4

    def write_float64(self, value: Float64) -> None:
        self.write_tag(DataFormatTags.FLOAT64LE.value)
        self._ensure(8)
        self.data[self.index : self.index + 8] = struct.pack("<d", value.value)
        self.index += 8

    def write_utf8(self, value: str) -> None:
        length = len(value)
        if length <= 32:
            self.write_tag(DataFormatTags.UTF8_LENGTH_START.value + length)
        elif length <= 255:
            self.write_tag(DataFormatTags.UTF8_LENGTH8.value)
            self.write_int(length, 1)
        elif length <= 65535:
            self.write_tag(DataFormatTags.UTF8_LENGTH16LE.value)
            self.write_int(length, 2)
        elif length <= 4294967295:
            self.write_tag(DataFormatTags.UTF8_LENGTH32LE.value)
            self.write_int(length, 4)
        elif length <= 18446744073709551615:
            self.write_tag(DataFormatTags.UTF8_LENGTH64LE.value)
            self.write_int(length, 8)
        else:
            self.write_tag(DataFormatTags.UTF8_NULL_TERMINATED.value)
            length = len(value) + 1

        self._ensure(length)
        self.data[self.index : self.index + length] = value.encode("utf-8")
        self.index += length

    def write_data(self, value: bytes) -> None:
        length = len(value)
        terminator: int = None

        if length <= 32:
            self.write_tag(DataFormatTags.DATA_LENGTH_START.value + length)
        elif length <= 255:
            self.write_tag(DataFormatTags.DATA_LENGTH8.value)
            self.write_int(length, 1)
        elif length <= 65535:
            self.write_tag(DataFormatTags.DATA_LENGTH16LE.value)
            self.write_int(length, 2)
        elif length <= 4294967295:
            self.write_tag(DataFormatTags.DATA_LENGTH32LE.value)
            self.write_int(length, 4)
        elif length <= 18446744073709551615:
            self.write_tag(DataFormatTags.DATA_LENGTH64LE.value)
            self.write_int(length, 8)
        else:
            self.write_tag(DataFormatTags.DATA_TERMINATED.value)
            length = len(value) + 1
            terminator = DataFormatTags.TERMINATOR.value

        self._ensure(length)
        self.data[self.index : self.index + length] = value
        self.index += length

        if terminator:
            self.data[self.index] = terminator
            self.index += 1

    def write_date(self, value: SecondsSince2001) -> None:
        self.write_tag(DataFormatTags.DATE.value)
        self.write_float64(value)

    def write_uuid(self, value: UUID) -> None:
        self.write_tag(DataFormatTags.UUID.value)
        self._ensure(16)
        self.data[self.index : self.index + 16] = value.bytes
        self.index += 16


class DatastreamParser:
    def decode(reader: DatastreamReader) -> Any:
        tag = reader.read_tag()
        match tag:
            case DataFormatTags.INVALID:
                raise ValueError("Invalid tag detected at index {reader.index}")
            case DataFormatTags.TRUE:
                return reader.read_true()
            case DataFormatTags.FALSE:
                return reader.read_false()
            case DataFormatTags.TERMINATOR:
                return Magics.TERMINATOR
            case DataFormatTags.NULL:
                return None
            case DataFormatTags.UUID:
                return reader.read_uuid()
            case DataFormatTags.DATE:
                return reader.read_date()
            case DataFormatTags.INTEGER_MINUS_ONE:
                return reader.read_negative_one()
            case (
                tag
            ) if tag >= DataFormatTags.INTEGER_RANGE_START_0.value and tag <= DataFormatTags.INTEGER_RANGE_STOP_39.value:
                return reader.read_int_range(
                    tag - DataFormatTags.INTEGER_RANGE_START_0.value
                )
            case DataFormatTags.INT8:
                return reader.read_int8()
            case DataFormatTags.INT16LE:
                return reader.read_int16le()
            case DataFormatTags.INT32LE:
                return reader.read_int32le()
            case DataFormatTags.INT64LE:
                return reader.read_int64le()
            case DataFormatTags.FLOAT32LE:
                return reader.read_float32le()
            case DataFormatTags.FLOAT64LE:
                return reader.read_float64le()
            case (
                tag
            ) if tag >= DataFormatTags.UTF8_LENGTH_START.value and tag <= DataFormatTags.UTF8_LENGTH_STOP.value:
                return reader.read_utf8(tag - DataFormatTags.UTF8_LENGTH_START.value)
            case DataFormatTags.UTF8_LENGTH8:
                return reader.read_utf8_length8()
            case DataFormatTags.UTF8_LENGTH16LE:
                return reader.read_utf8_length16le()
            case DataFormatTags.UTF8_LENGTH32LE:
                return reader.read_utf8_length32le()
            case DataFormatTags.UTF8_LENGTH64LE:
                return reader.read_utf8_length64le()
            case DataFormatTags.UTF8_NULL_TERMINATED:
                return reader.read_utf8_null_terminated()
            case (
                tag
            ) if tag >= DataFormatTags.DATA_LENGTH_START.value and tag <= DataFormatTags.DATA_LENGTH_STOP.value:
                return reader.read_data(tag - DataFormatTags.DATA_LENGTH_START.value)
            case DataFormatTags.DATA_LENGTH8:
                return reader.read_data_length8()
            case DataFormatTags.DATA_LENGTH16LE:
                return reader.read_data_length16le()
            case DataFormatTags.DATA_LENGTH32LE:
                return reader.read_data_length32le()
            case DataFormatTags.DATA_LENGTH64LE:
                return reader.read_data_length64le()
            case DataFormatTags.DATA_TERMINATED:
                return reader.read_data_terminated()
            case (
                tag
            ) if tag >= DataFormatTags.COMPRESSION_START.value and tag <= DataFormatTags.COMPRESSION_STOP.value:
                return reader.read_compressed(
                    tag - DataFormatTags.COMPRESSION_START.value
                )
            case (
                tag
            ) if tag >= DataFormatTags.ARRAY_LENGTH_START.value and tag <= DataFormatTags.ARRAY_LENGTH_STOP.value:
                length = tag - DataFormatTags.ARRAY_LENGTH_START.value
                return [DatastreamParser.decode(reader) for _ in range(length)]
            case DataFormatTags.ARRAY_TERMINATED:
                decoded = (DatastreamParser.decode(reader) for _ in itertools.count())
                return list(
                    itertools.takewhile(lambda x: x != Magics.TERMINATOR, decoded)
                )
            case (
                tag
            ) if tag >= DataFormatTags.DICTIONARY_LENGTH_START.value and tag <= DataFormatTags.DICTIONARY_LENGTH_STOP.value:
                length = tag - DataFormatTags.DICTIONARY_LENGTH_START.value
                return {
                    DatastreamParser.decode(reader): DatastreamParser.decode(reader)
                    for _ in range(length)
                }
            case DataFormatTags.DICTIONARY_TERMINATED:
                decoded = (DatastreamParser.decode(reader) for _ in itertools.count())
                items = list(
                    itertools.takewhile(lambda x: x != Magics.TERMINATOR, decoded)
                )
                pairs = zip(items[0::2], items[1::2])
                return dict(pairs)

    def encode(value: Any, writer: DatastreamWriter) -> None:
        if isinstance(value, Enum):
            value = value.value

        if value is None:
            writer.write_tag(DataFormatTags.NULL.value)
        elif value is True:
            writer.write_true()
        elif value is False:
            writer.write_false()
        elif value is Magics.TERMINATOR:
            writer.write_tag(DataFormatTags.TERMINATOR.value)
        elif isinstance(value, int):
            writer.write_number(value)
        elif isinstance(value, float):
            writer.write_float64(Float64(value))
        elif isinstance(value, Int8):
            writer.write_tag(DataFormatTags.INT8.value)
            writer.write_int(value.value, 1)
        elif isinstance(value, Int16):
            writer.write_tag(DataFormatTags.INT16LE.value)
            writer.write_int(value.value, 2)
        elif isinstance(value, Int32):
            writer.write_tag(DataFormatTags.INT32LE.value)
            writer.write_int(value.value, 4)
        elif isinstance(value, Int64):
            writer.write_tag(DataFormatTags.INT64LE.value)
            writer.write_int(value.value, 8)
        elif isinstance(value, Float32):
            writer.write_float32(value)
        elif isinstance(value, Float64):
            writer.write_float64(value)
        elif isinstance(value, SecondsSince2001):
            writer.write_date(value)
        elif isinstance(value, str):
            writer.write_utf8(value)
        elif isinstance(value, bytes):
            writer.write_data(value)
        elif isinstance(value, UUID):
            writer.write_uuid(value)
        elif isinstance(value, list):
            length = len(value)
            if length <= 12:
                writer.write_tag(DataFormatTags.ARRAY_LENGTH_START.value + length)
            else:
                writer.write_tag(DataFormatTags.ARRAY_TERMINATED)

            for item in value:
                DatastreamParser.encode(item, writer)

            if length > 12:
                writer.write_tag(DataFormatTags.TERMINATOR.value)
        elif isinstance(value, dict):
            length = len(value)
            if length <= 14:
                writer.write_tag(DataFormatTags.DICTIONARY_LENGTH_START.value + length)
            else:
                writer.write_tag(DataFormatTags.DICTIONARY_TERMINATED)

            for key, dict_value in value.items():
                DatastreamParser.encode(key, writer)
                DatastreamParser.encode(dict_value, writer)

            if length > 14:
                writer.write_tag(DataFormatTags.TERMINATOR.value)
        else:
            raise ValueError(f"Unsupported type {type(value)}")
