import functools
import logging

from pyhap import tlv
from pyhap.accessory import Accessory
from pyhap.datastream import (
    DatastreamManagement,
    HDSProtocolSpecificErrorReason,
    HDSStatus,
    Protocol,
    Topic,
)
from pyhap.datastream.datastream_connection import DatastreamConnection
from pyhap.recording.recording_config import (
    AudioBitrate,
    AudioRecordingOptions,
    CameraRecordingConfiguration,
    CameraRecordingOptions,
    EventTriggerOption,
    MediaContainerType,
    VideoRecordingOptions,
)
from pyhap.recording.recording_stream import CameraDelegate, RecordingStream

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

intle = functools.partial(int.from_bytes, byteorder="little")
int64_bytes = functools.partial(int.to_bytes, length=8, byteorder="little")
int32_bytes = functools.partial(int.to_bytes, length=4, byteorder="little")
int16_bytes = functools.partial(int.to_bytes, length=2, byteorder="little")
int8_bytes = functools.partial(int.to_bytes, length=1, byteorder="little")

# region Tags for TLV encoding


class VideoRecordingConfigTag:
    VIDEO_CODEC_CONFIGURATION = b"\x01"


class VideoCodecConfigTag:
    CODEC_TYPE = b"\x01"
    CODEC_PARAMETERS = b"\x02"
    ATTRIBUTES = b"\x03"


class VideoCodecParamTag:
    PROFILE_ID = b"\x01"
    LEVEL = b"\x02"
    BITRATE = b"\x03"
    IFRAME_INTERVAL = b"\x04"


class VideoAttributesTag:
    IMAGE_WIDTH = b"\x01"
    IMAGE_HEIGHT = b"\x02"
    FRAME_RATE = b"\x03"


class AudioRecordingConfigTag:
    AUDIO_CODEC_CONFIGURATION = b"\x01"


class CameraRecordingConfigTag:
    PREBUFFER_LENGTH = b"\x01"
    EVENT_TRIGGER_OPTIONS = b"\x02"
    MEDIA_CONTAINER_CONFIGURATIONS = b"\x03"


class MediaContainerConfigTag:
    MEDIA_CONTAINER_TYPE = b"\x01"
    MEDIA_CONTAINER_PARAMETERS = b"\x02"


class MediaContainerParamTag:
    FRAGMENT_LENGTH = b"\x01"


class AudioCodecParamTag:
    CHANNEL = b"\x01"
    BIT_RATE = b"\x02"
    SAMPLE_RATE = b"\x03"
    MAX_AUDIO_BITRATE = b"\x04"


class AudioCodecConfigTag:
    CODEC_TYPE = b"\x01"
    CODEC_PARAMETERS = b"\x02"


class SelectedRecordingConfigTag:
    RECORDING_CONFIG = b"\x01"
    VIDEO_CONFIG = b"\x02"
    AUDIO_CONFIG = b"\x03"


# endregion

# region Characteristic Values


class Active:
    INACTIVE = 0
    ACTIVE = 1


class EventSnapshotActive:
    ENABLE = 1
    DISABLE = 0


class HomeKitCameraActive:
    OFF = 0
    ON = 1


# endregion


class RecordingManagement:
    def __init__(
        self,
        accessory: Accessory,
        options: CameraRecordingOptions,
        delegate: CameraDelegate,
    ) -> None:
        self._accessory = accessory
        self._options = options
        self._delegate = delegate

        self.selected_config = None
        self.event_trigger_options = EventTriggerOption.MOTION

        self._recording_active = True
        self._recording_stream = None
        self._construct_services()

    def _construct_services(self) -> None:
        """
        A HKSV camera requires the following services:
        - MotionSensor
        - CameraRTPStreamManagement
        - DataStreamTransportManagement
        - CameraOperatingMode (new)
        - CameraRecordingManagement (new)
        - Microphone (optional)
        - OccupancySensor (optional)
        """
        accesory = self._accessory
        recording_mgmt = accesory.add_preload_service("CameraRecordingManagement")
        recording_mgmt.configure_char("Active", value=Active.ACTIVE)
        recording_mgmt.configure_char(
            "SelectedCameraRecordingConfiguration",
            getter_callback=lambda: self.selected_config,
            setter_callback=self._set_selected_config,
        )
        recording_mgmt.configure_char(
            "SupportedCameraRecordingConfiguration",
            value=self._supported_camera_config(self._options),
        )
        recording_mgmt.configure_char(
            "SupportedVideoRecordingConfiguration",
            value=self._supported_video_config(self._options["video"]),
        )
        recording_mgmt.configure_char(
            "SupportedAudioRecordingConfiguration",
            value=self._supported_audio_config(self._options["audio"]),
        )

        op_mode = accesory.add_preload_service("CameraOperatingMode")
        op_mode.configure_char("EventSnapshotsActive", value=EventSnapshotActive.ENABLE)
        op_mode.configure_char("HomeKitCameraActive", value=HomeKitCameraActive.ON)

        data_stream_mgmt = accesory.add_preload_service("DataStreamTransportManagement")
        recording_mgmt.add_linked_service(data_stream_mgmt)

        motion_sensor = accesory.add_preload_service(
            "MotionSensor", chars=["StatusActive"]
        )
        motion_sensor.configure_char("StatusActive", value=True)
        recording_mgmt.add_linked_service(motion_sensor)

        self.motion_detected = motion_sensor.get_characteristic("MotionDetected")
        self.event_trigger_options = EventTriggerOption.MOTION

        # for debugging
        motion_switch = accesory.add_preload_service("Switch")
        motion_switch.configure_char(
            "On",
            getter_callback=lambda: self.motion_detected.value or False,
            setter_callback=lambda x: self.motion_detected.client_update_value(x),
        )

        # register handler for DATA_SEND
        self._recording_mgmt = recording_mgmt
        self._operating_mode = op_mode
        self._datastream = DatastreamManagement(data_stream_mgmt)
        self._datastream.add_request_handler(
            Protocol.DATA_SEND, Topic.OPEN, self._handle_data_send
        )

    def _handle_data_send(
        self, connection: DatastreamConnection, request_id: int, data: dict
    ):
        logger.debug("Received data send request %s", data)
        stream_id = data.get("streamId")
        stream_type = data.get("type")
        target = data.get("target")
        reason = data.get("reason")

        if target != "controller" or stream_type != "ipcamera.recording":
            logger.debug(
                "(%s) Receieved data send with unexpected target: %s or type: %s",
                connection.peername,
                target,
                stream_type,
            )
            connection.send_response(
                protocol=Protocol.DATA_SEND,
                topic=Topic.OPEN,
                request_id=request_id,
                status=HDSStatus.PROTOCOL_SPECIFIC_ERROR,
                message={"status": HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE},
            )
            return

        if not self._recording_active:
            connection.send_response(
                protocol=Protocol.DATA_SEND,
                topic=Topic.OPEN,
                request_id=request_id,
                status=HDSStatus.PROTOCOL_SPECIFIC_ERROR,
                message={"status": HDSProtocolSpecificErrorReason.NOT_ALLOWED},
            )
            return

        if (
            self._operating_mode.get_characteristic("HomeKitCameraActive").get_value()
            == HomeKitCameraActive.OFF
        ):
            connection.send_response(
                protocol=Protocol.DATA_SEND,
                topic=Topic.OPEN,
                request_id=request_id,
                status=HDSStatus.PROTOCOL_SPECIFIC_ERROR,
                message={"status": HDSProtocolSpecificErrorReason.NOT_ALLOWED},
            )
            return

        if self._recording_stream:
            logger.debug(
                "(%s) Rejecting DATA_SEND OPEN as another stream (%s) is already recording with stream_id %d",
                connection.peername,
                self._recording_stream.connection.peername,
                self._recording_stream.stream_id,
            )
            connection.send_response(
                protocol=Protocol.DATA_SEND,
                topic=Topic.OPEN,
                request_id=request_id,
                status=HDSStatus.PROTOCOL_SPECIFIC_ERROR,
                message={"status": HDSProtocolSpecificErrorReason.BUSY},
            )
            return

        if not self.selected_config:
            connection.send_response(
                protocol=Protocol.DATA_SEND,
                topic=Topic.OPEN,
                request_id=request_id,
                status=HDSStatus.PROTOCOL_SPECIFIC_ERROR,
                message={
                    "status": HDSProtocolSpecificErrorReason.INVALID_CONFIGURATION
                },
            )
            return

        logger.debug(
            "(%s) HDS DATA_SEND Open with reason '%s'", connection.peername, reason
        )
        self._recording_stream = RecordingStream(
            connection=connection,
            delegate=self._delegate,
            request_id=request_id,
            stream_id=stream_id,
        )
        self._recording_stream.start()

    def _supported_camera_config(self, options: CameraRecordingOptions) -> str:
        media_containers = (
            options["mediaContainerConfiguration"]
            if type(options.get("mediaContainerConfiguration")) is list
            else [options.get("mediaContainerConfiguration")]
        )
        return tlv.encode(
            CameraRecordingConfigTag.PREBUFFER_LENGTH,
            int32_bytes(options["prebufferLength"]),
            CameraRecordingConfigTag.EVENT_TRIGGER_OPTIONS,
            int64_bytes(self.event_trigger_options.value or 0),
            CameraRecordingConfigTag.MEDIA_CONTAINER_CONFIGURATIONS,
            [
                tlv.encode(
                    MediaContainerConfigTag.MEDIA_CONTAINER_TYPE,
                    MediaContainerType.FRAGMENTED_MP4,  # media container type = FragmentedMP4
                    MediaContainerConfigTag.MEDIA_CONTAINER_PARAMETERS,
                    tlv.encode(  # media container parameters
                        MediaContainerParamTag.FRAGMENT_LENGTH,
                        int32_bytes(config["fragmentLength"]),
                    ),
                )
                for config in media_containers
            ],
            to_base64=True,
        )

    def _supported_video_config(self, options: VideoRecordingOptions) -> str:
        if not options.get("parameters"):
            raise ValueError("Video recording options must contain parameters")
        if not options.get("resolutions"):
            raise ValueError("Video recording options must contain resolutions")

        codec_parameters = tlv.encode(
            VideoCodecParamTag.PROFILE_ID,
            [x.value for x in options["parameters"]["profiles"]],
            VideoCodecParamTag.LEVEL,
            [x.value for x in options["parameters"]["levels"]],
        )

        attributes = [
            tlv.encode(
                VideoAttributesTag.IMAGE_WIDTH,
                int16_bytes(x[0]),
                VideoAttributesTag.IMAGE_HEIGHT,
                int16_bytes(x[1]),
                VideoAttributesTag.FRAME_RATE,
                int8_bytes(x[2]),
            )
            for x in options["resolutions"]
        ]

        video_stream_configuration = tlv.encode(
            VideoCodecConfigTag.CODEC_TYPE,
            options["type"].value,
            VideoCodecConfigTag.CODEC_PARAMETERS,
            codec_parameters,
            VideoCodecConfigTag.ATTRIBUTES,
            attributes,
        )

        return tlv.encode(
            VideoRecordingConfigTag.VIDEO_CODEC_CONFIGURATION,
            video_stream_configuration,
            to_base64=True,
        )

    def _supported_audio_config(self, options: AudioRecordingOptions) -> str:
        codecs = (
            options.get("codecs")
            if type(options.get("codecs")) is list
            else [options.get("codecs")]
        )

        if not codecs:
            raise ValueError("Audio recording options must contain codecs")

        codec_configs = []
        for codec in codecs:
            samplerates = (
                [int32_bytes(c.value) for c in codec["samplerate"]]
                if type(codec["samplerate"]) is list
                else [int32_bytes(codec["samplerate"].value)]
            )

            audio_parameters = tlv.encode(
                AudioCodecParamTag.CHANNEL,
                int32_bytes(codec.get("audio_channels", 1)),
                AudioCodecParamTag.BIT_RATE,
                (codec["bitrateMode"] or AudioBitrate.VARIABLE).value,
                AudioCodecParamTag.SAMPLE_RATE,
                samplerates,
            )

            codec_configs.append(
                tlv.encode(
                    AudioCodecConfigTag.CODEC_TYPE,
                    int32_bytes(codec["type"].value),
                    AudioCodecConfigTag.CODEC_PARAMETERS,
                    audio_parameters,
                )
            )

        return tlv.encode(
            AudioRecordingConfigTag.AUDIO_CODEC_CONFIGURATION,
            codec_configs,
            to_base64=True,
        )

    def _set_selected_config(self, value) -> CameraRecordingConfiguration:
        logger.info("Setting selected camera recording configuration %s", value)
        self.selected_config = value
        self.recording_config = self.parse_selected_config(value)
        logger.debug("Parsed camera recording configuration: %s", self.recording_config)

    @staticmethod
    def parse_selected_config(
        value: str,
    ) -> CameraRecordingConfiguration:
        objs = tlv.decode(value, from_base64=True)

        recording = tlv.decode(objs[SelectedRecordingConfigTag.RECORDING_CONFIG])
        video = tlv.decode(objs[SelectedRecordingConfigTag.VIDEO_CONFIG])
        audio = tlv.decode(objs[SelectedRecordingConfigTag.AUDIO_CONFIG])

        # recording
        prebuffer_length = intle(recording[CameraRecordingConfigTag.PREBUFFER_LENGTH])
        event_trigger_options_val = intle(
            recording[CameraRecordingConfigTag.EVENT_TRIGGER_OPTIONS],
        )
        event_trigger_options = [
            x for x in EventTriggerOption if x.value & event_trigger_options_val
        ]
        container_config = tlv.decode(
            recording[CameraRecordingConfigTag.MEDIA_CONTAINER_CONFIGURATIONS]
        )
        container_type = container_config[MediaContainerConfigTag.MEDIA_CONTAINER_TYPE]
        container_params = tlv.decode(
            container_config[MediaContainerConfigTag.MEDIA_CONTAINER_PARAMETERS]
        )
        fragment_length = intle(
            container_params[MediaContainerParamTag.FRAGMENT_LENGTH]
        )

        # video
        video_codec = video[VideoCodecConfigTag.CODEC_TYPE]
        video_params = tlv.decode(video[VideoCodecConfigTag.CODEC_PARAMETERS])
        video_attrs = tlv.decode(video[VideoCodecConfigTag.ATTRIBUTES])

        profile = video_params[VideoCodecParamTag.PROFILE_ID]
        level = video_params[VideoCodecParamTag.LEVEL]
        video_bitrate = intle(video_params[VideoCodecParamTag.BITRATE])
        iframe_interval = intle(video_params[VideoCodecParamTag.IFRAME_INTERVAL])

        width = intle(video_attrs[VideoAttributesTag.IMAGE_WIDTH])
        height = intle(video_attrs[VideoAttributesTag.IMAGE_HEIGHT])
        framerate = intle(video_attrs[VideoAttributesTag.FRAME_RATE])

        # audio
        audio_codec = audio[AudioCodecConfigTag.CODEC_TYPE]
        audio_params = tlv.decode(audio[AudioCodecConfigTag.CODEC_PARAMETERS])

        audio_channels = audio_params[AudioCodecParamTag.CHANNEL]
        samplerate = audio_params[AudioCodecParamTag.SAMPLE_RATE]
        audio_bitrate_mode = audio_params[AudioCodecParamTag.BIT_RATE]
        audio_bitrate = intle(audio_params[AudioCodecParamTag.MAX_AUDIO_BITRATE])

        config: CameraRecordingConfiguration = {
            "prebuffer_length": prebuffer_length,
            "event_trigger_types": event_trigger_options,
            "media_container_configuration": {
                "type": container_type,
                "fragment_length": fragment_length,
            },
            "video_codec": {
                "type": video_codec,
                "parameters": {
                    "profile": profile,
                    "levels": level,
                    "bitrate": video_bitrate,
                    "iframe_interval": iframe_interval,
                },
                "resolution": [width, height, framerate],
            },
            "audio_codec": {
                "audio_channels": audio_channels,
                "type": audio_codec,
                "samplerate": samplerate,
                "bitrate_mode": audio_bitrate_mode,
                "bitrate": audio_bitrate,
            },
        }

        return config
