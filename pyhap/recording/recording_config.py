from enum import Enum, IntEnum
from typing import List, Optional, TypedDict, Union


class EventTriggerOption(IntEnum):
    MOTION = 1
    DOORBELL = 2


class MediaContainerType:
    FRAGMENTED_MP4 = b"\x00"


class MediaContainerConfiguration(TypedDict):
    type: MediaContainerType
    fragment_length: int


class VideoCodecType(Enum):
    H264 = b"\x00"
    H265 = b"\x01"


class H264Profile(Enum):
    BASELINE = b"\x00"
    MAIN = b"\x01"
    HIGH = b"\x02"


class H264Level(Enum):
    LEVEL3_1 = b"\x00"
    LEVEL3_2 = b"\x01"
    LEVEL4_0 = b"\x02"


class VideoCodecPacketizationMode(Enum):
    NON_INTERLEAVED = b"\x00"


class VideoCodecCVO():
    UNSUPPORTED = b"\x00"
    SUPPORTED = b"\x01"


class H264CodecParameters(TypedDict):
    levels: List[H264Level]
    profiles: List[H264Profile]


class VideoRecordingOptions(TypedDict):
    type: VideoCodecType
    parameters: H264CodecParameters
    resolutions: List[List[int]]


class AudioRecordingCodecType(IntEnum):
    AAC_LC = 0
    AAC_ELD = 1


class AudioBitrate(Enum):
    VARIABLE = b"\x00"
    CONSTANT = b"\x01"


class AudioRecordingSamplerate(IntEnum):
    KHZ_8 = 0
    KHZ_16 = 1
    KHZ_24 = 2
    KHZ_32 = 3
    KHZ_44_1 = 4
    KHZ_48 = 5


class AudioRecordingCodec(TypedDict):
    type: AudioRecordingCodecType
    audio_channels: Optional[int]
    bitrate_mode: Optional[AudioBitrate]
    samplerate: Union[AudioRecordingSamplerate, List[AudioRecordingSamplerate]]


class AudioRecordingOptions(TypedDict):
    codecs: Union[List[AudioRecordingCodec], AudioRecordingCodec]


class CameraRecordingOptions(TypedDict):
    prebufferLength: int
    overrideEventTriggerOptions: Optional[List[EventTriggerOption]]
    mediaContainerConfiguration: Union[
        MediaContainerConfiguration, List[MediaContainerConfiguration]
    ]
    video: VideoRecordingOptions
    audio: AudioRecordingOptions


### Selected Configuration


class SelectedH264CodecParameters(TypedDict):
    profile: H264Profile
    level: H264Level
    bitrate: int
    iframe_interval: int


class CameraRecordingVideoConfiguration(TypedDict):
    type: VideoCodecType
    parameters: SelectedH264CodecParameters
    resolution: List[int]


class CameraRecordingAudioConfiguration(AudioRecordingCodec):
    bitrate: int
    samplerate: AudioRecordingSamplerate


class CameraRecordingConfiguration(TypedDict):
    prebuffer_length: int
    event_trigger_types: List[EventTriggerOption]
    media_container_configuration: MediaContainerConfiguration
    video_codec: CameraRecordingVideoConfiguration
    audio_codec: CameraRecordingAudioConfiguration