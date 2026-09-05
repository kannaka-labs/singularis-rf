"""singularis-rf — Singularis Prime over amateur-radio text modes (RX-only station)."""
from .codec import (  # noqa: F401
    PROTOCOL, DEFAULT_FRAME_LENGTH, CodecError, Spec, Frame, Message, FrameAssembler,
    crc16, escape_value, unescape_value, normalize_identifier, parse_frame, looks_like_frame,
    decode_frames, decode_payload, encode_payload, encode_message, encode_block, decode_block,
)

__version__ = "0.1.0"
