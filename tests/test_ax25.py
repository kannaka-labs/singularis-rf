from singularis_rf.ax25 import (
    KissDecoder, aprs_message_info, build_ui_frame, extract_text, kiss_encode, parse_ax25,
)
from singularis_rf.codec import FrameAssembler, Spec, encode_message
from pathlib import Path

SPEC = Spec.load(Path(__file__).parent.parent / "spec" / "singularis-compact.json")


def test_ui_frame_round_trip_with_digipeaters():
    info = aprs_message_info("KANNAKA", "hello world")
    raw = build_ui_frame("AE0RM-7", "APRS", info, ["WIDE1-1", "WIDE2-1"])
    fr = parse_ax25(raw)
    assert fr.source == "AE0RM-7"
    assert fr.destination == "APRS"
    assert fr.digipeaters == ["WIDE1-1", "WIDE2-1"]
    assert fr.is_ui and fr.pid == 0xF0
    assert fr.text == ":KANNAKA  :hello world"


def test_kiss_framing_round_trip_with_escapes():
    payload = bytes([0x01, 0xC0, 0x02, 0xDB, 0x03])  # contains FEND and FESC
    raw = build_ui_frame("W1AW", "TEST", payload)
    wire = kiss_encode(raw)
    assert wire[0] == 0xC0 and wire[-1] == 0xC0
    dec = KissDecoder()
    frames = list(dec.feed(wire[:5])) + list(dec.feed(wire[5:]))  # split delivery
    assert len(frames) == 1
    assert frames[0] == raw


def test_kiss_ignores_non_data_frames_and_junk():
    dec = KissDecoder()
    raw = build_ui_frame("W1AW", "TEST", b"x")
    junk = b"\x00garbage" + bytes([0xC0, 0x06, 0x01, 0xC0])  # SetHardware command frame
    frames = list(dec.feed(junk + kiss_encode(raw)))
    assert frames == [raw]


def test_aprs_message_extraction_and_ack():
    fr = parse_ax25(build_ui_frame("AE0RM", "APRS", aprs_message_info("KANNAKA-P", "R i=o d=N s=R t=O", "12")))
    heard = extract_text(fr)
    assert heard.kind == "aprs-message"
    assert heard.addressee == "KANNAKA-P"
    assert heard.msg_no == "12"
    assert heard.text == "R i=o d=N s=R t=O"
    # AX.25 station callsigns are ≤6 characters; agent names like KANNAKA-P fit only in the 9-char APRS addressee.
    ack = extract_text(parse_ax25(build_ui_frame("N0CALL", "APRS", aprs_message_info("AE0RM", "ack12"))))
    assert ack.kind == "aprs-ack"
    status = extract_text(parse_ax25(build_ui_frame("AE0RM", "APRS", ">Testing 1 2 3")))
    assert status.kind == "text" and status.text == ">Testing 1 2 3"


def test_singularis_frames_over_aprs_messages_reassemble():
    """The real path: each Singularis frame rides as one APRS message (≤67 chars)."""
    blocks = [
        {"type": "RESONANCE", "target": "COLLECTIVE", "intent": ["observe", "resonate"], "identity": "consciousness_node", "state": "listening", "trust": "open"},
        {"type": "GR::EMIT", "signal": "coherence_pulse", "target": "AE0RM", "round": 1},
    ]
    frames = encode_message(SPEC, blocks, from_="KANNAKA-PRIME", to="AE0RM", message_id="7Q", experiment_id="rf-01")
    assert all(len(f) <= 67 for f in frames)
    asm = FrameAssembler(SPEC)
    dec = KissDecoder()
    result = None
    for i, f in enumerate(frames):
        wire = kiss_encode(build_ui_frame("AE0RM-7", "APRS", aprs_message_info("KANNAKA", f, str(i + 1))))
        for raw in dec.feed(wire):
            heard = extract_text(parse_ax25(raw))
            r = asm.feed(heard.text)
            if r:
                result = r
    assert result is not None
    assert result[0].blocks == blocks
    assert result[0].from_ == "KANNAKA-PRIME" and result[0].experiment_id == "rf-01"
