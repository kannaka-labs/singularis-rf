"""APRS transmit via Direwolf, against tests/fake_direwolf.py (KISS + AGW monitor + ack bot)."""
import asyncio
from pathlib import Path

import pytest

from singularis_rf.agw import decode_frames, encode_frame, monitor_text
from singularis_rf.aprs_tx import AprsPolicy, AprsTransmitter, aprs_addressee, validate_ax25_callsign
from singularis_rf.ax25 import aprs_message_info, build_ui_frame, extract_text, parse_ax25
from singularis_rf.codec import Spec, decode_frames as decode_sp1
from singularis_rf.tx import TxPolicy, TxRefused
from tests.fake_direwolf import FakeDirewolf

SPEC = Spec.load(Path(__file__).parent.parent / "spec" / "singularis-compact.json")
BLOCKS = [
    {"type": "SIGNAL", "source": "SC-8999c7ce", "destination": "N0CALL", "coherence": 85, "content": "Evidence suggests resonance…"},
    {"type": "TRACE", "signalType": "finding", "confidence": "high", "action": "delivered", "observations": ["n=10", "p=0.21", "simulated"]},
]


def policy(**kw) -> TxPolicy:
    base = dict(enabled=True, callsign="AE0RM", gap_between_frames_s=0.0)
    base.update(kw)
    return TxPolicy(**base)


def aprs(**kw) -> AprsPolicy:
    base = dict(path=["WIDE1-1"], ack_timeout_s=0.5, retries=3, tx_confirm_timeout_s=1.0, require_tx_confirm=True, gap_between_frames_s=0.0, auto_ack=True)
    base.update(kw)
    return AprsPolicy(**base)


class Bus:
    def __init__(self):
        self.events = []

    async def publish(self, subject, payload):
        self.events.append((subject, payload))


async def with_fake(fake: FakeDirewolf, fn):
    await fake.start()
    try:
        return await fn()
    finally:
        await fake.stop()


# ---------------------------------------------------------------------------
# Pure pieces
# ---------------------------------------------------------------------------

def test_agw_frame_round_trip_and_monitor_text():
    wire = encode_frame("T", "AE0RM", "APZSCC", b"AE0RM>APZSCC,WIDE1-1::KANNAKA  :hello{1\r") + encode_frame("m")
    buf = bytearray(wire[:20])
    assert decode_frames(buf) == []           # partial header: nothing yet
    buf += wire[20:]
    frames = decode_frames(buf)
    assert [f.kind for f in frames] == ["T", "m"]
    assert frames[0].call_from == "AE0RM" and frames[0].call_to == "APZSCC"
    assert monitor_text(frames[0]) == "AE0RM>APZSCC,WIDE1-1::KANNAKA  :hello{1"
    assert len(buf) == 0


def test_callsign_and_addressee_rules():
    assert validate_ax25_callsign("ae0rm-7") == "AE0RM-7"
    for bad in (None, "", "KANNAKA-PRIME", "AE0RM-16", "A B"):
        with pytest.raises(TxRefused):
            validate_ax25_callsign(bad)
    assert aprs_addressee("*") == "ALL" and aprs_addressee("@ALLCALL") == "ALL"
    assert aprs_addressee("kannaka") == "KANNAKA"  # 7 chars: fine as a 9-char APRS addressee
    with pytest.raises(TxRefused, match="9 characters"):
        aprs_addressee("KANNAKA-PRIME")


# ---------------------------------------------------------------------------
# Against the fake Direwolf
# ---------------------------------------------------------------------------

def test_directed_message_is_sent_confirmed_and_acked_per_frame():
    fake = FakeDirewolf(ack_bot="N0CALL")
    bus = Bus()

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port, publish=bus.publish)
        assert await tx.prepare() == "AE0RM"
        try:
            return await tx.transmit(BLOCKS, "N0CALL", message_id="7Q", ref="dl-1"), tx
        finally:
            await tx.close()

    result, tx = asyncio.run(with_fake(fake, go))
    assert result.ok, result.error
    assert result.sent == len(result.frames) >= 2
    # Every transmitted packet is an APRS message from our callsign to the addressee carrying one SP1 frame with {n}.
    msgs = [extract_text(parse_ax25(raw)) for raw in fake.transmitted]
    msgs = [m for m in msgs if m.kind == "aprs-message"]
    assert len(msgs) == len(result.frames)
    for m, frame in zip(msgs, result.frames):
        assert m.addressee == "N0CALL" and m.text == frame and m.msg_no
        assert parse_ax25(fake.transmitted[0]).source == "AE0RM"
        assert parse_ax25(fake.transmitted[0]).digipeaters == ["WIDE1-1"]
    assert decode_sp1(SPEC, result.frames).blocks == BLOCKS
    assert tx.counters["tx_confirmed"] >= len(result.frames)
    assert tx.counters["acks_received"] == len(result.frames)
    events = [p["event"] for s, p in bus.events]
    assert events[0] == "start" and events[-1] == "complete" and events.count("frame_sent") == len(result.frames)
    assert all(p.get("ref") == "dl-1" and p.get("mode") == "aprs" for s, p in bus.events)


def test_lost_acks_trigger_retries_then_success():
    fake = FakeDirewolf(ack_bot="N0CALL", drop_first_acks=2)
    bus = Bus()

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(retries=3), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port, publish=bus.publish)
        await tx.prepare()
        try:
            return await tx.transmit([BLOCKS[0]], "N0CALL", message_id="7R"), tx
        finally:
            await tx.close()

    result, tx = asyncio.run(with_fake(fake, go))
    assert result.ok, result.error
    assert tx.counters["retries"] == 2
    assert [p["event"] for s, p in bus.events].count("retry") == 2


def test_no_ack_at_all_fails_after_retries():
    fake = FakeDirewolf(ack_bot=None)  # nobody answers
    bus = Bus()

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(retries=2, ack_timeout_s=0.2), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port, publish=bus.publish)
        await tx.prepare()
        try:
            return await tx.transmit([BLOCKS[0]], "N0CALL", message_id="7S")
        finally:
            await tx.close()

    result = asyncio.run(with_fake(fake, go))
    assert not result.ok and "no ack from N0CALL" in result.error
    assert bus.events[-1][1]["event"] == "timeout"


def test_broadcast_needs_no_ack_and_uses_ALL():
    fake = FakeDirewolf(ack_bot=None)

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port)
        await tx.prepare()
        try:
            return await tx.transmit([BLOCKS[0]], "*", message_id="7T")
        finally:
            await tx.close()

    result = asyncio.run(with_fake(fake, go))
    assert result.ok and result.to == "ALL"
    m = extract_text(parse_ax25(fake.transmitted[0]))
    assert m.addressee == "ALL" and m.msg_no is None
    assert result.frames[0].startswith("AE0RM>*:SP1 7T:")


def test_guard_refuses_bad_callsign_or_disabled_without_touching_the_radio():
    fake = FakeDirewolf(ack_bot="N0CALL")

    async def go():
        tx = AprsTransmitter(SPEC, policy(callsign="KANNAKA-PRIME"), aprs(), "X", kiss_port=fake.kiss_port, agw_port=fake.agw_port)
        with pytest.raises(TxRefused, match="not a valid AX.25 callsign"):
            await tx.prepare()
        tx2 = AprsTransmitter(SPEC, policy(enabled=False), aprs(), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port)
        with pytest.raises(TxRefused, match="disabled"):
            await tx2.prepare()

    asyncio.run(with_fake(fake, go))
    assert fake.transmitted == []


def test_missing_tx_confirmation_is_an_error_when_required():
    fake = FakeDirewolf(ack_bot="N0CALL", tx_delay=5.0)  # 'T' arrives far too late

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(tx_confirm_timeout_s=0.2), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port)
        await tx.prepare()
        try:
            return await tx.transmit([BLOCKS[0]], "N0CALL")
        finally:
            await tx.close()

    result = asyncio.run(with_fake(fake, go))
    assert not result.ok and "not confirmed on air" in result.error


def test_auto_ack_answers_messages_addressed_to_us():
    fake = FakeDirewolf()

    async def go():
        tx = AprsTransmitter(SPEC, policy(), aprs(), "AE0RM", kiss_port=fake.kiss_port, agw_port=fake.agw_port)
        await tx.prepare()
        try:
            await fake.hear(build_ui_frame("W1AW", "APRS", aprs_message_info("AE0RM", "R i=o d=N s=R t=O", "42")))
            await asyncio.sleep(0.3)
            return tx.counters["acks_sent"]
        finally:
            await tx.close()

    acks_sent = asyncio.run(with_fake(fake, go))
    assert acks_sent == 1
    ack = extract_text(parse_ax25(fake.transmitted[-1]))
    assert ack.kind == "aprs-ack" and ack.addressee == "W1AW" and ack.text == "ack42"


def test_aprs_policy_from_env():
    p = AprsPolicy.from_env({"RF_APRS_PATH": "WIDE1-1, WIDE2-1", "RF_APRS_RETRIES": "5", "RF_APRS_AUTO_ACK": "0"})
    assert p.path == ["WIDE1-1", "WIDE2-1"] and p.retries == 5 and p.auto_ack is False
    assert AprsPolicy.from_env({"RF_APRS_PATH": ""}).path == []
