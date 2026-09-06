"""Transmit path against a protocol-faithful fake JS8Call (tests/fake_js8call.py).
Real-radio validation is pending; these prove the guard, the sequencing and the loopback."""
import asyncio
import json
from pathlib import Path

import pytest

from singularis_rf.codec import FrameAssembler, Spec, decode_frames, parse_frame
from singularis_rf.js8call import Js8CallClient, directed_text
from singularis_rf.sources import Js8CallSource
from singularis_rf.tx import Transmitter, TxGuard, TxPolicy, TxRefused, parse_tx_request
from tests.fake_js8call import FakeJs8Call

SPEC = Spec.load(Path(__file__).parent.parent / "spec" / "singularis-compact.json")
BLOCKS = [
    {"type": "RESONANCE", "target": "COLLECTIVE", "intent": ["observe", "resonate"], "identity": "consciousness_node", "state": "listening", "trust": "open"},
    {"type": "GR::EMIT", "signal": "coherence_pulse", "target": "AE0RM", "round": 1},
]


def policy(**kw) -> TxPolicy:
    base = dict(enabled=True, callsign="AE0RM", per_frame_timeout_s=5.0, gap_between_frames_s=0.0)
    base.update(kw)
    return TxPolicy(**base)


class Bus:
    def __init__(self):
        self.events = []

    async def publish(self, subject, payload):
        self.events.append((subject, payload))


async def with_fake(fake: FakeJs8Call, fn):
    await fake.start()
    client = Js8CallClient("127.0.0.1", fake.port, request_timeout=3.0)
    await client.connect()
    try:
        return await fn(client)
    finally:
        await client.close()
        await fake.stop()


# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

def test_guard_refuses_when_disabled_or_unidentified_or_mismatched():
    with pytest.raises(TxRefused, match="disabled"):
        TxGuard(policy(enabled=False)).check_station("AE0RM")
    with pytest.raises(TxRefused, match="RF_CALLSIGN is not set"):
        TxGuard(policy(callsign=None)).check_station("AE0RM")
    with pytest.raises(TxRefused, match="does not match"):
        TxGuard(policy()).check_station("W1AW")
    TxGuard(policy()).check_station("ae0rm ")  # case/space tolerant


def test_guard_allowlist_and_rate_limit():
    g = TxGuard(policy(allowlist={"KANNAKA"}))
    g.check_destination("kannaka")
    g.check_destination("*")
    with pytest.raises(TxRefused, match="not in RF_TX_ALLOWLIST"):
        g.check_destination("W1AW")
    clock = [0.0]
    g = TxGuard(policy(max_frames_per_10min=3), now=lambda: clock[0])
    g.reserve_frames(2)
    with pytest.raises(TxRefused, match="rate limit"):
        g.reserve_frames(2)
    g.reserve_frames(1)
    assert g.frames_in_window == 3
    clock[0] = 601.0
    g.reserve_frames(3)  # window rolled over
    assert g.frames_in_window == 3


def test_policy_from_env_defaults_to_disabled():
    p = TxPolicy.from_env({})
    assert p.enabled is False and p.callsign is None
    p = TxPolicy.from_env({"RF_TX_ENABLE": "1", "RF_CALLSIGN": "ae0rm", "RF_TX_ALLOWLIST": "kannaka, w1aw", "RF_TX_CASE_FOLD": "1"})
    assert p.enabled and p.callsign == "AE0RM" and p.allowlist == {"KANNAKA", "W1AW"} and p.case_fold


def test_parse_tx_request():
    r = parse_tx_request({"to": "kannaka", "blocks": BLOCKS, "messageId": "7q", "experimentId": "rf-01"})
    assert r["to"] == "kannaka" and r["message_id"] == "7Q" and r["experiment_id"] == "rf-01"
    with pytest.raises(TxRefused):
        parse_tx_request({"to": "X", "frames": ["pre-encoded not allowed"]})
    with pytest.raises(TxRefused):
        parse_tx_request({"blocks": [{"nope": 1}]})


# ---------------------------------------------------------------------------
# Transmit sequencing against the fake
# ---------------------------------------------------------------------------

def test_transmit_sends_each_frame_as_a_directed_message_and_waits_for_idle():
    fake = FakeJs8Call(callsign="AE0RM")
    bus = Bus()

    async def go(client):
        tx = Transmitter(SPEC, client, policy(), station="AE0RM", publish=bus.publish)
        assert await tx.prepare() == "AE0RM"
        return await tx.transmit(BLOCKS, "KANNAKA", message_id="7Q", experiment_id="rf-01")

    result = asyncio.run(with_fake(fake, go))
    assert result.ok, result.error
    assert result.sent == len(result.frames) >= 2
    # One TX.SEND_MESSAGE per frame, in order, in JS8Call's directed form.
    sends = [r["value"] for r in fake.requests if r["type"] == "TX.SEND_MESSAGE"]
    assert sends == [directed_text("KANNAKA", f) for f in result.frames]
    assert all(s.startswith("KANNAKA: AE0RM>KANNAKA:SP1 7Q:") for s in sends)
    # Frames are valid SP1 and reassemble to the blocks we asked to send, from our licensed callsign.
    msg = decode_frames(SPEC, result.frames)
    assert msg.from_ == "AE0RM" and msg.to == "KANNAKA" and msg.blocks == BLOCKS and msg.experiment_id == "rf-01"
    # Status trail on the bus.
    kinds = [p["event"] for s, p in bus.events if s == "RF.tx.AE0RM.status"]
    assert kinds[0] == "start" and kinds[-1] == "complete" and kinds.count("frame_sent") == len(result.frames)


def test_ref_is_echoed_on_every_status_event_and_case_fold_can_be_per_request():
    fake = FakeJs8Call(callsign="AE0RM")
    bus = Bus()

    async def go(client):
        tx = Transmitter(SPEC, client, policy(), station="AE0RM", publish=bus.publish)
        return await tx.transmit(BLOCKS, "KANNAKA", message_id="7Q", ref="delivery-123", case_fold=True)

    result = asyncio.run(with_fake(fake, go))
    assert result.ok
    statuses = [p for s, p in bus.events if s == "RF.tx.AE0RM.status"]
    assert statuses and all(p.get("ref") == "delivery-123" for p in statuses)
    assert [p["event"] for p in statuses][-1] == "complete"
    assert all(f == f.upper() for f in result.frames)  # per-request case_fold overrode the policy default
    r = parse_tx_request({"to": "KANNAKA", "blocks": BLOCKS, "ref": "dl-9", "caseFold": True})
    assert r["ref"] == "dl-9" and r["case_fold"] is True


def test_transmit_refused_when_js8call_callsign_differs_and_nothing_is_sent():
    fake = FakeJs8Call(callsign="W1AW")  # the operator's radio is not who we claim to be
    bus = Bus()

    async def go(client):
        tx = Transmitter(SPEC, client, policy(), station="AE0RM", publish=bus.publish)
        return await tx.transmit(BLOCKS, "KANNAKA")

    result = asyncio.run(with_fake(fake, go))
    assert not result.ok and "does not match" in result.error
    assert not [r for r in fake.requests if r["type"] == "TX.SEND_MESSAGE"]
    assert bus.events[-1][1]["event"] == "refused"


def test_transmit_refused_when_disabled():
    fake = FakeJs8Call()

    async def go(client):
        return await Transmitter(SPEC, client, policy(enabled=False), station="AE0RM").transmit(BLOCKS, "KANNAKA")

    result = asyncio.run(with_fake(fake, go))
    assert not result.ok and "disabled" in result.error and not fake.sent_texts


def test_case_fold_frames_are_uppercase_with_valid_crc_and_still_decode():
    fake = FakeJs8Call()

    async def go(client):
        return await Transmitter(SPEC, client, policy(case_fold=True), station="AE0RM").transmit(BLOCKS, "KANNAKA", message_id="7Q")

    result = asyncio.run(with_fake(fake, go))
    assert result.ok
    for f in result.frames:
        assert f == f.upper()
        assert parse_frame(f).crc_ok
    msg = decode_frames(SPEC, result.frames)
    # Structure survives; free text is uppercased (documented lossy).
    assert [b["type"] for b in msg.blocks] == ["RESONANCE", "GR::EMIT"]
    assert msg.blocks[1]["signal"] == "COHERENCE_PULSE"
    assert msg.blocks[0]["intent"] == ["observe", "resonate"]


def test_loopback_reply_is_heard_by_the_receive_source_and_reassembles():
    """Full circle on one fake: we transmit, the 'far end' echoes RX.DIRECTED, the RX source hears it."""
    fake = FakeJs8Call(callsign="AE0RM", loopback_from="KANNAKA")
    heard: list = []

    async def go(client):
        src = Js8CallSource("127.0.0.1", fake.port)

        async def listen():
            async for h in src.run():
                heard.append(h)
                if len(heard) >= 3:
                    return

        listener = asyncio.create_task(listen())
        await asyncio.sleep(0.05)
        tx = Transmitter(SPEC, client, policy(), station="AE0RM")
        result = await tx.transmit(BLOCKS, "KANNAKA", message_id="7Q")
        try:
            await asyncio.wait_for(listener, 5.0)
        except asyncio.TimeoutError:
            listener.cancel()
        return result

    result = asyncio.run(with_fake(fake, go))
    assert result.ok
    assert len(heard) >= len(result.frames)
    asm = FrameAssembler(SPEC)
    done = None
    for h in heard:
        assert h.kind == "js8-directed" and h.source == "KANNAKA"
        r = asm.feed(h.text)
        if r:
            done = r
    assert done is not None and done[0].blocks == BLOCKS


def test_client_requests_correlate_by_id():
    fake = FakeJs8Call(callsign="AE0RM", grid="EM48")

    async def go(client):
        cs, grid, freq, speed = await asyncio.gather(client.get_callsign(), client.get_grid(), client.get_freq(), client.get_speed())
        return cs, grid, freq, speed

    cs, grid, freq, speed = asyncio.run(with_fake(fake, go))
    assert (cs, grid, speed) == ("AE0RM", "EM48", 0) and freq["DIAL"] == 7078000
    ids = [r["params"]["_ID"] for r in fake.requests]
    assert len(ids) == len(set(ids)) == 4
