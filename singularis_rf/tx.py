"""Transmit path for JS8Call — deliberately guarded.

    NATS RF.tx.<STATION>  {to, blocks, experimentId?, messageId?}  ─┐
    CLI  singularis-rf-tx --to AE0RM blocks.json                    ─┼─► TxGuard ─► encode (SP1) ─► JS8Call TX.SEND_MESSAGE, one frame per over
                                                                     │            └─► RF.tx.<STATION>.status per frame
                                                                     ▼
                                                        refused unless ALL of:
                                                          RF_TX_ENABLE=1
                                                          RF_CALLSIGN set and == JS8Call's configured station callsign
                                                          destination in RF_TX_ALLOWLIST (if set)
                                                          within the rate limit (RF_TX_MAX_FRAMES_PER_10MIN)

JS8Call is the transmitter and carries the licensed callsign on every over; this
driver only hands it text. The control operator configures JS8Call; if the callsign
there does not match RF_CALLSIGN, nothing is sent. Part 97 station identification is
satisfied by JS8Call's own directed-message framing (FROM: TO ...).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .codec import CodecError, Spec, encode_message, random_message_id
from .js8call import Js8CallClient, Js8CallError, directed_text


class TxRefused(RuntimeError):
    """The guard said no. The reason is the message."""


@dataclass
class TxPolicy:
    enabled: bool = False
    callsign: str | None = None
    allowlist: set[str] = field(default_factory=set)
    max_frames_per_10min: int = 30
    frame_length: int = 67
    case_fold: bool = False
    per_frame_timeout_s: float = 240.0   # a 67-char frame at JS8 normal speed is ~6-7 overs of 15 s
    gap_between_frames_s: float = 2.0

    @classmethod
    def from_env(cls, env=os.environ) -> "TxPolicy":
        allow = {c.strip().upper() for c in (env.get("RF_TX_ALLOWLIST") or "").split(",") if c.strip()}
        return cls(
            enabled=env.get("RF_TX_ENABLE", "0") in ("1", "true", "yes"),
            callsign=(env.get("RF_CALLSIGN") or "").strip().upper() or None,
            allowlist=allow,
            max_frames_per_10min=int(env.get("RF_TX_MAX_FRAMES_PER_10MIN", "30")),
            frame_length=int(env.get("RF_TX_FRAME_LENGTH", "67")),
            case_fold=env.get("RF_TX_CASE_FOLD", "0") in ("1", "true", "yes"),
            per_frame_timeout_s=float(env.get("RF_TX_FRAME_TIMEOUT_S", "240")),
            gap_between_frames_s=float(env.get("RF_TX_GAP_S", "2")),
        )


class TxGuard:
    def __init__(self, policy: TxPolicy, now: Callable[[], float] = time.monotonic):
        self.policy = policy
        self._now = now
        self._sent: deque[float] = deque()

    def check_station(self, js8_callsign: str) -> None:
        if not self.policy.enabled:
            raise TxRefused("transmit disabled (RF_TX_ENABLE is not 1)")
        if not self.policy.callsign:
            raise TxRefused("RF_CALLSIGN is not set; the licensed station callsign must be declared")
        if js8_callsign.strip().upper() != self.policy.callsign:
            raise TxRefused(f"JS8Call station callsign '{js8_callsign}' does not match RF_CALLSIGN '{self.policy.callsign}'")

    def check_destination(self, to: str) -> None:
        dest = (to or "*").strip().upper()
        if self.policy.allowlist and dest not in self.policy.allowlist and dest != "*":
            raise TxRefused(f"destination {dest} is not in RF_TX_ALLOWLIST")

    def reserve_frames(self, n: int) -> None:
        now = self._now()
        while self._sent and now - self._sent[0] > 600:
            self._sent.popleft()
        if len(self._sent) + n > self.policy.max_frames_per_10min:
            raise TxRefused(f"rate limit: {len(self._sent)} frames sent in the last 10 min, {n} more would exceed {self.policy.max_frames_per_10min}")
        for _ in range(n):
            self._sent.append(now)

    @property
    def frames_in_window(self) -> int:
        now = self._now()
        return sum(1 for t in self._sent if now - t <= 600)


@dataclass
class TxResult:
    message_id: str
    to: str
    frames: list[str]
    sent: int
    ok: bool
    error: str | None = None
    started_at: float = 0.0
    finished_at: float = 0.0


class Transmitter:
    def __init__(self, spec: Spec, client: Js8CallClient, policy: TxPolicy, station: str,
                 publish: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
                 subject_prefix: str = "RF"):
        self.spec = spec
        self.client = client
        self.policy = policy
        self.guard = TxGuard(policy)
        self.station = station
        self.publish = publish
        self.subject_prefix = subject_prefix
        self.js8_callsign: str | None = None
        self._lock = asyncio.Lock()   # one message on the air at a time

    async def status(self, payload: dict[str, Any]) -> None:
        if self.publish:
            await self.publish(f"{self.subject_prefix}.tx.{self.station}.status", {"station": self.station, "ts": time.time(), **payload})

    async def prepare(self) -> str:
        """Learn JS8Call's callsign and verify the guard once at startup (re-verified per message)."""
        self.js8_callsign = await self.client.get_callsign()
        self.guard.check_station(self.js8_callsign)
        return self.js8_callsign

    def frames_for(self, blocks: list[dict[str, Any]], to: str, message_id: str | None, experiment_id: str | None, session_id: str | None,
                   case_fold: bool | None = None) -> tuple[str, list[str]]:
        if not self.policy.callsign:
            raise TxRefused("RF_CALLSIGN is not set")
        mid = (message_id or random_message_id()).upper()
        frames = encode_message(self.spec, blocks, from_=self.policy.callsign, to=to, message_id=mid,
                                experiment_id=experiment_id, session_id=session_id,
                                frame_length=self.policy.frame_length,
                                case_fold=self.policy.case_fold if case_fold is None else case_fold)
        return mid, frames

    async def transmit(self, blocks: list[dict[str, Any]], to: str, *, message_id: str | None = None,
                       experiment_id: str | None = None, session_id: str | None = None,
                       ref: str | None = None, case_fold: bool | None = None) -> TxResult:
        """`ref` is the requester's correlation id; it is echoed on every status event so the
        requester (e.g. the Collective's delivery ledger) can follow this message without polling."""
        async with self._lock:
            started = time.time()
            to = (to or "*").strip().upper()
            tag = {"ref": ref} if ref else {}
            try:
                self.js8_callsign = await self.client.get_callsign()
                self.guard.check_station(self.js8_callsign)
                self.guard.check_destination(to)
                mid, frames = self.frames_for(blocks, to, message_id, experiment_id, session_id, case_fold)
                self.guard.reserve_frames(len(frames))
            except (TxRefused, CodecError, Js8CallError) as e:
                await self.status({"event": "refused", "to": to, "error": str(e), **tag})
                return TxResult(message_id=message_id or "", to=to, frames=[], sent=0, ok=False, error=str(e), started_at=started, finished_at=time.time())

            await self.status({"event": "start", "message_id": mid, "to": to, "frames": len(frames), "callsign": self.js8_callsign, **tag})
            sent = 0
            for i, frame in enumerate(frames, 1):
                text = directed_text(to, frame)
                try:
                    await self.client.send_message(text)
                    done = await self.client.wait_tx_idle(self.policy.per_frame_timeout_s)
                except Js8CallError as e:
                    await self.status({"event": "error", "message_id": mid, "frame": i, "error": str(e), **tag})
                    return TxResult(mid, to, frames, sent, False, f"frame {i}: {e}", started, time.time())
                if not done:
                    await self.status({"event": "timeout", "message_id": mid, "frame": i, **tag})
                    return TxResult(mid, to, frames, sent, False, f"frame {i}: JS8Call did not finish within {self.policy.per_frame_timeout_s:.0f}s", started, time.time())
                sent += 1
                await self.status({"event": "frame_sent", "message_id": mid, "frame": i, "of": len(frames), "text": text, **tag})
                if i < len(frames):
                    await asyncio.sleep(self.policy.gap_between_frames_s)
            await self.status({"event": "complete", "message_id": mid, "to": to, "frames": len(frames), **tag})
            return TxResult(mid, to, frames, sent, True, None, started, time.time())


def parse_tx_request(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate an RF.tx.<STATION> request. Either {blocks:[...]} or {frames:[...]} (pre-encoded, rejected here: we always encode with our own callsign)."""
    if not isinstance(payload, dict):
        raise TxRefused("request must be a JSON object")
    blocks = payload.get("blocks")
    if not isinstance(blocks, list) or not blocks or not all(isinstance(b, dict) and "type" in b for b in blocks):
        raise TxRefused("request needs a non-empty 'blocks' list of Singularis blocks")
    to = str(payload.get("to") or "*")
    return {
        "blocks": blocks, "to": to,
        "message_id": str(payload["messageId"]).upper() if payload.get("messageId") else None,
        "experiment_id": payload.get("experimentId"), "session_id": payload.get("sessionId"),
        # `ref` is the requester's correlation id (the Collective uses its delivery id); echoed on every status event.
        "ref": str(payload["ref"])[:64] if payload.get("ref") else None,
        "case_fold": bool(payload.get("caseFold", False)),
    }


async def serve_requests(transmitter: Transmitter, nc, station: str, subject_prefix: str = "RF") -> None:
    """Consume RF.tx.<STATION> from NATS and transmit; reply on the request's reply subject when present."""
    subject = f"{subject_prefix}.tx.{station}"
    sub = await nc.subscribe(subject)
    print(f"[tx] listening on {subject}", file=sys.stderr)
    async for msg in sub.messages:
        try:
            req = parse_tx_request(json.loads(msg.data.decode("utf-8")))
            result = await transmitter.transmit(req["blocks"], req["to"], message_id=req["message_id"], experiment_id=req["experiment_id"],
                                                session_id=req["session_id"], ref=req["ref"], case_fold=req["case_fold"] or None)
            out = {"ok": result.ok, "message_id": result.message_id, "to": result.to, "frames": result.frames, "sent": result.sent, "error": result.error, "ref": req["ref"]}
        except (TxRefused, ValueError) as e:
            out = {"ok": False, "error": str(e)}
        if msg.reply:
            await nc.publish(msg.reply, json.dumps(out).encode("utf-8"))
