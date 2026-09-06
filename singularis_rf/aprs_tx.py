"""APRS transmit path via Direwolf — guarded like the JS8Call driver.

    NATS RF.tx.<STATION> {to, blocks, ref?} ─► TxGuard ─► SP1 frames ─► one APRS message per frame
        :ADDRESSEE:<frame>{n}   as an AX.25 UI frame from RF_CALLSIGN via KISS ─► Direwolf keys the radio
                                    ▲                                          │
                                    │  ack n from ADDRESSEE (directed only)     │ AGW 'T' = frame went out
                                    └──────────────────────────────────────────┘
    Incoming APRS messages addressed to RF_CALLSIGN with {n} get an `ack n` (auto-ack), the way any APRS
    station is expected to behave.

Guard: RF_TX_ENABLE=1, RF_CALLSIGN set and AX.25-valid (≤6 chars + optional -SSID), destination in
RF_TX_ALLOWLIST if set, rate limit. Direwolf's MYCALL is rendered from RF_CALLSIGN at container start
so the two cannot disagree. Every AX.25 frame carries the callsign in its source field: that is the
Part 97 station identification.
"""
from __future__ import annotations

import asyncio
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .agw import AgwMonitor, monitor_text
from .ax25 import KissDecoder, aprs_message_info, build_ui_frame, extract_text, kiss_encode, parse_ax25
from .codec import CodecError, Spec, encode_message, random_message_id
from .tx import TxGuard, TxPolicy, TxRefused, TxResult

AX25_CALL = re.compile(r"^[A-Z0-9]{1,6}(-(?:[0-9]|1[0-5]))?$")
# APRS "tocall" for experimental/unregistered software is APZxxx.
APRS_TOCALL = "APZSCC"


@dataclass
class AprsPolicy:
    path: list[str]                 # digipeater path, e.g. ["WIDE1-1"]; [] for direct
    ack_timeout_s: float = 30.0     # wait for `ack n` before retrying a directed message
    retries: int = 3                # APRS convention: a few tries, then give up
    tx_confirm_timeout_s: float = 20.0  # AGW 'T' should follow a KISS send within seconds
    require_tx_confirm: bool = True
    gap_between_frames_s: float = 5.0
    auto_ack: bool = True

    @classmethod
    def from_env(cls, env) -> "AprsPolicy":
        path = [p.strip().upper() for p in (env.get("RF_APRS_PATH", "WIDE1-1")).split(",") if p.strip()]
        return cls(
            path=path,
            ack_timeout_s=float(env.get("RF_APRS_ACK_TIMEOUT_S", "30")),
            retries=int(env.get("RF_APRS_RETRIES", "3")),
            tx_confirm_timeout_s=float(env.get("RF_APRS_TX_CONFIRM_S", "20")),
            require_tx_confirm=env.get("RF_APRS_REQUIRE_TX_CONFIRM", "1") in ("1", "true", "yes"),
            gap_between_frames_s=float(env.get("RF_APRS_GAP_S", "5")),
            auto_ack=env.get("RF_APRS_AUTO_ACK", "1") in ("1", "true", "yes"),
        )


def validate_ax25_callsign(callsign: str | None) -> str:
    cs = (callsign or "").strip().upper()
    if not AX25_CALL.match(cs):
        raise TxRefused(f"RF_CALLSIGN '{callsign}' is not a valid AX.25 callsign (≤6 alphanumerics, optional -SSID 0-15)")
    return cs


def aprs_addressee(to: str) -> str:
    dest = (to or "*").strip().upper()
    if dest in ("*", "@ALLCALL", "ALLCALL"):
        return "ALL"  # APRS convention for a message to everyone (no acks expected)
    if len(dest) > 9:
        raise TxRefused(f"APRS addressee '{dest}' exceeds 9 characters")
    return dest


class AprsTransmitter:
    """Talks to Direwolf over KISS (send + hear) and AGW (transmit confirmation)."""

    def __init__(self, spec: Spec, policy: TxPolicy, aprs: AprsPolicy, station: str,
                 kiss_host: str = "127.0.0.1", kiss_port: int = 8001, agw_host: str = "127.0.0.1", agw_port: int = 8000,
                 publish: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None, subject_prefix: str = "RF"):
        self.spec, self.policy, self.aprs, self.station = spec, policy, aprs, station
        self.kiss_host, self.kiss_port, self.agw_host, self.agw_port = kiss_host, kiss_port, agw_host, agw_port
        self.publish, self.subject_prefix = publish, subject_prefix
        self.guard = TxGuard(policy)
        self.callsign: str | None = None
        self._lock = asyncio.Lock()
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task | None = None
        self._agw_task: asyncio.Task | None = None
        self._acks: dict[str, asyncio.Event] = {}          # "ADDRESSEE:n" → set when heard
        self._tx_confirms: dict[str, asyncio.Event] = {}   # info text → set when AGW 'T' seen
        self._msg_no = 0
        self.counters = {"frames_sent": 0, "acks_received": 0, "acks_sent": 0, "tx_confirmed": 0, "retries": 0}

    # ---- lifecycle ----
    async def prepare(self) -> str:
        self.callsign = validate_ax25_callsign(self.policy.callsign)
        self.guard.check_station(self.callsign)  # enabled + declared; the KISS side has no callsign to compare
        reader, self._writer = await asyncio.open_connection(self.kiss_host, self.kiss_port)
        self._reader_task = asyncio.create_task(self._hear(reader))
        self._agw_task = asyncio.create_task(self._watch_agw())
        return self.callsign

    async def close(self) -> None:
        for t in (self._reader_task, self._agw_task):
            if t:
                t.cancel()
        if self._writer:
            self._writer.close()

    async def status(self, payload: dict[str, Any]) -> None:
        if self.publish:
            await self.publish(f"{self.subject_prefix}.tx.{self.station}.status", {"station": self.station, "mode": "aprs", "ts": time.time(), **payload})

    # ---- radio side ----
    async def _send_info(self, info: str) -> None:
        assert self._writer and self.callsign
        frame = build_ui_frame(self.callsign, APRS_TOCALL, info, self.aprs.path)
        self._writer.write(kiss_encode(frame))
        await self._writer.drain()

    async def _hear(self, reader: asyncio.StreamReader) -> None:
        dec = KissDecoder()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    raise ConnectionError("KISS peer closed")
                for raw in dec.feed(data):
                    try:
                        heard = extract_text(parse_ax25(raw))
                    except ValueError:
                        continue
                    if not heard or not self.callsign:
                        continue
                    if heard.kind == "aprs-ack" and heard.addressee == self.callsign:
                        m = re.match(r"^ack([A-Za-z0-9]{1,5})", heard.text)
                        if m:
                            key = f"{heard.source}:{m.group(1)}"
                            self.counters["acks_received"] += 1
                            ev = self._acks.get(key)
                            if ev:
                                ev.set()
                    elif heard.kind == "aprs-message" and heard.addressee == self.callsign and heard.msg_no and self.aprs.auto_ack:
                        await self._send_info(aprs_message_info(heard.source, f"ack{heard.msg_no}"))
                        self.counters["acks_sent"] += 1
        except (OSError, ConnectionError) as e:
            print(f"[aprs] KISS hearing stopped: {e}", file=sys.stderr)

    async def _watch_agw(self) -> None:
        mon = AgwMonitor(self.agw_host, self.agw_port)
        async for f in mon.frames():
            if f.kind != "T":
                continue
            text = monitor_text(f)
            info = text.split(":", 1)[1] if ":" in text else text
            for key, ev in list(self._tx_confirms.items()):
                if info.rstrip().endswith(key) or key in info:
                    ev.set()
                    self.counters["tx_confirmed"] += 1

    def _next_msg_no(self) -> str:
        self._msg_no = (self._msg_no % 99999) + 1
        return str(self._msg_no)

    async def _send_and_confirm(self, info: str) -> bool:
        ev = asyncio.Event()
        self._tx_confirms[info] = ev
        try:
            await self._send_info(info)
            if not self.aprs.require_tx_confirm:
                return True
            try:
                await asyncio.wait_for(ev.wait(), self.aprs.tx_confirm_timeout_s)
                return True
            except asyncio.TimeoutError:
                return False
        finally:
            self._tx_confirms.pop(info, None)

    # ---- the transmit ----
    async def transmit(self, blocks: list[dict[str, Any]], to: str, *, message_id: str | None = None,
                       experiment_id: str | None = None, session_id: str | None = None,
                       ref: str | None = None, case_fold: bool | None = None) -> TxResult:
        async with self._lock:
            started = time.time()
            tag = {"ref": ref} if ref else {}
            try:
                self.callsign = validate_ax25_callsign(self.policy.callsign)
                self.guard.check_station(self.callsign)
                self.guard.check_destination(to)
                addressee = aprs_addressee(to)
                mid = (message_id or random_message_id()).upper()
                frames = encode_message(self.spec, blocks, from_=self.callsign, to=addressee if addressee != "ALL" else "*", message_id=mid,
                                        experiment_id=experiment_id, session_id=session_id,
                                        frame_length=self.policy.frame_length,
                                        case_fold=self.policy.case_fold if case_fold is None else case_fold)
                self.guard.reserve_frames(len(frames))
            except (TxRefused, CodecError) as e:
                await self.status({"event": "refused", "to": to, "error": str(e), **tag})
                return TxResult(message_id=message_id or "", to=to, frames=[], sent=0, ok=False, error=str(e), started_at=started, finished_at=time.time())

            directed = addressee != "ALL"
            await self.status({"event": "start", "message_id": mid, "to": addressee, "frames": len(frames), "callsign": self.callsign, "path": self.aprs.path, **tag})
            sent = 0
            for i, frame in enumerate(frames, 1):
                n = self._next_msg_no() if directed else None
                info = aprs_message_info(addressee, frame, n)
                acked = False
                for attempt in range(1, (self.aprs.retries if directed else 1) + 1):
                    if attempt > 1:
                        self.counters["retries"] += 1
                        await self.status({"event": "retry", "message_id": mid, "frame": i, "attempt": attempt, **tag})
                    ack_ev = asyncio.Event()
                    if directed and n:
                        self._acks[f"{addressee}:{n}"] = ack_ev
                    on_air = await self._send_and_confirm(info)
                    if not on_air:
                        self._acks.pop(f"{addressee}:{n}", None)
                        await self.status({"event": "error", "message_id": mid, "frame": i, "error": "no transmit confirmation from Direwolf (AGW 'T')", **tag})
                        return TxResult(mid, addressee, frames, sent, False, f"frame {i}: not confirmed on air", started, time.time())
                    self.counters["frames_sent"] += 1
                    if not directed:
                        acked = True
                        break
                    try:
                        await asyncio.wait_for(ack_ev.wait(), self.aprs.ack_timeout_s)
                        acked = True
                        break
                    except asyncio.TimeoutError:
                        continue
                    finally:
                        self._acks.pop(f"{addressee}:{n}", None)
                if directed and not acked:
                    await self.status({"event": "timeout", "message_id": mid, "frame": i, "error": f"no ack from {addressee} after {self.aprs.retries} tries", **tag})
                    return TxResult(mid, addressee, frames, sent, False, f"frame {i}: no ack from {addressee}", started, time.time())
                sent += 1
                await self.status({"event": "frame_sent", "message_id": mid, "frame": i, "of": len(frames), "text": info, "acked": directed, **tag})
                if i < len(frames):
                    await asyncio.sleep(self.aprs.gap_between_frames_s)
            await self.status({"event": "complete", "message_id": mid, "to": addressee, "frames": len(frames), **tag})
            return TxResult(mid, addressee, frames, sent, True, None, started, time.time())
