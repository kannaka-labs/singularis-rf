"""JS8Call TCP JSON API client (Settings → Reporting → API; default 127.0.0.1:2442).

Protocol: newline-delimited JSON objects {"type": "...", "value": "...", "params": {...}}.
Requests may carry params._ID; the matching response echoes it. Events arrive unsolicited:
RX.DIRECTED, RX.ACTIVITY, RX.SPOT, TX.FRAME, TX.TEXT, RIG.PTT, RIG.FREQ, STATION.STATUS, PING, CLOSE.

Requests used here:
  STATION.GET_CALLSIGN  → STATION.CALLSIGN {value}
  STATION.GET_GRID      → STATION.GRID
  RIG.GET_FREQ          → RIG.FREQ {params.DIAL, params.FREQ, params.OFFSET}
  MODE.GET_SPEED        → MODE.SPEED {params.SPEED}
  TX.GET_TEXT           → TX.TEXT {value}
  TX.SET_TEXT           (sets the outgoing text without sending)
  TX.SEND_MESSAGE       (sets the outgoing text and transmits on the next cycle)

Real-radio validation of TX.SEND_MESSAGE sequencing is pending; everything here is
exercised against tests/fake_js8call.py, which mirrors the documented shapes.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable


class Js8CallError(RuntimeError):
    pass


@dataclass
class Js8Event:
    type: str
    value: str
    params: dict[str, Any] = field(default_factory=dict)
    received_at: float = 0.0


class Js8CallClient:
    """One connection; request/response by _ID plus a fan-out of events to listeners."""

    def __init__(self, host: str = "127.0.0.1", port: int = 2442, request_timeout: float = 10.0):
        self.host, self.port = host, port
        self.request_timeout = request_timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._ids = itertools.count(1)
        self._pending: dict[str, asyncio.Future] = {}
        self._listeners: list[Callable[[Js8Event], Awaitable[None] | None]] = []
        self._reader_task: asyncio.Task | None = None
        self.connected = False
        self.last_tx_text: str | None = None
        self.events_seen = 0

    # ---- lifecycle ----
    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        self.connected = True
        self._reader_task = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        self.connected = False
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(Js8CallError("connection closed"))
        self._pending.clear()

    def on_event(self, listener: Callable[[Js8Event], Awaitable[None] | None]) -> None:
        self._listeners.append(listener)

    # ---- I/O ----
    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                line = await self._reader.readline()
                if not line:
                    raise ConnectionError("JS8Call closed the API socket")
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ev = Js8Event(type=str(msg.get("type", "")), value=str(msg.get("value", "") or ""), params=dict(msg.get("params") or {}), received_at=time.time())
                self.events_seen += 1
                if ev.type == "TX.TEXT":
                    self.last_tx_text = ev.value
                rid = ev.params.get("_ID")
                fut = self._pending.pop(str(rid), None) if rid is not None else None
                if fut is not None and not fut.done():
                    fut.set_result(ev)
                for cb in list(self._listeners):
                    try:
                        r = cb(ev)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception as e:  # noqa: BLE001
                        print(f"[js8call] listener failed: {e}", file=sys.stderr)
        except (ConnectionError, OSError, asyncio.IncompleteReadError) as e:
            self.connected = False
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(Js8CallError(str(e)))
            self._pending.clear()

    async def send(self, type_: str, value: str = "", params: dict[str, Any] | None = None) -> None:
        if not self._writer or not self.connected:
            raise Js8CallError("not connected")
        msg = {"type": type_, "value": value, "params": params or {}}
        self._writer.write((json.dumps(msg) + "\n").encode("utf-8"))
        await self._writer.drain()

    async def request(self, type_: str, value: str = "", params: dict[str, Any] | None = None) -> Js8Event:
        rid = str(next(self._ids))
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut
        await self.send(type_, value, {**(params or {}), "_ID": rid})
        try:
            return await asyncio.wait_for(fut, self.request_timeout)
        except asyncio.TimeoutError as e:
            self._pending.pop(rid, None)
            raise Js8CallError(f"{type_} timed out") from e

    # ---- convenience ----
    async def get_callsign(self) -> str:
        return (await self.request("STATION.GET_CALLSIGN")).value.strip().upper()

    async def get_grid(self) -> str:
        return (await self.request("STATION.GET_GRID")).value.strip()

    async def get_freq(self) -> dict[str, Any]:
        return (await self.request("RIG.GET_FREQ")).params

    async def get_speed(self) -> int | None:
        p = (await self.request("MODE.GET_SPEED")).params
        return int(p["SPEED"]) if "SPEED" in p else None

    async def get_tx_text(self) -> str:
        return (await self.request("TX.GET_TEXT")).value

    async def send_message(self, text: str) -> None:
        """Queue text for transmission. JS8Call transmits it on its next TX cycle."""
        await self.send("TX.SEND_MESSAGE", text)

    async def wait_tx_idle(self, timeout: float, poll: float = 2.0) -> bool:
        """True when JS8Call reports an empty outgoing text buffer (message fully sent)."""
        deadline = time.monotonic() + timeout
        poll = min(poll, max(0.2, timeout / 20))  # short timeouts (tests) poll faster
        saw_text = False
        while time.monotonic() < deadline:
            try:
                txt = await self.get_tx_text()
            except Js8CallError:
                txt = self.last_tx_text or ""
            if txt.strip():
                saw_text = True
            elif saw_text or self.last_tx_text == "":
                return True
            await asyncio.sleep(poll)
        return False

    async def events(self) -> AsyncIterator[Js8Event]:
        q: asyncio.Queue[Js8Event] = asyncio.Queue()
        self.on_event(q.put_nowait)
        while self.connected or not q.empty():
            try:
                yield await asyncio.wait_for(q.get(), 1.0)
            except asyncio.TimeoutError:
                continue


def directed_text(to: str, text: str) -> str:
    """JS8Call directed-message form: 'CALLSIGN: text' ('@ALLCALL: text' for everyone)."""
    dest = to.strip().upper()
    if dest in ("", "*"):
        dest = "@ALLCALL"
    return f"{dest}: {text}"
