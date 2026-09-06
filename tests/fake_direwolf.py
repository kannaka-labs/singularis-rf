"""A stand-in for Direwolf's network side, for tests.

KISS TCP: accepts data frames from any client (a "transmission"), then — like Direwolf
monitoring its own TX — emits an AGW 'T' frame to monitoring clients, and delivers
frames heard "on the air" to every KISS client. An optional ack bot plays a distant
APRS station: any message addressed to it with {n} is answered with `ack n`.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from singularis_rf.agw import HEADER_LEN, decode_frames, encode_frame
from singularis_rf.ax25 import KissDecoder, aprs_message_info, build_ui_frame, extract_text, kiss_encode, parse_ax25


def monitor_line(raw: bytes) -> str:
    fr = parse_ax25(raw)
    path = "," + ",".join(fr.digipeaters) if fr.digipeaters else ""
    return f"{fr.source}>{fr.destination}{path}:{fr.text}"


@dataclass
class FakeDirewolf:
    ack_bot: str | None = None          # callsign of a station that acks messages addressed to it
    ack_delay: float = 0.05
    tx_delay: float = 0.02              # time from KISS receipt to AGW 'T'
    drop_first_acks: int = 0            # simulate lost acks: ignore this many messages before acking
    transmitted: list[bytes] = field(default_factory=list)
    kiss_port: int = 0
    agw_port: int = 0
    _kiss_writers: list[asyncio.StreamWriter] = field(default_factory=list)
    _agw_writers: list[asyncio.StreamWriter] = field(default_factory=list)
    _servers: list[asyncio.AbstractServer] = field(default_factory=list)
    _dropped: int = 0

    async def start(self) -> None:
        k = await asyncio.start_server(self._kiss, "127.0.0.1", 0)
        a = await asyncio.start_server(self._agw, "127.0.0.1", 0)
        self.kiss_port = k.sockets[0].getsockname()[1]
        self.agw_port = a.sockets[0].getsockname()[1]
        self._servers = [k, a]

    async def stop(self) -> None:
        for w in self._kiss_writers + self._agw_writers:
            w.close()
        for s in self._servers:
            s.close()
            await s.wait_closed()

    async def hear(self, raw_ax25: bytes) -> None:
        """Deliver a frame heard on the air to every KISS client."""
        for w in list(self._kiss_writers):
            try:
                w.write(kiss_encode(raw_ax25))
                await w.drain()
            except (ConnectionError, OSError):
                pass

    async def _kiss(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._kiss_writers.append(writer)
        dec = KissDecoder()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                for raw in dec.feed(data):
                    self.transmitted.append(raw)
                    asyncio.create_task(self._after_tx(raw))
        except (ConnectionError, OSError):
            pass
        finally:
            if writer in self._kiss_writers:
                self._kiss_writers.remove(writer)

    async def _after_tx(self, raw: bytes) -> None:
        await asyncio.sleep(self.tx_delay)
        fr = parse_ax25(raw)
        line = monitor_line(raw)
        for w in list(self._agw_writers):
            try:
                w.write(encode_frame("T", fr.source, fr.destination, (line + "\r").encode()))
                await w.drain()
            except (ConnectionError, OSError):
                pass
        heard = extract_text(fr)
        if self.ack_bot and heard and heard.kind == "aprs-message" and heard.addressee == self.ack_bot and heard.msg_no:
            if self._dropped < self.drop_first_acks:
                self._dropped += 1
                return
            await asyncio.sleep(self.ack_delay)
            await self.hear(build_ui_frame(self.ack_bot, "APRS", aprs_message_info(fr.source, f"ack{heard.msg_no}")))

    async def _agw(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        monitoring = False
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                buf += data
                for f in decode_frames(buf):
                    if f.kind == "m":
                        monitoring = not monitoring
                        if monitoring:
                            self._agw_writers.append(writer)
                        elif writer in self._agw_writers:
                            self._agw_writers.remove(writer)
        except (ConnectionError, OSError):
            pass
        finally:
            if writer in self._agw_writers:
                self._agw_writers.remove(writer)
