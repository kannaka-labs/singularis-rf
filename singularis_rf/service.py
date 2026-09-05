"""RX-only Singularis Compact station.

    audio (RTL-SDR via rtl_fm, or a file) → Direwolf → KISS TCP ─┐
    JS8Call TCP API ────────────────────────────────────────────┼─► heard text
    stdin / file lines ─────────────────────────────────────────┘        │
                                                                          ▼
                                       FrameAssembler (Singularis Compact, spec-driven)
                                                                          │
              ┌───────────────────────────────────────────────────────────┤
              ▼                                                           ▼
     NATS RF.rx.<from>  (complete Singularis messages)          NATS RF.heard.<source>  (all traffic heard)
     NATS RF.station.<station> heartbeat every 60 s             JSONL log on disk

Never transmits. There is no TX path in this service by design: transmission
needs a licensed control operator and belongs to a separate, deliberately
enabled component.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .ax25 import HeardText
from .codec import BUNDLED_SPEC, FrameAssembler, Spec, looks_like_frame
from .sources import Js8CallSource, KissTcpSource, LineSource


@dataclass
class Config:
    station: str
    source: str            # kiss | js8call | stdin
    kiss_host: str = "127.0.0.1"
    kiss_port: int = 8001
    js8_host: str = "127.0.0.1"
    js8_port: int = 2442
    nats_url: str | None = None
    nats_user: str | None = None
    nats_password: str | None = None
    log_path: str | None = None
    spec_path: str = str(BUNDLED_SPEC)
    heartbeat_s: int = 60
    subject_prefix: str = "RF"

    @classmethod
    def from_env(cls, argv: list[str] | None = None) -> "Config":
        p = argparse.ArgumentParser(description="Singularis Compact RX-only station")
        p.add_argument("--source", choices=["kiss", "js8call", "stdin"], default=os.environ.get("RF_SOURCE", "kiss"))
        p.add_argument("--station", default=os.environ.get("RF_STATION") or f"RX-{socket.gethostname()}")
        p.add_argument("--kiss", default=os.environ.get("KISS_ADDR", "127.0.0.1:8001"))
        p.add_argument("--js8call", default=os.environ.get("JS8CALL_ADDR", "127.0.0.1:2442"))
        p.add_argument("--log", default=os.environ.get("RF_LOG"))
        p.add_argument("--spec", default=os.environ.get("RF_SPEC", str(BUNDLED_SPEC)))
        p.add_argument("--no-nats", action="store_true")
        p.add_argument("--once", action="store_true", help="exit when the source ends (stdin/file mode)")
        a = p.parse_args(argv)
        kh, kp = a.kiss.rsplit(":", 1)
        jh, jp = a.js8call.rsplit(":", 1)
        return cls(
            station=a.station.upper(), source=a.source, kiss_host=kh, kiss_port=int(kp), js8_host=jh, js8_port=int(jp),
            nats_url=None if a.no_nats else (os.environ.get("KANNAKA_NATS_URL") or os.environ.get("NATS_URL")),
            nats_user=os.environ.get("NATS_USER"), nats_password=os.environ.get("NATS_PASSWORD"),
            log_path=a.log, spec_path=a.spec,
        ), a.once


class Station:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.spec = Spec.load(cfg.spec_path)
        self.assembler = FrameAssembler(self.spec)
        self.nc = None
        self.started = time.time()
        self.counters = {"heard": 0, "frames": 0, "messages": 0, "published": 0, "nats_errors": 0}
        self.log = open(cfg.log_path, "a", encoding="utf-8") if cfg.log_path else None
        if cfg.source == "kiss":
            self.source: Any = KissTcpSource(cfg.kiss_host, cfg.kiss_port)
        elif cfg.source == "js8call":
            self.source = Js8CallSource(cfg.js8_host, cfg.js8_port)
        else:
            self.source = LineSource()

    # ---- output ----
    async def connect_nats(self) -> None:
        if not self.cfg.nats_url:
            print("[station] NATS disabled (no KANNAKA_NATS_URL/NATS_URL)", file=sys.stderr)
            return
        try:
            import nats  # nats-py
            self.nc = await nats.connect(
                servers=self.cfg.nats_url.split(","), user=self.cfg.nats_user, password=self.cfg.nats_password,
                name=f"singularis-rf-{self.cfg.station}", max_reconnect_attempts=-1, reconnect_time_wait=5,
            )
            print(f"[station] NATS connected {self.nc.connected_url.netloc}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001 — the station must keep decoding without the bus
            self.counters["nats_errors"] += 1
            print(f"[station] NATS connect failed: {e}", file=sys.stderr)
            self.nc = None

    async def publish(self, subject: str, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=False)
        if self.log:
            self.log.write(json.dumps({"subject": subject, **payload}, ensure_ascii=False) + "\n")
            self.log.flush()
        if self.nc is None:
            return
        try:
            await self.nc.publish(subject, line.encode("utf-8"))
            self.counters["published"] += 1
        except Exception as e:  # noqa: BLE001
            self.counters["nats_errors"] += 1
            print(f"[station] publish {subject} failed: {e}", file=sys.stderr)

    def _subject(self, *parts: str) -> str:
        clean = [p.replace(".", "-").replace(" ", "-").replace("*", "ALL") or "UNKNOWN" for p in parts]
        return ".".join([self.cfg.subject_prefix, *clean])

    # ---- core ----
    async def on_heard(self, heard: HeardText) -> None:
        self.counters["heard"] += 1
        now = time.time()
        base = {
            "station": self.cfg.station, "source": self.source.name, "kind": heard.kind,
            "from": heard.source, "to": heard.destination, "text": heard.text, "ts": now,
        }
        await self.publish(self._subject("heard", self.source.name), base)
        if not looks_like_frame(heard.text):
            return
        self.counters["frames"] += 1
        result = self.assembler.feed(heard.text, now)
        if result is None:
            return
        message, frames = result
        self.counters["messages"] += 1
        payload = {
            "station": self.cfg.station, "source": self.source.name, "ts": now,
            "message": message.to_dict(),
            "frames": [f.raw for f in frames],
            "heard_via": {"from": heard.source, "to": heard.destination},
            "spec": {"protocol": self.spec.protocol, "version": self.spec.version},
        }
        await self.publish(self._subject("rx", message.from_), payload)
        print(f"[station] RX {message.from_}>{message.to} {message.message_id}: {len(message.blocks)} block(s) via {heard.source}", file=sys.stderr)

    async def heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            await self.publish(self._subject("station", self.cfg.station), {
                "station": self.cfg.station, "source": self.source.name, "connected": bool(getattr(self.source, "connected", True)),
                "uptime_s": int(time.time() - self.started), "counters": dict(self.counters),
                "pending_messages": self.assembler.pending_count, "spec": {"protocol": self.spec.protocol, "version": self.spec.version},
                "rx_only": True, "ts": time.time(),
            })

    async def run(self, once: bool = False) -> None:
        await self.connect_nats()
        hb = asyncio.create_task(self.heartbeat())
        try:
            async for heard in self.source.run():
                await self.on_heard(heard)
            if not once:
                await asyncio.Event().wait()
        finally:
            hb.cancel()
            if self.nc is not None:
                try:
                    await self.nc.drain()
                except Exception:  # noqa: BLE001
                    pass
            if self.log:
                self.log.close()


def main(argv: list[str] | None = None) -> int:
    cfg, once = Config.from_env(argv)
    print(f"[station] {cfg.station} RX-only, source={cfg.source}, spec={Path(cfg.spec_path).name}", file=sys.stderr)
    station = Station(cfg)
    try:
        asyncio.run(station.run(once=once))
    except KeyboardInterrupt:
        pass
    print(f"[station] counters {json.dumps(station.counters)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
