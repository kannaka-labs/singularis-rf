#!/usr/bin/env bash
# Entrypoint: audio → Direwolf (KISS on 127.0.0.1:8001) → Singularis RX station → NATS.
# Modes (RF_AUDIO):
#   rtl    : rtl_fm on RF_FREQ (default APRS 144.390 MHz) from an RTL-SDR dongle       [needs --device /dev/bus/usb]
#   wav    : play RF_WAV once through Direwolf (self-test / replay), then exit
#   none   : no Direwolf; the station reads RF_SOURCE (js8call | stdin) directly
set -euo pipefail

RF_AUDIO="${RF_AUDIO:-rtl}"
RF_FREQ="${RF_FREQ:-144.390M}"
RF_GAIN="${RF_GAIN:-40}"
RF_PPM="${RF_PPM:-0}"
mkdir -p /data/direwolf

# Direwolf: -t 0 no colours, -q hd quiet heard/decoded chatter, -r sample rate, '-' = raw audio on stdin.
# NOTE: run direwolf as the last stage of a backgrounded pipeline and take $! — a helper function
# inside the pipeline would set its PID in a subshell the parent never sees.
DW="direwolf -c /app/deploy/direwolf.conf -r 22050 -t 0 -q hd -"
DW_PID=""

case "$RF_AUDIO" in
  rtl)
    # The station runs regardless (heartbeats say connected:false until audio flows). The audio
    # chain waits for a dongle and restarts if it is unplugged, so plugging an RTL-SDR into the VM
    # is all that is needed — no container restart.
    python3 -m singularis_rf "$@" &
    ST_PID=$!
    trap 'kill "$ST_PID" 2>/dev/null; exit 0' TERM INT
    while true; do
      until rtl_test -t >/dev/null 2>&1; do
        echo "[run-rx] no RTL-SDR dongle detected; waiting (attach the device to /dev/bus/usb)" >&2
        sleep "${RF_DONGLE_POLL_S:-60}"
      done
      echo "[run-rx] rtl_fm ${RF_FREQ} → direwolf" >&2
      rtl_fm -f "$RF_FREQ" -M fm -s 22050 -g "$RF_GAIN" -p "$RF_PPM" -E dc - | $DW || true
      echo "[run-rx] audio chain ended; re-checking dongle in 10 s" >&2
      sleep 10
    done
    ;;
  wav)
    : "${RF_WAV:?RF_AUDIO=wav needs RF_WAV}"
    echo "[run-rx] replaying $RF_WAV → direwolf" >&2
    # Give the station a moment to connect to KISS before audio starts flowing.
    ( sleep 3; sox "$RF_WAV" -t raw -r 22050 -e signed -b 16 -c 1 - ) | $DW &
    DW_PID=$!
    ;;
  none)
    echo "[run-rx] no audio chain; station source=${RF_SOURCE}" >&2
    ;;
  *) echo "unknown RF_AUDIO=$RF_AUDIO" >&2; exit 2 ;;
esac

if [ "$RF_AUDIO" = "wav" ]; then
  # Self-test: run the station until Direwolf finishes the replay, then a grace period.
  python3 -m singularis_rf "$@" &
  ST_PID=$!
  wait "$DW_PID" || true
  sleep "${RF_WAV_GRACE:-5}"
  kill "$ST_PID" 2>/dev/null || true
  wait "$ST_PID" 2>/dev/null || true
  exit 0
fi

exec python3 -m singularis_rf "$@"
