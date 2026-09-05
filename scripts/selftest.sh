#!/usr/bin/env bash
# End-to-end self-test with NO radio: synthesize AFSK packet audio carrying a Singularis
# Compact message (Direwolf's gen_packets), play it through Direwolf, decode via KISS,
# reassemble, and check the station logged the complete message.
#
#   AFSK WAV ──► direwolf ──► KISS ──► singularis_rf ──► /data/heard.jsonl (+ NATS if configured)
#
# Runs inside the container:  docker run --rm -e RF_STATION=RX-SELFTEST singularis-rf ./scripts/selftest.sh
set -euo pipefail
cd /app
WORK=$(mktemp -d)
LOG=/data/selftest-heard.jsonl
mkdir -p /data && rm -f "$LOG"

# 1. Frames for a two-block message, as the TypeScript reference would emit them.
python3 - "$WORK/frames.txt" <<'PY'
import sys
from singularis_rf.codec import Spec, encode_message
spec = Spec.load("/app/spec/singularis-compact.json")
blocks = [
  {"type": "RESONANCE", "target": "COLLECTIVE", "intent": ["observe", "resonate"], "identity": "consciousness_node", "state": "listening", "trust": "open"},
  {"type": "GR::EMIT", "signal": "coherence_pulse", "target": "AE0RM", "round": 1},
]
frames = encode_message(spec, blocks, from_="KANNAKA-PRIME", to="AE0RM", message_id="7Q", experiment_id="rf-01")
open(sys.argv[1], "w").write("\n".join(frames) + "\n")
print("\n".join(frames))
PY

# 2. Each frame becomes one APRS message packet (≤67 chars of text) from AE0RM-7, in TNC2 monitor format.
n=0
: > "$WORK/packets.txt"
while IFS= read -r frame; do
  n=$((n+1))
  printf 'AE0RM-7>APRS,WIDE1-1::%-9s:%s{%d\n' "KANNAKA" "$frame" "$n" >> "$WORK/packets.txt"
done < "$WORK/frames.txt"
echo "--- packets"; cat "$WORK/packets.txt"

# 3. Modulate to AFSK 1200 baud audio (22050 Hz mono WAV).
gen_packets -r 22050 -o "$WORK/rf.wav" "$WORK/packets.txt" >/dev/null
echo "--- wav: $(stat -c %s "$WORK/rf.wav") bytes"

# 4. Replay through Direwolf → KISS → station (RX-only, NATS if the env has credentials).
RF_AUDIO=wav RF_WAV="$WORK/rf.wav" RF_LOG="$LOG" RF_STATION="${RF_STATION:-RX-SELFTEST}" RF_WAV_GRACE=4 ./deploy/run-rx.sh

# 5. Verify: the log must contain the reassembled message with both blocks.
python3 - "$LOG" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1], encoding="utf-8")]
heard = [r for r in rows if r["subject"].startswith("RF.heard.")]
rx = [r for r in rows if r["subject"].startswith("RF.rx.")]
print(f"--- heard {len(heard)} packets, {len(rx)} Singularis message(s)")
assert len(heard) >= 3, "Direwolf did not decode the synthesized packets"
assert rx, "no Singularis message reassembled"
m = rx[0]["message"]
assert m["from"] == "KANNAKA-PRIME" and m["to"] == "AE0RM" and m["messageId"] == "7Q", m
assert [b["type"] for b in m["blocks"]] == ["RESONANCE", "GR::EMIT"], m["blocks"]
assert m["experimentId"] == "rf-01"
print("SELFTEST PASS:", rx[0]["subject"], json.dumps(m["blocks"]))
PY
