# singularis-rf — RX-only Singularis Compact station.
# Bundles Direwolf (AX.25/APRS decoder) and the rtl-sdr tools so the host needs no packages installed.
FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1 PIP_BREAK_SYSTEM_PACKAGES=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends direwolf rtl-sdr sox python3 python3-pip ca-certificates usbutils \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir "nats-py>=2.7" pytest

WORKDIR /app
COPY pyproject.toml README.md ./
COPY spec ./spec
COPY singularis_rf ./singularis_rf
COPY tests ./tests
COPY deploy/direwolf.conf deploy/direwolf-tx.conf.template deploy/run-rx.sh ./deploy/
COPY scripts ./scripts
RUN chmod +x deploy/run-rx.sh scripts/*.sh && python3 -m pytest -q tests

# Direwolf refuses to be comfortable as root, and it is right; run as an ordinary user
# who is in the plugdev group so the RTL-SDR (/dev/bus/usb) is readable.
RUN useradd -m -u 1000 -G plugdev rf && mkdir -p /data && chown -R rf:rf /data /app
USER rf

# KISS TCP from Direwolf stays inside the container; nothing is exposed by default.
ENV RF_SOURCE=kiss KISS_ADDR=127.0.0.1:8001 RF_LOG=/data/heard.jsonl
VOLUME ["/data"]
ENTRYPOINT ["./deploy/run-rx.sh"]
