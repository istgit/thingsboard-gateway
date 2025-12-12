FROM --platform=$BUILDPLATFORM python:3.11-slim AS build
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log

COPY ./thingsboard_gateway /thingsboard_gateway
COPY . .

ENV PATH="/root/.cargo/bin:/root/.local/bin:$PATH" \
    PYTHONPATH="." \
    logs="/thingsboard_gateway/logs" \
    CRYPTOGRAPHY_DONT_BUILD_RUST=1

RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    zlib1g-dev \
    python3-grpcio \
    curl pkg-config libssl-dev \
    python3-grpcio -y && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* && \
  echo '#!/bin/sh\n\

echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\

python /thingsboard_gateway/tb_gateway.py' > /start-gateway.sh && chmod +x /start-gateway.sh && \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir cryptography && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    apt-get remove --purge -y gcc python3-dev build-essential libssl-dev libffi-dev zlib1g-dev pkg-config && \
    apt-get autoremove -y

VOLUME ["${logs}"]

CMD [ "/bin/sh", "/start-gateway.sh" ]

