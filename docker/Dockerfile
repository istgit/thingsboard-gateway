FROM --platform=$BUILDPLATFORM python:3.11-slim AS build

ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        build-essential \
        libssl-dev \
        libffi-dev \
        zlib1g-dev \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Tell cryptography not to compile Rust
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

# Copy everything first (your old working method used ADD ./ /)
ADD ./ /

# Create default config/ext folders that survive container restarts
RUN mkdir -p /default-config/config /default-config/extensions && \
    cp -r /thingsboard_gateway/config/*     /default-config/config/ && \
    cp -r /thingsboard_gateway/extensions/* /default-config/extensions/

# Upgrade pip and install Python dependencies
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir -r requirements.txt

# Create start script
RUN echo '#!/bin/sh\n\
CONF_FOLDER="/thingsboard_gateway/config"\n\
FIRSTLAUNCH="${CONF_FOLDER}/.firstlaunch"\n\
\n\
if [ ! -f "${FIRSTLAUNCH}" ]; then\n\
    echo "First launch – copying default config and extensions..."\n\
    cp -r /default-config/config/*     /thingsboard_gateway/config/\n\
    cp -r /default-config/extensions/* /thingsboard_gateway/extensions/\n\
    touch "${FIRSTLAUNCH}"\n\
    echo "# Remove this file only if you want to recreate default config!" > "${FIRSTLAUNCH}"\n\
fi\n\
\n\
# Fix DNS inside some networks\n\
echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\
\n\
exec python /thingsboard_gateway/tb_gateway.py\n\
' > /start-gateway.sh && chmod +x /start-gateway.sh

# Clean up build dependencies to keep image small
RUN apt-get remove --purge -y gcc python3-dev build-essential libssl-dev libffi-dev zlib1g-dev curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* /root/.cache/pip

# Environment variables
ENV PYTHONPATH=.
ENV PATH="/root/.local/bin:$PATH"

# Persisted folders
VOLUME ["/thingsboard_gateway/config", "/thingsboard_gateway/extensions", "/thingsboard_gateway/logs"]

# Final command
CMD ["/start-gateway.sh"]