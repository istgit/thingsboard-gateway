FROM --platform=$BUILDPLATFORM python:3.9-slim AS build
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log
 
# Update package lists and install necessary dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
   gcc python3-dev build-essential libssl-dev libffi-dev zlib1g-dev \
   && apt-get install python3-grpcio -y && rm -rf /var/lib/apt/lists/*
 
# Copy the application code and configuration
ADD ./ /

# Set up the start script
RUN echo '#Main start script\n\
CONF_FOLDER="./thingsboard_gateway/config"\n\
firstlaunch=${CONF_FOLDER}/.firstlaunch\n\
\n\
if [ ! -f ${firstlaunch} ]; then\n\
   cp -r /default-config/config/* /thingsboard_gateway/config/\n\
   cp -r /default-config/connectors/dnp3/* /thingsboard_gateway/connectors/dnp3/ 2>/dev/null || echo "No DNP3 connectors to copy"\n\
   cp -r /default-config/gateway/* /thingsboard_gateway/gateway/ 2>/dev/null || echo "No gateway files to copy"\n\
   cp -r /default-config/tb_utility/* /thingsboard_gateway/tb_utility/ 2>/dev/null || echo "No utility files to copy"\n\
   touch ${firstlaunch}\n\
   echo "#Remove this file only if you want to recreate default config files! This will overwrite existing files" > ${firstlaunch}\n\
fi\n\
echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\
\n\
python ./thingsboard_gateway/tb_gateway.py\n\
'\
>> start-gateway.sh  && chmod  +x start-gateway.sh
 
# Set environment variables
ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONPATH=.
ENV configs  /thingsboard_gateway/config
ENV extensions  /thingsboard_gateway/extensions
ENV logs  /thingsboard_gateway/logs
ENV connectors  /thingsboard_gateway/connectors/dnp3
ENV CRYPTOGRAPHY_DONT_BUILD_RUST  1
ENV utility /thingsboard_gateway/tb_utility
ENV gateway /thingsboard_gateway/gateway
 
# Create default-config backup directory and copy initial configs
RUN mkdir -p /default-config/config /default-config/connectors/dnp3 /default-config/gateway /default-config/tb_utility && \
   cp -r /thingsboard_gateway/config/* /default-config/config/ 2>/dev/null || echo "No config files" && \
   cp -r /thingsboard_gateway/connectors/dnp3/* /default-config/connectors/dnp3/ 2>/dev/null || echo "No DNP3 connector files" && \
   cp -r /thingsboard_gateway/gateway/* /default-config/gateway/ 2>/dev/null || echo "No gateway files" && \
   cp -r /thingsboard_gateway/tb_utility/* /default-config/tb_utility/ 2>/dev/null || echo "No utility files"
 
# Install Python packages from main requirements.txt
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
   python3 -m pip install --no-cache-dir --upgrade setuptools &&  \
   python3 -m pip install --no-cache-dir importlib_metadata && \
   python3 -m pip install --no-cache-dir -r requirements.txt
 
# Install DNP3 connector specific dependencies
RUN python3 -m pip install --no-cache-dir rich python-dotenv

# CRITICAL FIX: Only mount config, logs, and data as volumes
# Do NOT mount extensions - they should be built into the image!
VOLUME ["${configs}", "${logs}"]
 
# Verify DNP3 connector files are present in the image
RUN echo "=== Verifying DNP3 Connector Files ===" && \
    ls -la /thingsboard_gateway/extensions/ && \
    ls -la /thingsboard_gateway/extensions/dnp3/ && \
    echo "=== Extensions verified ==="
 
# Set the container command
CMD [ "/bin/sh",  "./start-gateway.sh" ]