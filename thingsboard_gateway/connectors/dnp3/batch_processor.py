# batch_processor.py
import json
import logging
import os
import sys
import time
from dotenv import load_dotenv
from socket import gethostbyname

import csv
from typing import Dict, Tuple, Optional

from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
from dnp3_python.dnp3station.station_utils import MyLogger, AppChannelListener, SOEHandler
from dnp3_python.dnp3station.station_utils import parsing_gvid_to_gvcls, parsing_gv_to_mastercmdtype
from dnp3_python.dnp3station.station_utils import collection_callback, command_callback, restart_callback
from dnp3_python.dnp3station.visitors import *
from typing import Callable, Union, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

# Your imports for converters, etc. (copy from main code)
from thingsboard_gateway.connectors.dnp3.dnp3_uplink_converter import DNP3UplinkConverter  # Adjust path
# ... other necessary imports

load_dotenv()

PROFILE_DIR = os.environ.get('PROFILE_DIR', '/default/path')

# Copy your OutstationSOEProxy, RemoteTerminal classes here (from main code)

def main(batch_json, master_ip, master_id):
    batch = json.loads(batch_json)
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(f"Batch_{os.getpid()}")
    logger.info(f"Starting batch process {os.getpid()} with {len(batch)} devices")

    manager = asiodnp3.DNP3Manager(1, asiodnp3.ConsoleLogger().Create())
    channel_log_level = opendnp3.levels.NORMAL
    channel_retry = asiopal.ChannelRetry().Default()
    listener = asiodnp3.PrintingChannelListener().Create()

    channels = {}
    masters = {}
    soe_handlers = {}
    rtus = []

    for device in batch:
        outstation_ip = device.get("outstation_ip")
        outstation_id = device.get("outstation_id")
        port = device.get("port", 20000)
        profile_file = device.get("profile", "DNP3Profile1.csv")
        logger.debug(f"Configuring outstation {outstation_id}: IP {outstation_ip}, Port {port}")

        channel = manager.AddTCPClient(f"tcpclient_{outstation_id}",
                                       channel_log_level,
                                       channel_retry,
                                       outstation_ip,  # Fixed: remote host
                                       "0.0.0.0",     # Local bind
                                       port,
                                       listener)
        channels[outstation_id] = channel
        time.sleep(0.2)

        stack_config = asiodnp3.MasterStackConfig()
        stack_config.link.LocalAddr = master_id
        stack_config.link.RemoteAddr = outstation_id
        soe_handlers[outstation_id] = OutstationSOEProxy(logger, outstation_id, profile_file)

        master = channel.AddMaster(f"master_{outstation_id}",
                                   soe_handlers[outstation_id],
                                   asiodnp3.DefaultMasterApplication().Create(),
                                   stack_config)
        masters[outstation_id] = master
        master.Enable()
        logger.info(f"Master enabled for outstation {outstation_id}")

        rtu = RemoteTerminal(None, device, master, soe_handlers[outstation_id])
        if None:  # gateway=None fix
            rtu._log = logging.getLogger(f"RTU_{rtu.name}")
            rtu._log.setLevel(logging.DEBUG)
        rtus.append(rtu)

    client = create_mqtt_client()  # Copy your function
    if client is None:
        logger.error("MQTT failed")
        sys.exit(1)

    while True:  # No stop_event; run forever (main will kill if needed)
        for rtu in rtus:
            if time.time() - rtu.previous_poll_time > rtu.polling_interval:
                logger.info(f"Polling RTU {rtu.name}")
                poll_rtu(rtu)
                rtu.previous_poll_time = time.time()
                data = rtu.soe_handler.data
                converted_data = rtu.uplink_converter.convert(rtu, data)

                telemetry = {rtu.name: converted_data.telemetry}
                attributes = {rtu.name: converted_data.attributes}
                if telemetry[rtu.name]:
                    client.publish("v1/gateway/telemetry", json.dumps(telemetry))
                    logger.info(f"Sent telemetry for {rtu.name}")
                if attributes[rtu.name]:
                    client.publish("v1/gateway/attributes", json.dumps(attributes))
                    logger.info(f"Sent attributes for {rtu.name}")

                rtu.soe_handler.clear_data()
        time.sleep(1)  # Adjusted for less CPU

    manager.Shutdown()

if __name__ == "__main__":
    batch_json = sys.argv[1]
    master_ip = sys.argv[2]
    master_id = int(sys.argv[3])
    main(batch_json, master_ip, master_id)