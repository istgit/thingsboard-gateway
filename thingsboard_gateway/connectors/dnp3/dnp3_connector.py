#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import csv
import logging
import multiprocessing
import os
import time
from random import choice
from socket import gethostbyname
from string import ascii_lowercase
from threading import Thread
from typing import Dict, Tuple, Optional

from pydnp3 import opendnp3, openpal, asiopal, asiodnp3

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

from thingsboard_gateway.connectors.dnp3.visitorindexedbinary import (
    VisitorIndexedAnalogOutputStatus,
    VisitorIndexedBinary,
    VisitorIndexedBinaryOutputStatus,
    VisitorIndexedCounter,
    VisitorIndexedDoubleBitBinary,
    VisitorIndexedFrozenCounter,
    VisitorIndexedTimeAndInterval,
    VisitorIndexedAnalogTime
)

RTUs = []


class OutstationSOEProxy(opendnp3.ISOEHandler):
    """
    Proxy for handling Sequence of Events (SOE) from DNP3 outstations.
    Maps DNP3 GroupVariation/Index to human-readable field names for ThingsBoard.
    """

    def __init__(self, logger, outstation_id: int, profile_file: str, profile_dir: str):
        """
        Initialize SOE handler with profile mapping.

        Args:
            logger: Logger instance for debugging.
            outstation_id: DNP3 outstation ID.
            profile_file: Name of the profile CSV file (e.g., 'dnp3profile.csv').
            profile_dir: Directory containing profile files.
        """
        super().__init__()
        # Store data with HUMAN-READABLE keys for ThingsBoard
        self.static_data: Dict[Tuple[int, str], Tuple[any, Optional[int]]] = {}
        self.event_data: Dict[Tuple[int, str], Tuple[any, Optional[int]]] = {}
        self.logger = logger
        self.outstation_id = outstation_id
        self.logger.setLevel(logging.DEBUG)

        if profile_dir.endswith(profile_file):
            profile_path = profile_dir
        else:
            profile_path = os.path.join(profile_dir, profile_file)

        self.profile = self._load_profile(profile_path)

        # Event GroupVariations - these trigger unsolicited responses
        self.event_gvs = {
            # Binary Input Events
            opendnp3.GroupVariation.Group2Var1,
            opendnp3.GroupVariation.Group2Var2,
            opendnp3.GroupVariation.Group2Var3,
            # Double-Bit Binary Events
            opendnp3.GroupVariation.Group4Var1,
            opendnp3.GroupVariation.Group4Var2,
            opendnp3.GroupVariation.Group4Var3,
            # Counter Events
            opendnp3.GroupVariation.Group22Var1,
            opendnp3.GroupVariation.Group22Var2,
            opendnp3.GroupVariation.Group22Var5,
            opendnp3.GroupVariation.Group22Var6,
            # Analog Input Events
            opendnp3.GroupVariation.Group32Var1,
            opendnp3.GroupVariation.Group32Var2,
            opendnp3.GroupVariation.Group32Var3,
            opendnp3.GroupVariation.Group32Var4,
            opendnp3.GroupVariation.Group32Var5,
            opendnp3.GroupVariation.Group32Var6,
            opendnp3.GroupVariation.Group32Var7,
            opendnp3.GroupVariation.Group32Var8,

            #Analog Output Status
            opendnp3.GroupVariation.Group40Var1,
        }

    def _load_profile(self, profile_path: str) -> Dict[Tuple[opendnp3.GroupVariation, int], str]:
        """
        Load DNP3 profile from CSV file.
        Maps (GroupVariation, Index) -> Field Name
        """
        profile = {}
        try:
            if not os.path.exists(profile_path):
                self.logger.error(f"Profile file {profile_path} not found")
                return profile

            self.logger.info(f"Loading profile from: {profile_path}")

            with open(profile_path, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0

                for row in reader:
                    row_count += 1

                    # Check if required columns exist
                    if 'GroupVariation' not in row or 'Index' not in row or 'Field' not in row:
                        self.logger.error(
                            f"Row {row_count} missing required columns. "
                            f"Expected: GroupVariation, Index, Field. Got: {list(row.keys())}"
                        )
                        continue

                    group_variation_str = row['GroupVariation'].strip()
                    index = int(row['Index'])
                    field = row['Field'].strip()

                    try:
                        group_variation = getattr(opendnp3.GroupVariation, group_variation_str)
                        profile[(group_variation, index)] = field
                        self.logger.debug(f"Loaded: {group_variation_str}[{index}] -> '{field}'")
                    except AttributeError:
                        self.logger.warning(
                            f"Invalid GroupVariation '{group_variation_str}' at row {row_count}. "
                            f"Must match opendnp3.GroupVariation enum (e.g., 'Group1Var2')"
                        )
                        continue

                self.logger.info(
                    f"Profile loaded: {len(profile)} mappings from {row_count} rows "
                    f"for outstation {self.outstation_id}"
                )

        except Exception as e:
            self.logger.error(f"Error loading profile {profile_path}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

        return profile

    def Process(self, info, values):
        """
        Process incoming DNP3 data and map to human-readable field names.
        """
        # Get the GroupVariation string for logging
        gv_str = str(info.gv).split('.')[-1]
        is_event_gv = info.gv in self.event_gvs

        self.logger.debug("=" * 60)
        self.logger.debug(f"SOE Process called for Outstation {self.outstation_id}")
        self.logger.debug(f"GroupVariation: {gv_str}")
        self.logger.debug(f"Is Event GV: {is_event_gv}")
        self.logger.debug("=" * 60)

        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalogTime
        }
        visitor_class = visitor_class_types.get(type(values))

        if not visitor_class:
            self.logger.warning(f"No visitor found for type: {type(values)}")
            return

        visitor = visitor_class()
        values.Foreach(visitor)

        self.logger.debug(f"Visitor extracted {len(visitor.index_and_value)} values")

        for item in visitor.index_and_value:
            index = item[0]
            value = item[1]
            event_time = item[2] if len(item) > 2 else 0

            # Map to human-readable field name using profile
            field = self.profile.get((info.gv, index))

            if field is None:
                # Fallback: try string-based lookup for flexibility
                for (profile_gv, profile_idx), profile_field in self.profile.items():
                    profile_gv_str = str(profile_gv).split('.')[-1]
                    if profile_gv_str == gv_str and profile_idx == index:
                        field = profile_field
                        self.logger.debug(f"Found field via string match: {gv_str}[{index}] -> {field}")
                        break

            if field is None:
                # Skip unmapped fields
                self.logger.warning(
                    f"⚠ UNMAPPED POINT: {gv_str}[{index}] = {value}"
                )
                self.logger.warning(
                    f"Add to profile CSV: {gv_str},{index},Description,Your Field Name"
                )
                continue

            # Store with human-readable field name as key
            key = (self.outstation_id, field)

            # Determine if this is event data or static data
            is_event = info.gv in self.event_gvs

            if is_event:
                self.event_data[key] = value, event_time
                self.logger.info("*" * 60)
                self.logger.info(f"⚡ EVENT DATA STORED")
                self.logger.info(f"   Outstation: {self.outstation_id}")
                self.logger.info(f"   GroupVariation: {gv_str}[{index}]")
                self.logger.info(f"   Field: '{field}'")
                self.logger.info(f"   Value: {value}")
                self.logger.info(f"   Timestamp: {event_time}")
                self.logger.info("*" * 60)
            else:
                self.static_data[key] = value, event_time
                self.logger.debug(
                    f"STATIC: Outstation {self.outstation_id}, {gv_str}[{index}] -> '{field}' = {value}"
                )

    def clear_data(self):
        """Clear stored data to prevent stale entries."""
        self.static_data.clear()
        self.event_data.clear()
        self.logger.debug(f"Cleared data for outstation {self.outstation_id}")

    def Start(self):
        self.logger.debug('In SOEHandler.Start')

    def End(self):
        self.logger.debug('In SOEHandler.End')


class RemoteTerminal:
    def __init__(self, gateway, device, master, soe_handler):
        self.gateway = gateway
        self.config = device
        self.name = self.config.get("deviceName")
        self.port = self.config.get("port", 20000)
        self.remote_ip = self.config.get("outstation_ip")
        self.remote_id = self.config.get("outstation_id")
        self.timeout = self.config.get("timeout", 6)
        self.datatypes = ('attributes', 'telemetry')
        self.previous_poll_time = 0
        self.polling_interval = self.config.get("polling_interval", 10000) / 1000.0

        if gateway is None:
            self._log = logging.getLogger(f"RTU_{self.name}")
            self._log.setLevel(logging.DEBUG)
        else:
            self._log = init_logger(gateway, f"RTU_{self.name}", "DEBUG", enable_remote_logging=True)

        self.master = master
        self.soe_handler = soe_handler
        self.profile = self.config.get("profile")
        self.uplink_converter = None
        self.downlink_converter = None

    def __repr__(self):
        return f"RemoteTerminal(name={self.name}, outstation_id={self.remote_id})"


def process_batch(batch, master_ip, master_id, stop_event, gateway_queue):
    """Process a batch of outstations with gateway integration"""
    log_level = logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - [%(processName)s] - %(levelname)s - %(message)s")
    logger = logging.getLogger(f"Batch_{os.getpid()}")
    logger.info(f"=== Starting batch with {len(batch)} devices ===")

    manager = asiodnp3.DNP3Manager(1, asiodnp3.ConsoleLogger().Create())
    channel_log_level = opendnp3.levels.NORMAL
    channel_retry = asiopal.ChannelRetry().Default()
    listener = asiodnp3.PrintingChannelListener().Create()

    device_data = {}
    profile_dir = batch[0].get('profile_dir',
                               '/thingsboard_gateway/connectors/dnp3')

    # === PHASE 1: Initialize all devices ===
    logger.info("PHASE 1: Initializing devices...")
    for i, device in enumerate(batch):
        outstation_ip = device.get("outstation_ip")
        outstation_id = device.get("outstation_id")
        port = device.get("port", 20000)
        device_name = device.get("deviceName")
        profile_file = device.get("profile", "DNP3Profile1.csv")
        polling_interval_sec = int(device.get('polling_interval', 60000) // 1000)

        logger.info(f"[{i + 1}/{len(batch)}] Setting up {device_name} (ID {outstation_id})")

        try:
            channel = manager.AddTCPClient(
                f"tcpclient_{outstation_id}",
                channel_log_level,
                channel_retry,
                outstation_ip,
                "0.0.0.0",
                port,
                listener
            )
            time.sleep(0.1)

            stack_config = asiodnp3.MasterStackConfig()
            stack_config.link.LocalAddr = master_id
            stack_config.link.RemoteAddr = outstation_id

            soe_handler = OutstationSOEProxy(logger, outstation_id, profile_file, profile_dir)

            master = channel.AddMaster(
                f"master_{outstation_id}",
                soe_handler,
                asiodnp3.DefaultMasterApplication().Create(),
                stack_config
            )

            master.AddClassScan(
                opendnp3.ClassField().AllClasses(),
                openpal.TimeDuration().Seconds(polling_interval_sec),
                opendnp3.TaskConfig().Default()
            )

            master.Enable()

            # Create RTU object
            rtu = RemoteTerminal(None, device, master, soe_handler)

            device_data[outstation_id] = {
                'name': device_name,
                'master': master,
                'soe_handler': soe_handler,
                'config': device,
                'rtu': rtu,
                'last_poll': 0,
                'last_event_publish': 0,
                'polling_interval': polling_interval_sec,
                'event_check_interval': 1.0
            }

            logger.info(f"✓ {device_name} initialized successfully")
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"✗ Failed to initialize {device_name}: {str(e)}")
            continue

    logger.info(f"PHASE 1 Complete: {len(device_data)}/{len(batch)} devices initialized")

    if len(device_data) == 0:
        logger.error("No devices initialized successfully. Exiting.")
        manager.Shutdown()
        return

    time.sleep(5)

    # === PHASE 2: Load converters ===
    logger.info("PHASE 2: Loading converters...")
    for outstation_id, dev_info in device_data.items():
        rtu = dev_info['rtu']
        try:
            rtu.uplink_converter = TBModuleLoader.import_module(
                "dnp3",
                rtu.config.get('converter', 'DNP3UplinkConverter')
            )(rtu, logger)

            rtu.downlink_converter = TBModuleLoader.import_module(
                "dnp3",
                rtu.config.get('downlink_converter', 'DNP3DownlinkConverter')
            )(rtu.config)

            logger.info(f"✓ Converters loaded for {dev_info['name']}")
        except Exception as e:
            logger.error(f"✗ Failed to load converters for {dev_info['name']}: {str(e)}")

    # === PHASE 3: Main polling loop ===
    logger.info("PHASE 3: Starting polling loop with event monitoring...")
    poll_count = 0
    event_count = 0
    last_progress_log = 0

    try:
        while not stop_event.is_set():
            current_time = time.time()

            for outstation_id, dev_info in device_data.items():
                device_name = dev_info['name']
                soe_handler = dev_info['soe_handler']
                rtu = dev_info['rtu']

                # Check for new event data (unsolicited responses)
                if current_time - dev_info['last_event_publish'] >= dev_info['event_check_interval']:
                    raw_event_data = soe_handler.event_data.copy()

                    if raw_event_data:
                        logger.info("=" * 80)
                        logger.info(f"🔔 UNSOLICITED EVENT DETECTED for {device_name}")
                        logger.info("=" * 80)
                        logger.info(f"   Device: {device_name} (Outstation {outstation_id})")
                        logger.info(f"   Event data points: {len(raw_event_data)}")
                        logger.info("-" * 80)

                        # Log what changed with details
                        for (oid, field), (value, ts) in raw_event_data.items():
                            logger.info(f"   Field: '{field}'")
                            logger.info(f"   Value: {value}")
                            logger.info(f"   Timestamp: {ts}")
                            logger.info("-" * 40)

                        try:
                            # Convert ONLY event data
                            logger.info("   Converting event data to ThingsBoard format...")
                            converted_data = rtu.uplink_converter.convert(rtu, raw_event_data)

                            if (converted_data is not None and
                                    (converted_data.attributes_datapoints_count > 0 or
                                     converted_data.telemetry_datapoints_count > 0)):

                                logger.info(f"   ✓ Converted: {converted_data.telemetry_datapoints_count} telemetry, "
                                            f"{converted_data.attributes_datapoints_count} attributes")

                                # Send to gateway queue with event flag
                                gateway_queue.put({
                                    'type': 'event',
                                    'device_name': device_name,
                                    'converted_data': converted_data
                                })

                                event_count += 1
                                # Clear ONLY event data after sending
                                soe_handler.event_data.clear()
                                dev_info['last_event_publish'] = current_time

                                logger.info(f"   ✓ Event data queued for ThingsBoard")
                                logger.info("=" * 80)
                            else:
                                logger.warning("   ⚠ Conversion returned no data!")
                                logger.info("=" * 80)
                        except Exception as e:
                            logger.error("=" * 80)
                            logger.error(f"   ✗ Error converting event data: {str(e)}")
                            logger.error("=" * 80)
                            import traceback
                            traceback.print_exc()

                # Regular polling cycle (static data)
                if current_time - dev_info['last_poll'] >= dev_info['polling_interval']:
                    raw_static_data = soe_handler.static_data.copy()

                    if raw_static_data:
                        logger.info(f"📊 {device_name}: POLL - {len(raw_static_data)} data points")

                        try:
                            # Convert static data from regular poll
                            converted_data = rtu.uplink_converter.convert(rtu, raw_static_data)

                            if (converted_data is not None and
                                    (converted_data.attributes_datapoints_count > 0 or
                                     converted_data.telemetry_datapoints_count > 0)):
                                # Send to gateway queue
                                gateway_queue.put({
                                    'type': 'poll',
                                    'device_name': device_name,
                                    'converted_data': converted_data
                                })

                                poll_count += 1
                                # Clear static data after sending
                                soe_handler.static_data.clear()

                                logger.info(f"✓ {device_name}: Poll data sent")
                        except Exception as e:
                            logger.error(f"Error converting poll data for {device_name}: {str(e)}")

                    dev_info['last_poll'] = current_time

            # Progress logging - reduced frequency (every 5 minutes)
            if current_time - last_progress_log >= 300:
                if (poll_count + event_count) > 0:
                    logger.info(f"📈 Progress: {poll_count} polls, {event_count} events")
                    last_progress_log = current_time

            time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Batch process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in polling loop: {str(e)}", exc_info=True)
    finally:
        logger.info(f"=== Shutting down batch ({poll_count} polls, {event_count} events) ===")

        try:
            manager.Shutdown()
        except:
            pass

        logger.info("Batch process shutdown complete")


class Dnp3Connector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type

        self.__config = config
        self.__id = self.__config.get('id')
        self.name = config.get("name", 'dnp3 connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))

        self._log = init_logger(
            self.__gateway,
            self.name,
            self.__config.get('logLevel', 'INFO'),
            enable_remote_logging=self.__config.get('enableRemoteLogging', False),
            is_connector_logger=True
        )

        self._converter_log = init_logger(
            self.__gateway,
            self.name + "_converter",
            self.__config.get('logLevel', 'INFO'),
            enable_remote_logging=self.__config.get('enableRemoteLogging', False),
            is_connector_logger=True,
            attr_name=self.name
        )

        self.__devices = self.__config["devices"]
        self._master_id = self.__config.get("master_id", 2)
        self._master_ip = gethostbyname(self.__config["master_ip"])

        # Get profile directory from config
        self._profile_dir = self.__config.get(
            "profile_dir",
            "/thingsboard_gateway/connectors/dnp3"
        )

        self.statistics = {
            'MessagesReceived': 0,
            'MessagesSent': 0
        }

        self.__methods = ["run", "g30v2", "show"]
        self.stop_event = multiprocessing.Event()
        self.processes = []
        self.gateway_queue = multiprocessing.Queue()

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        time.sleep(2)

        # Add profile_dir to each device config
        for device in self.__devices:
            device['profile_dir'] = self._profile_dir

        batch_size = self.__config.get("batch_size", 20)
        devices = self.__devices
        batches = [devices[i:i + batch_size] for i in range(0, len(devices), batch_size)]

        self._log.info(f"Starting {len(batches)} batch processes with {len(devices)} total devices")

        for i, batch in enumerate(batches):
            self._log.info(f"Starting batch {i + 1}/{len(batches)} with {len(batch)} devices")
            p = multiprocessing.Process(
                target=process_batch,
                args=(batch, self._master_ip, self._master_id, self.stop_event, self.gateway_queue)
            )
            p.start()
            self.processes.append(p)

        self._connected = True

        # Main thread: process queue and send to gateway
        while not self.__stopped:
            try:
                # Check queue for data from batch processes
                if not self.gateway_queue.empty():
                    message = self.gateway_queue.get(timeout=1)

                    if message['type'] in ['event', 'poll']:
                        device_name = message['device_name']
                        converted_data = message['converted_data']
                        message_type = message['type']

                        self.collect_statistic_and_send(
                            self.get_name(),
                            self.get_id(),
                            converted_data
                        )

                        type_label = "UNSOLICITED EVENT" if message_type == 'event' else "REGULAR POLL"
                        self._log.info(
                            f"✓ [{type_label}] Data sent to ThingsBoard for {device_name}: "
                            f"{converted_data.telemetry_datapoints_count} telemetry, "
                            f"{converted_data.attributes_datapoints_count} attributes"
                        )

                else:
                    time.sleep(0.1)

            except Exception as e:
                self._log.error(f"Error processing queue: {str(e)}")
                time.sleep(1)

        self._log.info("DNP3 Connector main loop ended")

    def collect_statistic_and_send(self, connector_name, connector_id, data):
        """Send data to ThingsBoard using gateway's send_to_storage method"""
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        try:
            self.__gateway.send_to_storage(connector_name, connector_id, data)
            self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

            # Update statistics
            StatisticsService.count_connector_message(
                self.name,
                stat_parameter_name='connectorMsgsReceived'
            )
        except Exception as e:
            self._log.error(f"Failed to send data to ThingsBoard for {connector_name}: {str(e)}")

    def close(self):
        self._log.info("Closing DNP3 Connector...")
        self.__stopped = True
        self.stop_event.set()

        for p in self.processes:
            p.join(timeout=5)
            if p.is_alive():
                self._log.warning(f"Process {p.pid} did not terminate, forcing...")
                p.terminate()

        self._connected = False
        self._log.info("DNP3 Connector closed")

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def on_attributes_update(self, content):
        """Handle attribute updates from ThingsBoard"""
        try:
            self._log.debug("Attribute update received: %s", content)
        except Exception as e:
            self._log.exception(e)

    def server_side_rpc_handler(self, content):
        """Handle RPC requests from ThingsBoard"""
        try:
            self._log.debug("RPC request received: %s", content)
        except Exception as e:
            self._log.exception(e)