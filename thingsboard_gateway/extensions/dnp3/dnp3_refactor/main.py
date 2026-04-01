"""
DNP3 Connector with multiprocessing support
- Scales to 20 outstations per worker process
- Uses .env file for configuration
- Direct ThingsBoard MQTT connection
"""

import asyncio
import json
import logging
import time
import csv
import os
from random import choice
from string import ascii_lowercase
from threading import Thread
from typing import Dict, Tuple, Optional
from time import time as get_time, sleep
from socket import gethostbyname
import multiprocessing as mp
from multiprocessing import Process
from dotenv import load_dotenv

from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
import paho.mqtt.client as mqtt

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dnp3_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DNP3Connector')


class OutstationSOEProxy(opendnp3.ISOEHandler):
    """SOE handler that collects DNP3 data points"""

    def __init__(self, logger_inst: logging.Logger, outstation_id: int, profile_file: str):
        super().__init__()
        self.data: Dict[Tuple[int, str], Tuple] = {}
        self.logger = logger_inst
        self.outstation_id = outstation_id
        self.profile = self._load_profile(profile_file)

    def _load_profile(self, profile_file: str) -> Dict:
        """Load DNP3 profile from CSV file"""
        profile = {}
        try:
            with open(profile_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    gv_str = row['GroupVariation']
                    idx = int(row['Index'])
                    field = row['Field']
                    scale = float(row.get('Scale', 1.0))

                    # Parse group and variation from string like "Group30Var6"
                    import re
                    match = re.match(r'Group(\d+)Var(\d+)', gv_str)
                    if match:
                        g = int(match.group(1))
                        v = int(match.group(2))
                        key = (g, v, idx)
                        profile[key] = {
                            'field': field,
                            'scale': scale,
                            'desc': row.get('Description', '')
                        }
            self.logger.info(f"Loaded profile {profile_file}: {len(profile)} points")
        except Exception as e:
            self.logger.error(f"Error loading profile {profile_file}: {e}")
        return profile

    def Process(self, info, values):
        """Process DNP3 SOE data"""
        try:
            type_id = opendnp3.GroupVariationToType(info.gv)
            group = type_id >> 8
            variation = type_id & 0xFF

            if values.Count() == 0:
                return

            # Route to appropriate visitor
            if isinstance(values, opendnp3.ICollectionIndexedBinary):
                self._process_binary(group, variation, values)
            elif isinstance(values, opendnp3.ICollectionIndexedAnalog):
                self._process_analog(group, variation, values)
            elif isinstance(values, opendnp3.ICollectionIndexedCounter):
                self._process_counter(group, variation, values)
            elif isinstance(values, opendnp3.ICollectionIndexedBinaryOutputStatus):
                self._process_binary_output_status(group, variation, values)
            elif isinstance(values, opendnp3.ICollectionIndexedAnalogOutputStatus):
                self._process_analog_output_status(group, variation, values)
        except Exception as e:
            self.logger.error(f"Process error: {e}")

    def _process_binary(self, group, variation, values):
        """Process binary values"""

        class BinaryVisitor(opendnp3.IVisitorIndexedBinary):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent

            def OnValue(self, indexed_value):
                try:
                    index = indexed_value.index
                    key = (group, variation, index)
                    if key in self.parent.profile:
                        field = self.parent.profile[key]['field']
                        value = indexed_value.value.value
                        event_time = getattr(indexed_value.value, 'time', None)
                        timestamp = event_time.value if event_time and hasattr(event_time, 'value') else None
                        self.parent.data[(self.parent.outstation_id, field)] = (value, timestamp)
                except:
                    pass

        visitor = BinaryVisitor(self)
        values.Foreach(visitor)

    def _process_analog(self, group, variation, values):
        """Process analog values with scaling"""

        class AnalogVisitor(opendnp3.IVisitorIndexedAnalog):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent

            def OnValue(self, indexed_value):
                try:
                    index = indexed_value.index
                    key = (group, variation, index)
                    if key in self.parent.profile:
                        field = self.parent.profile[key]['field']
                        scale = self.parent.profile[key]['scale']
                        value = indexed_value.value.value * scale if scale != 1.0 else indexed_value.value.value
                        event_time = getattr(indexed_value.value, 'time', None)
                        timestamp = event_time.value if event_time and hasattr(event_time, 'value') else None
                        self.parent.data[(self.parent.outstation_id, field)] = (value, timestamp)
                except:
                    pass

        visitor = AnalogVisitor(self)
        values.Foreach(visitor)

    def _process_counter(self, group, variation, values):
        """Process counter values"""

        class CounterVisitor(opendnp3.IVisitorIndexedCounter):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent

            def OnValue(self, indexed_value):
                try:
                    index = indexed_value.index
                    key = (group, variation, index)
                    if key in self.parent.profile:
                        field = self.parent.profile[key]['field']
                        value = indexed_value.value.value
                        event_time = getattr(indexed_value.value, 'time', None)
                        timestamp = event_time.value if event_time and hasattr(event_time, 'value') else None
                        self.parent.data[(self.parent.outstation_id, field)] = (value, timestamp)
                except:
                    pass

        visitor = CounterVisitor(self)
        values.Foreach(visitor)

    def _process_binary_output_status(self, group, variation, values):
        """Process binary output status"""

        class BinaryOutputVisitor(opendnp3.IVisitorIndexedBinaryOutputStatus):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent

            def OnValue(self, indexed_value):
                try:
                    index = indexed_value.index
                    key = (group, variation, index)
                    if key in self.parent.profile:
                        field = self.parent.profile[key]['field']
                        value = indexed_value.value.value
                        event_time = getattr(indexed_value.value, 'time', None)
                        timestamp = event_time.value if event_time and hasattr(event_time, 'value') else None
                        self.parent.data[(self.parent.outstation_id, field)] = (value, timestamp)
                except:
                    pass

        visitor = BinaryOutputVisitor(self)
        values.Foreach(visitor)

    def _process_analog_output_status(self, group, variation, values):
        """Process analog output status with scaling"""

        class AnalogOutputVisitor(opendnp3.IVisitorIndexedAnalogOutputStatus):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent

            def OnValue(self, indexed_value):
                try:
                    index = indexed_value.index
                    key = (group, variation, index)
                    if key in self.parent.profile:
                        field = self.parent.profile[key]['field']
                        scale = self.parent.profile[key]['scale']
                        value = indexed_value.value.value * scale if scale != 1.0 else indexed_value.value.value
                        event_time = getattr(indexed_value.value, 'time', None)
                        timestamp = event_time.value if event_time and hasattr(event_time, 'value') else None
                        self.parent.data[(self.parent.outstation_id, field)] = (value, timestamp)
                except:
                    pass

        visitor = AnalogOutputVisitor(self)
        values.Foreach(visitor)

    def clear_data(self):
        """Clear stored data"""
        self.data.clear()

    def Start(self):
        pass

    def End(self):
        pass


class RemoteTerminal:
    """Represents a single DNP3 outstation"""

    def __init__(self, gateway, device, master, soe_handler):
        self.gateway = gateway
        self.config = device
        self.name = device.get("deviceName")
        self.remote_ip = device.get("outstation_ip")
        self.remote_id = device.get("outstation_id")
        self.port = device.get("port", 20000)
        self.polling_interval = device.get("polling_interval", 60000)
        self.previous_poll_time = 0
        self.datatypes = ('attributes', 'telemetry')

        self.master = master
        self.soe_handler = soe_handler
        self.uplink_converter = None
        self.downlink_converter = None

        self._log = logging.getLogger(f"RTU_{self.name}")

    def __repr__(self):
        return f"RemoteTerminal(name={self.name}, outstation_id={self.remote_id})"


class DNP3Connector:
    """Main DNP3 connector with multiprocessing support"""

    def __init__(self, config_path, device_slice=None):
        with open(config_path, 'r') as f:
            full_config = json.load(f)

        self.master_ip = full_config.get('master_ip', os.getenv('MASTER_IP', '0.0.0.0'))
        self.master_id = full_config.get('master_id', int(os.getenv('MASTER_ID', 1000)))
        self.devices = device_slice or full_config['devices']

        self.profiles = {}
        self.masters = {}
        self.channels = {}
        self.soe_handlers = {}
        self.rtus = []

        # DNP3 Manager
        self.channel_log_level = opendnp3.levels.NORMAL
        self.channel_retry = asiopal.ChannelRetry().Default()
        self.listener = asiodnp3.PrintingChannelListener().Create()
        self._manager = asiodnp3.DNP3Manager(3, asiodnp3.ConsoleLogger().Create())

        # MQTT Client - Direct ThingsBoard connection
        self.mqtt_client = mqtt.Client()
        tb_host = os.getenv('TB_GATEWAY_HOST', 'localhost')
        tb_port = int(os.getenv('TB_GATEWAY_PORT', 1883))
        tb_token = os.getenv('TB_ACCESS_TOKEN', '')

        if tb_token:
            self.mqtt_client.username_pw_set(tb_token, '')

        try:
            self.mqtt_client.connect(tb_host, tb_port, 60)
            self.mqtt_client.loop_start()
            logger.info(f"Connected to ThingsBoard at {tb_host}:{tb_port}")
        except Exception as e:
            logger.error(f"Failed to connect to ThingsBoard: {e}")

        # Async event loop
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._connected = False
        self._stopped = False

        self.load_profiles()

    def load_profiles(self):
        """Load DNP3 profiles for all devices"""
        for dev in self.devices:
            profile_file = dev.get('profile')
            if profile_file and profile_file not in self.profiles:
                self.profiles[profile_file] = profile_file

    def parse_method(self, method):
        """Parse group and variation from method string (e.g., 'g30v6')"""
        import re
        match = re.match(r'g(\d+)v(\d+)', method.lower())
        if match:
            return int(match.group(1)), int(match.group(2))
        raise ValueError(f"Invalid method: {method}")

    def create_master(self, dev):
        """Create a DNP3 master for the given device"""
        name = dev['deviceName']
        ip = dev['outstation_ip']
        port = dev['port']
        outstation_id = dev['outstation_id']
        profile_file = dev['profile']

        channel = self._manager.AddTCPClient(
            f"tcpclient_{name}",
            levels=self.channel_log_level,
            retry=self.channel_retry,
            local=self.master_ip,
            host=ip,
            port=port,
            listener=self.listener
        )

        self.channels[name] = channel

        # Create SOE handler
        soe_handler = OutstationSOEProxy(logger, outstation_id, profile_file)
        self.soe_handlers[name] = soe_handler

        # Configure master
        stack_config = asiodnp3.MasterStackConfig()
        stack_config.link.LocalAddr = self.master_id
        stack_config.link.RemoteAddr = outstation_id
        stack_config.link.KeepAliveTimeout = openpal.TimeDuration().Seconds(60)
        stack_config.master.disableUnsolOnStartup = False
        stack_config.master.unsolClassMask = opendnp3.ClassField.AllEventClasses()
        stack_config.master.startupIntegrityClassMask = opendnp3.ClassField.AllClasses()
        stack_config.master.eventScanOnEventsAvailableClassMask = opendnp3.ClassField.AllEventClasses()
        stack_config.master.responseTimeout = openpal.TimeDuration().Seconds(5)
        stack_config.master.taskRetryPeriod = openpal.TimeDuration().Seconds(5)

        master = channel.AddMaster(
            f"master_{name}",
            soe_handler,
            asiodnp3.DefaultMasterApplication(),
            stack_config
        )

        master.Enable()
        logger.info(f"Master created for {name}")
        return master

    async def _run(self):
        """Main async loop"""
        while not self._stopped:
            current_time = get_time() * 1000

            for rtu in self.rtus:
                if rtu.previous_poll_time + rtu.polling_interval < current_time:
                    try:
                        master = self.masters[rtu.name]
                        soe_handler = self.soe_handlers[rtu.name]

                        # Poll attributes
                        for attr in rtu.config.get('attributes', []):
                            method = attr.get('method')
                            if method:
                                try:
                                    g, v = self.parse_method(method)
                                    gv_id = opendnp3.GroupVariationID(g, v)
                                    master.ScanRange(gv_id, 0, 18)
                                    await asyncio.sleep(0.2)
                                except Exception as e:
                                    logger.error(f"Scan error for {rtu.name}: {e}")

                        await asyncio.sleep(1)

                        # Process collected data
                        if soe_handler.data:
                            self._send_to_thingsboard(rtu, soe_handler.data.copy())
                            soe_handler.clear_data()

                        rtu.previous_poll_time = current_time

                    except Exception as e:
                        logger.error(f"Poll error for {rtu.name}: {e}")

            if self._stopped:
                break

            await asyncio.sleep(0.5)

    def _send_to_thingsboard(self, rtu, data):
        """Send collected data to ThingsBoard"""
        try:
            telemetry = {}
            attributes = {}

            for (outstation_id, field), (value, timestamp) in data.items():
                ts = timestamp if timestamp else int(get_time() * 1000)

                # Determine if attribute or telemetry
                is_attribute = False
                for attr_config in rtu.config.get('attributes', []):
                    if attr_config.get('key') == field:
                        is_attribute = True
                        break

                if is_attribute:
                    attributes[field] = value
                else:
                    if 'ts' not in telemetry:
                        telemetry['ts'] = ts
                    telemetry[field] = value

            # Send attributes
            if attributes:
                payload = {rtu.name: attributes}
                self.mqtt_client.publish("v1/gateway/attributes", json.dumps(payload))
                logger.info(f"Sent attributes for {rtu.name}: {len(attributes)} fields")

            # Send telemetry
            if telemetry:
                payload = {rtu.name: [telemetry]}
                self.mqtt_client.publish("v1/gateway/telemetry", json.dumps(payload))
                logger.info(f"Sent telemetry for {rtu.name}: {len(telemetry) - 1} fields")

        except Exception as e:
            logger.error(f"Error sending to ThingsBoard for {rtu.name}: {e}")

    def start(self):
        """Start the connector"""
        logger.info(f"Starting DNP3 Connector with {len(self.devices)} devices")

        # Create masters
        for i, dev in enumerate(self.devices):
            name = dev['deviceName']
            try:
                if i > 0:
                    sleep(1)

                master = self.create_master(dev)
                self.masters[name] = master

                # Create RTU object
                rtu = RemoteTerminal(None, dev, master, self.soe_handlers[name])
                self.rtus.append(rtu)

                logger.info(f"✓ {name}")
            except Exception as e:
                logger.error(f"✗ {name}: {e}")

        sleep(5)

        self._connected = True
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            logger.exception(e)

    def shutdown(self):
        """Shutdown the connector"""
        self._stopped = True
        if self._manager:
            self._manager.Shutdown()
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()


def worker_process(device_slice, config_path, worker_id):
    """Worker process for handling a slice of devices"""
    try:
        connector = DNP3Connector(config_path, device_slice)
        connector.start()
    except KeyboardInterrupt:
        if 'connector' in locals():
            connector.shutdown()
    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")


if __name__ == '__main__':
    config_path = os.getenv('CONFIG_PATH', 'dnp3_5_devices.json')

    print(f"\n{'=' * 60}")
    print(f"DNP3 CONNECTOR WITH MULTIPROCESSING")
    print(f"{'=' * 60}\n")

    try:
        with open(config_path, 'r') as f:
            full_config = json.load(f)

        devices = full_config['devices']
        logger.info(f"Loaded {len(devices)} devices")

        # Calculate number of processes: 1 per 20 devices
        num_devices = len(devices)
        slice_size = 20
        num_processes = min(mp.cpu_count(), (num_devices + slice_size - 1) // slice_size)

        logger.info(f"Scaling to {num_processes} processes (1 per {slice_size} devices)")

        processes = []
        for i in range(num_processes):
            start = i * slice_size
            end = min(start + slice_size, num_devices)
            device_slice = devices[start:end]

            if device_slice:
                p = Process(target=worker_process, args=(device_slice, config_path, i + 1))
                p.start()
                processes.append(p)
                logger.info(f"Worker {i + 1}: devices {start + 1}-{end} (PID: {p.pid})")

        print(f"✓ {num_processes} workers running\n")

        for p in processes:
            p.join()

    except KeyboardInterrupt:
        print("\nShutting down...")
        for p in processes:
            p.terminate()
            p.join(timeout=5)
        print("Done.\n")
    except Exception as e:
        logger.error(f"Error: {e}")