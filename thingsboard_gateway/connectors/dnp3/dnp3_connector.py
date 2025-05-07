"""
things to do here:
1. implement module installer row 35 for the DNP3 connector
2. make the callback from the DNP3 library async
3. increase the timeout for the DNP reponses - check if the parameter works

NOTES
Hard code change in master_new.py: self.stack_config.master.responseTimeout = openpal.TimeDuration().Seconds(self.timeout)
this is to increase the timeout for the comms
"""


import asyncio
import datetime
import json
import logging
import time
from random import choice
from re import search
from socket import gethostbyname
from string import ascii_lowercase
from threading import Thread
from typing import Dict
from time import time, sleep

import csv
import os
from typing import Dict, Tuple, Optional

from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
from dnp3_python.dnp3station.station_utils import MyLogger, AppChannelListener, SOEHandler
from dnp3_python.dnp3station.station_utils import parsing_gvid_to_gvcls, parsing_gv_to_mastercmdtype
from dnp3_python.dnp3station.station_utils import collection_callback, command_callback, restart_callback
from dnp3_python.dnp3station.visitors import *
from typing import Callable, Union, Dict, List, Optional, Tuple

from pydnp3.opendnp3 import IMasterApplication


# alias DbPointVal
DbPointVal = Union[float, int, bool, None]
DbStorage = Dict[opendnp3.GroupVariation, Dict[
    int, DbPointVal]]  # e.g., {GroupVariation.Group30Var6: {0: 4.8, 1: 14.1, 2: 27.2, 3: 0.0, 4: 0.0}

#from dnp3_python.dnp3station.master_new import MyMasterNew

#from pkg_resources import non_empty_lines

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService

# Try import library or install it and import
installation_required = False

from dnp3_python.dnp3station.master_new import MyMasterNew, DbStorage

RTUs = []

class DNP3Connector(Connector, Thread):
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

        self._log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self.__gateway, self.name + "_converter",
                                          self.__config.get('logLevel', 'INFO'),
                                          enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                          is_connector_logger=True, attr_name=self.name)

        self.__devices = self.__config["devices"]
        self._master_id = self.__config.get("master_id", 2)
        self._master_ip = gethostbyname(self.__config["master_ip"])
        self.channel_log_level: opendnp3.levels = opendnp3.levels.NORMAL | opendnp3.levels.ALL_COMMS
        self.channel_log_level: opendnp3.levels = opendnp3.levels.NORMAL
        self.channel_retry = asiopal.ChannelRetry().Default()
        self.listener = asiodnp3.PrintingChannelListener().Create()

        # Single DNP3Manager and SOEHandler
        self._log.debug('Creating a DNP3Manager.')

        self._manager = asiodnp3.DNP3Manager(1, asiodnp3.ConsoleLogger().Create())
        self.channels = {} # Map outstation_id to master
        self.masters = {} # Map outstation_id to channel
        self._soe_handlers = {}  # Store SOE handlers per outstation_id

        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._default_converters = {
            "uplink": "DNP3UplinkConverter",
            "downlink": "DNP3DownlinkConverter"
        }
        self.__methods = ["run", "g30v2","show"]
        self.__loop = asyncio.new_event_loop()



    def open(self):
        self.__stopped = False
        # self.__fill_converters()
        self.start()

    def run(self):

        # Create a channel and master for each outstation
        """ set up and initialise the remote terminal objects"""
        for device in self.__devices:
            outstation_ip = device.get("outstation_ip")
            print(outstation_ip)
            outstation_id = device.get("outstation_id")
            port = device.get("port", 20000)
            print(self._master_ip)
            device_name = device.get("deviceName")
            profile_file = device.get("profile", "DNP3Profile.csv")
            self._log.debug(f"Configuring outstation {outstation_id}: IP {outstation_ip}, Port {port}")



            # Create a channel
            channel = self._manager.AddTCPClient(f"tcpclient_{outstation_id}",
                                                 levels=self.channel_log_level,
                                                 retry=self.channel_retry,
                                                 local=self._master_ip,
                                                 host=outstation_ip,
                                                 port=port,
                                                listener=self.listener)
            self.channels[outstation_id] = channel

            #Create a master for this channel
            stack_config = asiodnp3.MasterStackConfig()
            stack_config.link.LocalAddr = self._master_id
            stack_config.link.RemoteAddr = outstation_id

            self._soe_handlers[outstation_id] = OutstationSOEProxy(self._log, outstation_id, profile_file)

            master = channel.AddMaster(f"master_{outstation_id}",
                                       self._soe_handlers[outstation_id],
                                       asiodnp3.DefaultMasterApplication().Create(),
                                       stack_config)

            self._log.debug('Enabling the master. At this point, traffic will start to flow between the Master and Outstations.')
            master.Enable()
            self.masters[outstation_id] = master


        for device in self.__devices:
            outstation_id = device.get("outstation_id")
            newRTU = RemoteTerminal(self.__gateway, device, self.masters[outstation_id], self._soe_handlers[outstation_id])
            if newRTU is not False:
                RTUs.append(newRTU)

        print("This is the RTUs:", RTUs)
        """ set the converters for the RTUs"""
        self.__fill_converters()

        self._connected = True
        try:
            self.__loop.run_until_complete(self._run())
            #self._run()
        except Exception as e:
            self._log.exception(e)

    def close(self):
        self.__stopped = True
        self._connected = False

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

    def collect_statistic_and_send(self, connector_name, connector_id, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, connector_id, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    async def _run(self):
        while not self.__stopped:
            current_time = time() * 1000
            for device in RTUs:
                """continue with polling and listening"""
                try:
                    if device.previous_poll_time + device.polling_interval < current_time:
                        await self.__process_data(device)
                        device.previous_poll_time = current_time
                except Exception as e:
                    self._log.exception(e)

            if self.__stopped:
                break
            else:
                sleep(.2)

    async def __process_data(self, device):
        device_responses = {}

        for datatype in device.datatypes:
            """cycle through all the datatype instances for telemetry and attributes"""
            for datatype_config in device.config[datatype]:
                try:
                    method = datatype_config.get("method")
                    if method is None:
                        self._log.error("Method not found in configuration: %r", datatype_config)
                        continue
                    else:
                        method = method.lower()
                    if method not in self.__methods:
                        self._log.error("Unknown method: %s, configuration is: %r", method, datatype_config)

                    response = await self.__process_methods(method, device, datatype_config)
                    device_responses[datatype_config['key']] = response
                    # print(">>>>>:",device.name,response)

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    StatisticsService.count_connector_bytes(self.name, response,
                                                            stat_parameter_name='connectorBytesReceived')
                except Exception as e:
                    self._log.error("exception on method to device \"%s\" with ip: \"%s\"", device.name, device.remote_ip)
                    self._log.exception(e)

        if device_responses:
            converted_data: ConvertedData = device.uplink_converter.convert(device, device_responses)
            print("CONVERTED DATA: ",converted_data)

            if (converted_data is not None and
                    (converted_data.attributes_datapoints_count > 0 or
                     converted_data.telemetry_datapoints_count > 0)):
                self.collect_statistic_and_send(self.get_name(), self.get_id(), converted_data)


    async def __process_methods(self, method, device, datatype_config):
        response = None
        if method == "run":
            #response = device.master.send_scan_all_request()
            #response = device.master.get_db_by_group_variation(group=1, variation=2)
            response = {k: v for k, v in self._soe_handlers[device.remote_id].data.items() if k[0] == device.remote_id}
            print("FROM PROCESSMETHODS:", response)
            pass
        elif method == "g30v2":
            #response = device.get_db_by_group_variation(group=30, variation=2)
            pass
        elif method == "show":
            #response = device.master.soe_handler.db
            pass
        else:
            self._log.error("Method \"%s\" - Not found", str(method))
        return response

    def __fill_converters(self):
        try:
            for device in RTUs:
                device.uplink_converter = TBModuleLoader.import_module("dnp3", device.config.get('converter',
                                                                                             self._default_converters[
                                                                                                 "uplink"]))(device,
                                                                                                             self._converter_log)
                device.downlink_converter = TBModuleLoader.import_module("dnp3", device.config.get('converter',
                                                                                               self._default_converters[
                                                                                                   "downlink"]))(device)
        except Exception as e:
            self._log.exception(e)

    @staticmethod
    def __get_common_parameters(device):
        return {"outstation_ip": gethostbyname(device["outstation_ip"]),
                "outstation_id": device.get("outstation_id",1),
                "port": device.get("port", 20000),
                "polling_interval": device.get("polling_interval", 5000)
                }

    def on_attributes_update(self, content):
        try:
            for device in self.__devices:
                if content["device"] == device["deviceName"]:
                    for attribute_request_config in device["attributeUpdateRequests"]:
                        for attribute, value in content["data"]:
                            if search(attribute, attribute_request_config["attributeFilter"]):
                                common_parameters = self.__get_common_parameters(device)
                                result = self.__process_methods(attribute_request_config["method"], common_parameters,
                                                                {**attribute_request_config, "value": value})
                                self._log.debug(
                                    "Received attribute update request for device \"%s\" "
                                    "with attribute \"%s\" and value \"%s\"",
                                    content["device"],
                                    attribute)
                                self._log.debug(result)
                                self._log.debug(content)
        except Exception as e:
            self._log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            for device in self.__devices:
                if content["device"] == device["deviceName"]:
                    for rpc_request_config in device["serverSideRpcRequests"]:
                        if search(content["data"]["method"], rpc_request_config["requestFilter"]):
                            common_parameters = self.__get_common_parameters(device)
                            result = self.__process_methods(rpc_request_config["method"], common_parameters,
                                                            {**rpc_request_config, "value": content["data"]["params"]})
                            self._log.debug("Received RPC request for device \"%s\" with command \"%s\" and value \"%s\"",
                                      content["device"],
                                      content["data"]["method"])
                            self._log.debug(result)
                            self._log.debug(content)
                            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                                          content=result)
        except Exception as e:
            self._log.exception(e)
            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"], success_sent=False)


"""
        Override ISOEHandler 
        This is an interface for SequenceOfEvents (SOE) callbacks from the Master stack to the application layer.
"""


class OutstationSOEProxy(opendnp3.ISOEHandler):
    def __init__(self, logger: logging.Logger, outstation_id: int, profile_file: str,
                 profile_dir: str = "/home/enmac/PycharmProjects/thingsboard-gateway/thingsboard_gateway/connectors/dnp3"):
        """
        Initialize SOE handler with profile mapping.

        Args:
            logger: Logger instance for debugging.
            outstation_id: DNP3 outstation ID.
            profile_file: Name of the profile CSV file (e.g., 'dnp3profile.csv').
            profile_dir: Directory containing profile files.
        """
        super().__init__()
        self.data: Dict[
            Tuple[int, str], Tuple[any, Optional[int]]] = {}  # {(outstation_id, field): (value, event_time)}
        self.logger = logger
        self.outstation_id = outstation_id
        self.logger.setLevel(logging.DEBUG)
        self.profile = self._load_profile(os.path.join(profile_dir, profile_file))

    def _load_profile(self, profile_path: str) -> Dict[Tuple[opendnp3.GroupVariation, int], str]:
        """
        Load DNP3 profile from CSV file.

        Args:
            profile_path: Path to the CSV file.

        Returns:
            Dict mapping (GroupVariation, Index) to Field name.
        """
        profile = {}
        try:
            if not os.path.exists(profile_path):
                self.logger.error(f"Profile file {profile_path} not found")
                return profile

            with open(profile_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    group_variation_str = row['GroupVariation']
                    index = int(row['Index'])
                    field = row['Field']

                    # Convert GroupVariation string (e.g., 'Group1Var1') to opendnp3.GroupVariation
                    try:
                        group_variation = getattr(opendnp3.GroupVariation, group_variation_str)
                    except AttributeError:
                        self.logger.warning(f"Invalid GroupVariation {group_variation_str} in profile")
                        continue

                    profile[(group_variation, index)] = field
                    self.logger.debug(f"Loaded profile mapping: {group_variation}, {index} → {field}")
        except Exception as e:
            self.logger.error(f"Error loading profile {profile_path}: {str(e)}")
        return profile

    def Process(self, info, values):

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
        if visitor_class:
            visitor = visitor_class()
            values.Foreach(visitor)
            for item in visitor.index_and_value:
                index = item[0]
                value = item[1]
                event_time = item[2] if len(item) > 2 else 0

                # Map to field name using profile
                field = self.profile.get((info.gv, index), f"Unknown_{info.gv}_{index}")

                # Store in self.data with field name
                key = (self.outstation_id, field)
                self.data[key] = value, event_time
                self.logger.debug(
                    f"SOE: Outstation {self.outstation_id}, Group {info.gv}, Index {index}, Field {field} ,Value {value}, Time {event_time}")



    def Start(self):
        print('In SOEHandler.Start')

    def End(self):
        print('In SOEHandler.End')


class RemoteTerminal():
    def __init__(self, gateway, device, master, soe_handler):

        self.gateway = gateway
        self.config = device
        self.master_log_level:int=15
        self.name = self.config.get("deviceName")
        self.port:int = self.config.get("port", 20000)
        self.remote_ip = self.config.get("outstation_ip")
        self.remote_id:int = self.config.get("outstation_id")
        self.timeout:int = self.config.get("timeout", 3)
        self.max_retries:int = self.config.get("max_retries", 2)
        self.retry_delay:int = self.config.get("retry_delay",1)
        self.stale_if_longer_than:float = 2  # in seconds
        self.datatypes = ('attributes', 'telemetry')
        self.previous_poll_time = 0
        self.polling_interval:float = self.config.get("polling_interval",10000)
        self._log = init_logger(gateway, f"RTU_{self.name}", "DEBUG", enable_remote_logging=True)
        self.data = {}
        self.master = master
        self.soe_handler = soe_handler
        self.uplink_converter = None
        self.downlink_converter = None
        self.profile = self.config.get("profile")

        polling_int = int(self.polling_interval)

        self._log.debug('Configuring some scans (periodic reads).')

        # Set up a "slow scan", an infrequent integrity poll that requests events and static data for all classes.
        self.slow_scan = self.master.AddClassScan(opendnp3.ClassField().AllClasses(),
                                                  openpal.TimeDuration().Milliseconds(polling_int),
                                                  opendnp3.TaskConfig().Default())

        # Set up a "fast scan", a relatively-frequent exception poll that requests events and class 1 static data.
        #self.fast_scan = self.master.AddClassScan(opendnp3.ClassField(opendnp3.ClassField.CLASS_1),
        #                                          openpal.TimeDuration().Minutes(1),
        #                                          opendnp3.TaskConfig().Default())

        def __repr__(self):
            return f"RemoteTerminal(name={self.name}. outstation_id={self.remote_id})"

        print(f"Outstation id ==== {self.remote_id}")


