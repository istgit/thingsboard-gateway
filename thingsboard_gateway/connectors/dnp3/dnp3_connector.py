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

from pkg_resources import non_empty_lines

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

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
        #self.channel_log_level: opendnp3.levels = opendnp3.levels.NORMAL | opendnp3.levels.ALL_COMMS
        self.channel_log_level: opendnp3.levels = opendnp3.levels.NORMAL
        self.channel_retry = asiopal.ChannelRetry().Default()
        self.listener = asiodnp3.PrintingChannelListener().Create()

        # Single DNP3Manager and SOEHandler
        self._log.debug('Creating a DNP3Manager.')

        self._manager = asiodnp3.DNP3Manager(1, asiodnp3.ConsoleLogger().Create())
        self.channels = {} # Map outstation_id to master
        self.masters = {} # Map outstation_id to channel
        self._soe_handlers = {}  # NEW: Store SOE handlers per outstation_id

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

            self._soe_handlers[outstation_id] = OutstationSOEProxy(self._log, outstation_id)

            master = channel.AddMaster(f"master_{outstation_id}",
                                       self._soe_handlers[outstation_id],
                                       asiodnp3.DefaultMasterApplication().Create(),
                                       stack_config)
            master.AddClassScan(opendnp3.ClassField(opendnp3.ClassField.ALL_CLASSES),
                                openpal.TimeDuration().Seconds(30),
                                opendnp3.TaskConfig().Default())

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


class RemoteTerminal_OLD():
    def __init__(self, gateway, device, master_id, master_ip):
        self.name = device.get("deviceName")
        self.datatypes = ('attributes', 'telemetry')
        self.config = device

        self.previous_poll_time = 0
        self.polling_interval = device.get("polling_interval",10000)

        self.master_ip = master_ip
        self.master_id = master_id
        self.remote_ip = device.get("outstation_ip")
        self.remote_id = device.get("outstation_id")
        self.timeout = device.get("timeout", 3)
        self.max_retries = device.get("max_retries", 2)
        self.retry_delay = device.get("retry_delay",1)

        self._log = init_logger(gateway, "RTU Init", "INFO", enable_remote_logging=True)

        self.uplink_converter = None
        self.downlink_converter = None

        """initialise device masters if not yet set up """
        try:
            self.master = MyMasterNew(
                master_ip=self.master_ip,
                outstation_ip=self.remote_ip,
                port=device.get("port"),
                master_id=self.master_id,
                outstation_id=self.remote_id,
                delay_polling_retry=self.retry_delay,
                timeout = self.timeout,
                num_polling_retry= self.max_retries
            )
            sleep(2)
            device.master.start()
            sleep(5)
        except Exception as e:
            self._log.exception(e)
            self._log.error("DNP3 connection initialisation failed - ip %s, id %s", self.master_ip, self.master_id )
            return

"""
        Override ISOEHandler 
        This is an interface for SequenceOfEvents (SOE) callbacks from the Master stack to the application layer.
"""
class MySOEHandler(opendnp3.ISOEHandler):
    def __init__(self):
        super(MySOEHandler, self).__init__()

    def Process(self, info, values):
        """
            Process measurement data.

        :param info: HeaderInfo
        :param values: A collection of values received from the Outstation (various data types are possible).
        """
        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalog,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval
        }
        visitor_class = visitor_class_types[type(values)]
        visitor = visitor_class()
        values.Foreach(visitor)
        for index, value in visitor.index_and_value:
            log_string = 'SOEHandler.Process {0}\theaderIndex={1}\tdata_type={2}\tindex={3}\tvalue={4}'
            print(log_string.format(info.gv, info.headerIndex, type(values).__name__, index, value))

    def Start(self):
        print('In SOEHandler.Start')

    def End(self):
        print('In SOEHandler.End')

class OutstationSOEProxy(opendnp3.ISOEHandler):
    def __init__(self, logger, outstation_id):
        super().__init__()
        self.data = {}
        self.logger = logger
        self.outstation_id = outstation_id
        self.logger.setLevel(logging.DEBUG)

    def Process(self, info, values):
        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalog,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval
        }
        visitor_class = visitor_class_types.get(type(values))
        if visitor_class:
            visitor = visitor_class()
            values.Foreach(visitor)
            for index, value in visitor.index_and_value:
                key = (self.outstation_id, info.gv, index)
                self.data[key] = value
                self.logger.debug(
                    f"SOE: Outstation {self.outstation_id}, Group {info.gv}, Index {index}, Value {value}")

    def Start(self):
        print('In SOEHandler.Start')

    def End(self):
        print('In SOEHandler.End')


class RemoteTerminal():
    def __init__(self, gateway, device, master, soe_handler,
                 listener=asiodnp3.PrintingChannelListener().Create(),
                 # Add the outstation_id and base_handler
                 master_application=asiodnp3.DefaultMasterApplication().Create(),
                 stack_config=None):
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



        self._log.debug('Configuring some scans (periodic reads).')
        # Set up a "slow scan", an infrequent integrity poll that requests events and static data for all classes.
        self.slow_scan = self.master.AddClassScan(opendnp3.ClassField().AllClasses(),
                                                  openpal.TimeDuration().Minutes(5),
                                                  opendnp3.TaskConfig().Default())
        # Set up a "fast scan", a relatively-frequent exception poll that requests events and class 1 static data.
        #self.fast_scan = self.master.AddClassScan(opendnp3.ClassField(opendnp3.ClassField.CLASS_1),
        #                                          openpal.TimeDuration().Minutes(1),
        #                                          opendnp3.TaskConfig().Default())

        def __repr__(self):
            return f"RemoteTerminal(name={self.name}. outstation_id={self.remote_id})"

        print(f"Outstation id ==== {self.remote_id}")

class MyChannelListener(asiodnp3.IChannelListener):
    """
        Override IChannelListener in this manner to implement application-specific channel behavior.
    """

    def __init__(self):
        super(AppChannelListener, self).__init__()

    def OnStateChange(self, state):
        print('In AppChannelListener.OnStateChange: state={}'.format(opendnp3.ChannelStateToString(state)))

class MyMasterApplication(opendnp3.IMasterApplication):
    def __init__(self):
        super(MyMasterApplication, self).__init__()

    # Overridden method
    def AssignClassDuringStartup(self):
        print('In MasterApplication.AssignClassDuringStartup')
        return False

    # Overridden method
    def OnClose(self):
        print('In MasterApplication.OnClose')

    # Overridden method
    def OnOpen(self):
        print('In MasterApplication.OnOpen')

    # Overridden method
    def OnReceiveIIN(self, iin):
        print('In MasterApplication.OnReceiveIIN')

    # Overridden method
    def OnTaskComplete(self, info):
        print('In MasterApplication.OnTaskComplete')

    # Overridden method
    def OnTaskStart(self, type, id):
        print('In MasterApplication.OnTaskStart')

def collection_callback(result=None):
    """
    :type result: opendnp3.CommandPointResult
    """
    print("Header: {0} | Index:  {1} | State:  {2} | Status: {3}".format(
        result.headerIndex,
        result.index,
        opendnp3.CommandPointStateToString(result.state),
        opendnp3.CommandStatusToString(result.status)
    ))

def command_callback(result=None):
    """
    :type result: opendnp3.ICommandTaskResult
    """
    print("Received command result with summary: {}".format(opendnp3.TaskCompletionToString(result.summary)))
    result.ForeachItem(collection_callback)

def restart_callback(result=opendnp3.RestartOperationResult()):
    if result.summary == opendnp3.TaskCompletion.SUCCESS:
        print("Restart success | Restart Time: {}".format(result.restartTime.GetMilliseconds()))
    else:
        print("Restart fail | Failure: {}".format(opendnp3.TaskCompletionToString(result.summary)))
