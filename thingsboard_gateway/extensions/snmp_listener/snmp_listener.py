#     Copyright 2024. ThingsBoard
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

import asyncio
from random import choice
from re import search
from socket import gethostbyname
from string import ascii_lowercase
from threading import Thread
from time import sleep, time

from thingsboard_gateway.connectors.connector import Connector
# from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
# from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

# Try import Pymodbus library or install it and import
installation_required = False

try:
    from puresnmp import __version__ as pymodbus_version

    if int(pymodbus_version.split('.')[0]) < 2:
        installation_required = True
except ImportError:
    installation_required = True

if installation_required:
    print("Modbus library not found - installing...")
    TBUtility.install_package("puresnmp", ">=2.0.0")

# <hb> import for snmp listener
from pysnmp.entity import engine as pysnmp_engine
from pysnmp.entity import config as pysnmp_config
from pysnmp.carrier.asyncore.dgram import udp
from pysnmp.entity.rfc3413 import ntfrcv

class SNMPListener(Connector, Thread):


    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type
        self.__config = config
        self.__id = self.__config.get('id')

        # hb - check this for correct loading of configuration information
        self.name = config.get("name", 'SNMP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self._log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'))
        self.__devices = self.__config["devices"]
        self.__device_ips = self.__config["device_ips"]
        self.__oids = self.__config["oids"]
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}

        self._default_converters = {
            "uplink": "SNMPListenerUplinkConverter",
            "downlink": "SNMPListenerDownlinkConverter"
        }
        self.__methods = ["listen"]
        self.__datatypes = ('attributes', 'telemetry')

        # add transport for the listener
        # Configure listener engine
        # use parameters for the ip address and port number below
        self.snmpEngine = pysnmp_engine.SnmpEngine()
        pysnmp_config.addTransport(
            self.snmpEngine,
            udp.domainName + (1,),
            udp.UdpTransport().openServerMode((self.__devices[0]["ip"], self.__devices[0]["port"]))
        )
        pysnmp_config.addV1System(self.snmpEngine, 'my-area', 'public')

        self.snmpNotifier = ntfrcv.NotificationReceiver(self.snmpEngine, self._callback_function)
        self.snmpEngine.transportDispatcher.jobStarted(1)

        self.__loop = asyncio.new_event_loop()

    def _callback_function(self, snmpEngine, stateReference, contextEngineId, contextName, varBinds, cbCtx):
        transportDomain, transportAddress = snmpEngine.msgAndPduDsp.getTransportInfo(stateReference)

        # hb - get the first default device
        default_device = self.__devices[0]

        # hb - message template for appending the message contents to
        converted_data = {
            "deviceName": "",
            "deviceType": "snmp",
            "attributes": [],
            "telemetry": []
        }
        # hb - get the IP address for the SOURCE
        if(transportAddress[0] in self.__device_ips):
            print("==== Incoming Trap ====")
            for name, val in varBinds:
                #print(f"IP address {transportAddress[0]}")
                #print('%s = %s' % (name.prettyPrint(), val.prettyPrint()))
                # hb - look up the OID values for the traps and messages
                lookup_name = name.prettyPrint()
                if(lookup_name in self.__oids):
                    new_name = self.__oids.get(lookup_name)
                    lookup_val = val.prettyPrint()
                    if(lookup_val in self.__oids):
                        new_val = self.__oids.get(lookup_val)
                    else:
                        new_val = lookup_val
                    response = {new_name:new_val}
                    converted_data["deviceName"] = self.__device_ips.get(transportAddress[0])
                    converted_data["attributes"].append(default_device["uplink_converter"].convert(("attributes", default_device["attributes"]), response))
                    # print("<hb> converted data after: ", converted_data)
                    # print(f'{new_name}={new_val}')

        # print("==== End of Incoming Trap ====")
        if isinstance(converted_data, dict) and (converted_data.get("attributes") or converted_data.get("telemetry")):
            self.collect_statistic_and_send(self.get_name(), self.get_id(), converted_data)

    def open(self):
        self.__stopped = False
        self.__fill_converters()
        self.start()

    def run(self):
        self._connected = True
        try:
            self.__loop.run_until_complete(self._run())
        except Exception as e:
            self._log.exception(e)

    async def _run(self):
        while not self.__stopped:
            current_time = time() * 1000
            # hb - in the case of the SNMP listener there will only be one default device to start the listener.
            # data from different devices will be separated according to the SRC IP address of the UDP packets/
            for device in self.__devices:
                try:
                    poll_interval = device.get("pollPeriod", 0)
                    if device.get("previous_poll_time", 0) + poll_interval < current_time:
                        await self.__process_data(device)
                        device["previous_poll_time"] = current_time
                except Exception as e:
                    self._log.exception(e)
            if self.__stopped:
                break
            else:
                sleep(.2)

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

    async def __process_data(self, device):
        common_parameters = self.__get_common_parameters(device)

        # hb - for the SNMP trap listener there will typically be only one type, being telemetry to have timestamps
        for datatype in self.__datatypes:
            for datatype_config in device[datatype]:
                try:
                    response = None
                    method = datatype_config.get("method")
                    # hb - for the SNMP trap listener, there will only be a LISTEN method
                    if method is None:
                        self._log.error("Method not found in configuration: %r", datatype_config)
                        continue
                    else:
                        method = method.lower()
                    if method not in self.__methods:
                        self._log.error("Unknown method: %s, configuration is: %r", method, datatype_config)
                    if method == "listen":
                        try:
                            # hb - refer to the callback function for the processing of the messages that were received.
                            self.snmpEngine.transportDispatcher.runDispatcher()
                        except:
                            self.snmpEngine.transportDispatcher.closeDispatcher()
                            raise
                    else:
                        self._log.error("Method \"%s\" - Not found", str(method))

                except Exception as e:
                    self._log.exception(e)

    def __fill_converters(self):
        try:
            # hb - there will only be the one default device for the listener.
            for device in self.__devices:
                device["uplink_converter"] = TBModuleLoader.import_module("snmp_listener", device.get('converter',
                                                                                             self._default_converters[
                                                                                                 "uplink"]))(device,
                                                                                                             self._log)
                device["downlink_converter"] = TBModuleLoader.import_module("snmp_listener", device.get('converter',
                                                                                               self._default_converters[
                                                                                                   "downlink"]))(device)
        except Exception as e:
            self._log.exception(e)

    @staticmethod
    def __get_common_parameters(device):
        return {"ip": gethostbyname(device["ip"]),
                "port": device.get("port", 161),
                "timeout": device.get("timeout", 6),
                "community": device["community"],
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

