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

import asyncio
import gc
import os
import resource
import traceback
from random import choice
from re import search
from socket import gethostbyname, gaierror
from string import ascii_lowercase
from threading import Thread
from time import time

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

# Try import library or install it and import
installation_required = False

try:
    from puresnmp import __version__ as pymodbus_version

    if int(pymodbus_version.split('.')[0]) < 2:
        installation_required = True
except ImportError:
    installation_required = True

if installation_required:
    print("Puresnmp library not found - installing...")
    TBUtility.install_package("puresnmp", ">=2.0.0")

# <hb> added import for version of the snmp module
from puresnmp import Client, credentials, PyWrapper, V2C
from puresnmp.exc import Timeout as SNMPTimeoutException

# --- Patch: puresnmp 2.0.1's SNMPClientProtocol.get_data() only aborts its
# UDP transport when ITS OWN internal per-attempt timeout fires (the
# `except (asyncio.TimeoutError, socket.timeout)` branch below, confirmed
# from the installed package source). If the awaiting task is cancelled
# from OUTSIDE instead - exactly what __poll_device_guarded's outer
# asyncio.wait_for() safety-net timeout does whenever it actually fires -
# a CancelledError is raised at the same await point, but that branch
# doesn't catch it, so transport.abort() is never called and the UDP
# socket is orphaned. This is the confirmed root cause of the file
# descriptor leak found in soak testing (and, as a side effect, of the
# InvalidStateError seen earlier - a late response landing on a future
# our own cancellation had already invalidated). Patched here at import
# time rather than editing the installed package, so the fix survives a
# `pip install --upgrade puresnmp` and stays visible next to the code
# that depends on it. Worth reporting upstream to exhuma/puresnmp too.
import socket as _socket
import puresnmp.transport as _puresnmp_transport


async def _patched_get_data(self, timeout):
    try:
        return await asyncio.wait_for(self.future, timeout)
    except (asyncio.TimeoutError, _socket.timeout) as exc:
        if self.transport:
            self.transport.abort()
        raise SNMPTimeoutException(
            f"{timeout} second timeout exceeded on UDP transport.") from exc
    except asyncio.CancelledError:
        # the actual fix: abort the transport here too, then re-raise -
        # never swallow a CancelledError, that breaks asyncio's
        # cancellation contract for whatever cancelled us in the first place
        if self.transport:
            self.transport.abort()
        raise


_puresnmp_transport.SNMPClientProtocol.get_data = _patched_get_data


class SNMP2Connector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type
        self.__config = config
        self.__id = self.__config.get('id')
        self.name = config.get("name", 'SNMP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self._log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self.__gateway, self.name + "_converter",
                                          self.__config.get('logLevel', 'INFO'),
                                          enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                          is_connector_logger=True, attr_name=self.name)
        self.__devices = self.__config["devices"]
        # hb - added loading of the polling profiles for the SNMP devices and OIDs
        self.__profiles = self.__config["profiles"]
        self.__oids = self.__config["oids"]

        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._default_converters = {
            "uplink": "SNMP2UplinkConverter",
            "downlink": "SNMP2DownlinkConverter"
        }
        self.__methods = ["get", "multiget", "getnext", "walk", "multiwalk", "set", "multiset",
                          "bulkget", "bulkwalk", "table", "bulktable"]

        self.__datatypes = ('attributes', 'telemetry')

        # hb - added the possible profile types
        self.__profile_types = ("slow_poll", "frequent_poll")

        # <hb> added property for short interval monitoring
        self.__short_interval_mode = False

        # scaling controls - bound how many polls are in flight at once so a
        # 1000-device sweep doesn't fire 1000 simultaneous UDP requests
        self.__max_concurrent_polls = self.__config.get("maxConcurrentPolls", 100)
        self.__semaphore = None  # created inside the running loop, in _run()

        # backoff controls for devices that are down / unreachable
        self.__max_backoff_ms = self.__config.get("maxBackoffMs", 300000)  # 5 min ceiling
        self.__failure_threshold = self.__config.get("failureThresholdForBackoff", 3)

        # the fd usage check forces a full gc.collect() pass, which is not
        # free (a generation-2 collection walks the whole heap) - the poll
        # loop ticks roughly every 100ms when idle, so calling this
        # unthrottled means ~10 full GC passes per second for no benefit.
        # Throttle it to a much coarser interval; leak growth plays out
        # over minutes/hours, so checking every 30s loses no useful signal.
        self.__fd_check_interval_ms = self.__config.get("fdCheckIntervalMs", 30000)
        self.__last_fd_check = 0

        self.__loop = asyncio.new_event_loop()

    def open(self):
        self.__stopped = False
        self.__fill_converters()
        self.__resolve_devices()
        self.start()

    def run(self):
        self._connected = True
        self.__loop.set_exception_handler(self.__handle_loop_exception)
        try:
            self.__loop.run_until_complete(self._run())
        except Exception as e:
            self._log.exception(e)

    def __handle_loop_exception(self, loop, context):
        """puresnmp's asyncio UDP transport can raise InvalidStateError when
        a response arrives after its request has already timed out and the
        associated future was already resolved - see
        https://github.com/exhuma/puresnmp/issues/125 for the related
        per-request-socket behavior that makes this race more likely under
        concurrent load. It's harmless (the stale response is simply
        discarded) but left unhandled it dumps a full ERROR traceback for
        every occurrence, which gets noisy fast at device counts this high
        and, with enableRemoteLogging on, ships straight to the platform.
        Everything else still goes through the default handler untouched."""
        exception = context.get('exception')
        if isinstance(exception, asyncio.InvalidStateError):
            tb = traceback.extract_tb(exception.__traceback__)
            if any('puresnmp' in frame.filename for frame in tb):
                self._log.debug("Discarded a late SNMP response that arrived after "
                                "its request had already timed out: %s", context.get('message'))
                return
        loop.default_exception_handler(context)

    def __resolve_devices(self):
        """Resolve hostnames once at startup instead of on every poll cycle.

        gethostbyname() is a blocking call - doing it inside the async poll
        loop stalls the whole event loop on every single device, every
        cycle. Resolve once here and cache the result on the device dict.
        """
        for device in self.__devices:
            try:
                device["_resolved_ip"] = gethostbyname(device["ip"])
            except (gaierror, OSError) as e:
                self._log.error("Could not resolve host \"%s\" for device \"%s\": %s - "
                                "will retry on next connector restart",
                                device.get("ip"), device.get("deviceName"), e)
                # fall back to whatever was configured; if it's already an
                # IP literal this still works fine
                device["_resolved_ip"] = device["ip"]

    async def _run(self):
        self.__semaphore = asyncio.Semaphore(self.__max_concurrent_polls)
        while not self.__stopped:
            self.__check_fd_usage()
            current_time = time() * 1000
            tasks = []
            for device in self.__devices:
                device_profiles_list = device.get("profiles")
                for index, profile in enumerate(self.__profiles):
                    # hb - check to see if this profile is applicable
                    if profile.get("profile_name") in device_profiles_list:
                        last_poll_times = device.get("last_poll_times", [0] * len(self.__profiles))
                        last_poll = last_poll_times[index]

                        # <hb> added checking for short poll interval
                        if self.__short_interval_mode is True and profile.get("fast_polling_option", "false") == "true":
                            poll_interval = 10000
                        else:
                            poll_interval = profile.get("pollPeriod", 10000)

                        if last_poll + poll_interval < current_time:
                            # skip devices in failure backoff so a bank of
                            # offline units doesn't keep consuming poll
                            # slots every single cycle
                            if self.__in_backoff(device, current_time):
                                continue

                            # mark as polled now, before awaiting, so a slow
                            # in-flight poll can't get scheduled again next
                            # tick while it's still running
                            last_poll_times[index] = current_time
                            device["last_poll_times"] = last_poll_times

                            tasks.append(self.__poll_device_guarded(device, profile))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            if self.__stopped:
                break
            else:
                await asyncio.sleep(.1)

    def __check_fd_usage(self):
        """Circuit breaker for the socket leak in puresnmp's UDP transport
        layer (every request opens a fresh datagram socket - see
        https://github.com/exhuma/puresnmp/issues/125 - and some fraction
        of those aren't being closed, causing fd count to climb steadily
        over a run). Rather than guess at puresnmp's internals and risk a
        silent no-op patch, this watches the process's actual fd usage and
        forces a clean, fast restart before exhaustion takes down every
        device at once. Safe because the systemd unit has Restart=always /
        RestartSec=10 - a ~10s blip beats a multi-hour outage."""
        now = time() * 1000
        if now - self.__last_fd_check < self.__fd_check_interval_ms:
            return
        self.__last_fd_check = now

        try:
            soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            open_fds_before = len(os.listdir(f'/proc/{os.getpid()}/fd'))

            # asyncio UDP transports normally close themselves via a __del__
            # safety net once nothing references them - but transport,
            # protocol, and the future they were going to fulfill typically
            # form a reference cycle, which plain refcounting can't reclaim
            # and only a full GC pass can. This is a cheap, safe experiment:
            # if it reclaims fds, the abandoned transports were reachable-
            # but-unused and this is a real mitigation; if the count doesn't
            # move, something is still holding a live reference and the
            # leak needs a fix at the puresnmp source level instead.
            collected = gc.collect()
            open_fds = len(os.listdir(f'/proc/{os.getpid()}/fd'))
            reclaimed = open_fds_before - open_fds
            if reclaimed > 0:
                self._log.info("Garbage collection reclaimed %d file descriptor(s) "
                               "(%d objects collected) - likely orphaned SNMP transports "
                               "from unanswered requests.", reclaimed, collected)

            usage_ratio = open_fds / soft_limit
            if usage_ratio >= 0.8:
                self._log.critical(
                    "Open file descriptors at %d/%d (%.0f%% of limit) - this looks like "
                    "the known puresnmp socket leak approaching exhaustion. Restarting "
                    "the process now rather than waiting for every device to start "
                    "failing; systemd will bring the service back up in ~10s.",
                    open_fds, soft_limit, usage_ratio * 100)
                os._exit(1)
            elif usage_ratio >= 0.5:
                self._log.warning("Open file descriptors at %d/%d (%.0f%% of limit) - "
                                  "climbing steadily suggests the puresnmp socket leak; "
                                  "worth tracking whether this correlates with poll volume.",
                                  open_fds, soft_limit, usage_ratio * 100)
        except Exception as e:
            self._log.debug("Could not check file descriptor usage: %s", e)

    def __in_backoff(self, device, current_time):
        backoff_until = device.get("_backoff_until", 0)
        return current_time < backoff_until

    def __register_poll_success(self, device):
        device["_consecutive_failures"] = 0
        device["_backoff_until"] = 0

    def __register_poll_failure(self, device, current_time):
        failures = device.get("_consecutive_failures", 0) + 1
        device["_consecutive_failures"] = failures
        if failures >= self.__failure_threshold:
            backoff_ms = min(self.__max_backoff_ms, 1000 * (2 ** (failures - self.__failure_threshold)))
            device["_backoff_until"] = current_time + backoff_ms
            self._log.warning("Device \"%s\" (%s) has failed %d consecutive polls - "
                              "backing off for %.0f seconds",
                              device.get("deviceName"), device.get("ip"), failures, backoff_ms / 1000)

    async def __poll_device_guarded(self, device, profile):
        """Wraps a single device poll with concurrency limiting and a hard
        timeout ceiling, so one slow/offline device can never stall the
        others, regardless of what puresnmp does internally."""
        async with self.__semaphore:
            device_timeout = device.get("timeout", 6)
            # generous outer ceiling: the per-request client timeout is the
            # primary control, this is a safety net against anything that
            # hangs without raising SNMPTimeoutException
            outer_timeout = device_timeout * 3 + 2
            try:
                await asyncio.wait_for(self.__process_data(device, profile), timeout=outer_timeout)
                self.__register_poll_success(device)
            except (asyncio.TimeoutError, SNMPTimeoutException):
                self.__register_poll_failure(device, time() * 1000)
                self._log.warning("Timeout polling device \"%s\" (%s)",
                                  device.get("deviceName"), device.get("ip"))
            except Exception as e:
                self.__register_poll_failure(device, time() * 1000)
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

    async def __process_data(self, device, profile):
        common_parameters = self.__get_common_parameters(device)
        device_responses = {}
        for datatype in self.__datatypes:
            datatype_configs = profile[datatype]

            # Collapse plain "get" entries into a single multiget request -
            # this is the single biggest lever for round-trip count: a
            # profile with 10 individual "get" telemetry entries used to
            # mean 10 separate SNMP requests per device per cycle, each
            # paying its own round-trip latency. One multiget PDU carries
            # all of them and comes back as one ordered list of values.
            # Anything using another method (walk/bulkwalk/table/etc.)
            # still goes out as its own request, since those already
            # return multi-value payloads in one round trip.
            batchable_configs = [c for c in datatype_configs
                                 if c.get("method", "").lower() == "get" and c.get("oid")]
            batchable_ids = set(id(c) for c in batchable_configs)
            other_configs = [c for c in datatype_configs if id(c) not in batchable_ids]

            try:
                if batchable_configs:
                    oids = [c["oid"] for c in batchable_configs]
                    response_list = await self.__process_methods(
                        "multiget", common_parameters, {"oid": oids})

                    # multiget preserves request order per SNMP GetRequest
                    # semantics, so this zip lines each value back up with
                    # the key it came from
                    for cfg, value in zip(batchable_configs, response_list):
                        device_responses[cfg['key']] = value

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    StatisticsService.count_connector_bytes(self.name, len(response_list),
                                                            stat_parameter_name='connectorBytesReceived')

                for datatype_config in other_configs:
                    method = datatype_config.get("method")
                    if method is None:
                        self._log.error("Method not found in configuration: %r", datatype_config)
                        continue
                    else:
                        method = method.lower()
                    if method not in self.__methods:
                        self._log.error("Unknown method: %s, configuration is: %r", method, datatype_config)

                    response = await self.__process_methods(method, common_parameters, datatype_config)
                    device_responses[datatype_config['key']] = response

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    StatisticsService.count_connector_bytes(self.name, response,
                                                            stat_parameter_name='connectorBytesReceived')
            except SNMPTimeoutException:
                self._log.error("Timeout exception on connection to device \"%s\" with ip: \"%s\"",
                                device["deviceName"],
                                device["ip"])
                raise
            except Exception as e:
                self._log.exception(e)

        # <hb> check if there are alarms active requiring short interval polling
        # (moved outside the per-config loop now that "upsAlarmsPresent" may
        # arrive via either the batched multiget path or an individual request)
        alarms_present = device_responses.get("upsAlarmsPresent")
        if alarms_present is not None:
            self.__short_interval_mode = alarms_present > 0

        if device_responses:  # hb - also pass the profile and oids to the uplink converter
            converted_data: ConvertedData = device["uplink_converter"].convert(device, profile, self.__oids, device_responses)

            if (converted_data is not None and
                    (converted_data.attributes_datapoints_count > 0 or
                     converted_data.telemetry_datapoints_count > 0)):
                self.collect_statistic_and_send(self.get_name(), self.get_id(), converted_data)

    async def __process_methods(self, method, common_parameters, datatype_config):
        client = Client(ip=common_parameters['ip'],
                        port=common_parameters['port'],
                        credentials=V2C(common_parameters['community']))

        client.configure(timeout=common_parameters['timeout'])
        client = PyWrapper(client)
        response = None

        if method == "get":
            oid = datatype_config["oid"]
            response = await client.get(oid=oid)
        elif method == "multiget":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = await client.multiget(oids=oids)
        elif method == "getnext":
            oid = datatype_config["oid"]
            master_response = await client.getnext(oid=oid)
            response = {master_response.oid: master_response.value}
        elif method == "walk":
            oid = datatype_config["oid"]
            response = {}
            async for binded_var in client.walk(oid=oid):
                response[binded_var.oid] = binded_var.value
        elif method == "multiwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = {}
            async for binded_var in client.multiwalk(oids=oids):
                response[binded_var.oid] = binded_var.value
        elif method == "set":
            oid = datatype_config["oid"]
            value = datatype_config["value"]
            response = await client.set(oid=oid, value=value)
        elif method == "multiset":
            mappings = datatype_config["mappings"]
            response = await client.multiset(mappings=mappings)
        elif method == "bulkget":
            scalar_oids = datatype_config.get("scalarOid", [])
            scalar_oids = scalar_oids if isinstance(scalar_oids, list) else list(scalar_oids)
            repeating_oids = datatype_config.get("repeatingOid", [])
            repeating_oids = repeating_oids if isinstance(repeating_oids, list) else list(repeating_oids)
            max_list_size = datatype_config.get("maxListSize", 1)
            response = await client.bulkget(scalar_oids=scalar_oids, repeating_oids=repeating_oids,
                                            max_list_size=max_list_size)
            response = response.scalars
        elif method == "bulkwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = {}
            async for binded_var in client.bulkwalk(bulk_size=bulk_size, oids=oids):
                response[binded_var.oid] = binded_var.value
        elif method == "table":
            oid = datatype_config["oid"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            response = await client.table(oid=oid)
        elif method == "bulktable":
            oid = datatype_config["oid"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = await client.bulktable(oid=oid, bulk_size=bulk_size)
        else:
            self._log.error("Method \"%s\" - Not found", str(method))
        return response

    def __fill_converters(self):
        try:
            for device in self.__devices:
                device["uplink_converter"] = TBModuleLoader.import_module("snmp2", device.get('converter',
                                                                                             self._default_converters[
                                                                                                 "uplink"]))(device,
                                                                                                             self._converter_log)
                device["downlink_converter"] = TBModuleLoader.import_module("snmp2", device.get('converter',
                                                                                               self._default_converters[
                                                                                                   "downlink"]))(device)
        except Exception as e:
            self._log.exception(e)

    @staticmethod
    def __get_common_parameters(device):
        # ip is resolved once in __resolve_devices() at startup, not on
        # every poll - gethostbyname() is a blocking call that would
        # otherwise stall the whole event loop each time it's invoked
        return {"ip": device.get("_resolved_ip", device["ip"]),
                "port": device.get("port", 161),
                "timeout": device.get("timeout", 6),
                "community": device["community"]
                }

    def on_attributes_update(self, content):
        try:
            device = self.__find_device_by_name(content["device"])
            if device is None:
                self._log.error("Device \"%s\" not found", content["device"])
                return

            for attribute_request_config in device["attributeUpdateRequests"]:
                for attribute, value in content["data"]:
                    if search(attribute, attribute_request_config["attributeFilter"]):
                        common_parameters = self.__get_common_parameters(device)
                        # NOTE: this must be scheduled on the connector's own
                        # event loop and awaited via the threadsafe bridge -
                        # calling the coroutine directly here without
                        # awaiting it silently does nothing.
                        result = asyncio.run_coroutine_threadsafe(
                            self.__process_methods(attribute_request_config["method"], common_parameters,
                                                   {**attribute_request_config, "value": value}),
                            loop=self.__loop
                        ).result(timeout=int(attribute_request_config.get("timeout", 5)))
                        self._log.debug(
                            "Received attribute update request for device \"%s\" "
                            "with attribute \"%s\" and value \"%s\"",
                            content["device"],
                            attribute)
                        self._log.debug(result)
                        self._log.debug(content)
        except Exception as e:
            self._log.exception(e)

    def __find_device_by_name(self, device_name):
        device_filter = tuple(filter(lambda device: device["deviceName"] == device_name, self.__devices))
        if len(device_filter):
            return device_filter[0]

    def server_side_rpc_handler(self, content):
        try:
            device = self.__find_device_by_name(content["device"])

            if device is None:
                self._log.error("Device \"%s\" not found", content["device"])
                return

            rpc_method_name = content["data"]["method"]

            if self.__check_and_process_reserved_rpc(device, rpc_method_name, content):
                return

            rpc_config = tuple(filter(lambda rpc_config: search(
                rpc_method_name, rpc_config['requestFilter']), device["serverSideRpcRequests"]))
            if len(rpc_config):
                self.__process_rpc_request(device, rpc_config[0], content)
            else:
                self._log.error("RPC method \"%s\" not found", rpc_method_name)
        except Exception as e:
            self._log.exception(e)
            self.__gateway.send_rpc_reply(device=content["device"],
                                          req_id=content["data"]["id"],
                                          content={'error': e.__repr__(), "success": False})

    def __check_and_process_reserved_rpc(self, device, rpc_method_name, content):
        if rpc_method_name in ('get', 'set'):
            self._log.debug('Processing reserved RPC method: %s', rpc_method_name)

            params = {}
            for param in content['data']['params'].split(';'):
                try:
                    (key, value) = param.split('=')
                except ValueError:
                    continue

                if key and value:
                    params[key] = value

            if rpc_method_name == 'set':
                content['data']['params'] = params['value']

            self.__process_rpc_request(device, params, content)
            return True

        return False

    def __process_rpc_request(self, device, rpc_config, content):
        common_parameters = self.__get_common_parameters(device)
        result = asyncio.run_coroutine_threadsafe(self.__process_methods(rpc_config["method"],
                                                                         common_parameters,
                                                                         {**rpc_config,
                                                                          "value": content["data"]["params"]}),
                                                  loop=self.__loop).result(timeout=int(rpc_config.get("timeout", 5)))
        result = result.decode("utf-8") if isinstance(result, bytes) else str(result)
        self._log.trace('RPC result: %s', result)
        self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                      content={"result": result})
