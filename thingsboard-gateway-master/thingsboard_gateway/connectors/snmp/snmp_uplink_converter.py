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

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class SNMPUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        #result = {
        #    "deviceName": self.__config["deviceName"],
        #    "deviceType": self.__config["deviceType"],
        #    "attributes": [],
        #    "telemetry": []
        #    }
        result = {}
        #print("<hb> DATA", data)
        try:
            if isinstance(data, dict):
                # print("dict")
                result = data
            elif isinstance(data, list):
                # print("list")
                if isinstance(data[0], str):
                    # print("list - str")
                    result[config[0]].append({config[1]["key"]: ','.join(data)})
                elif isinstance(data[0], dict):
                    # print("list - dict")
                    res = {}
                    for item in data:
                        res.update(**item)
                    # result[config[0]].append({config[1]["key"]: {str(k): str(v) for k, v in res.items()}})
                    result = {str(config[1]["key"]+"_"+k): str(v) for k, v in res.items()}

            elif isinstance(data, str):
                # print("str")
                result = {config[1]["key"]: data}
            elif isinstance(data, bytes):
                # print("bytes")
                # result[config[0]].append({config[1]["key"]: data.decode("UTF-8")})
                result = {config[1]["key"]: data.decode("UTF-8")}
            else:
                # print("number")
                # result[config[0]].append({config[1]["key"]: data})
                result = {config[1]["key"]: data}
            self._log.debug(result)
        except Exception as e:
            self._log.exception(e)
        print("result:", result)
        return result
