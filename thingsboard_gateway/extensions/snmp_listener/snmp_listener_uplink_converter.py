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

from datetime import timedelta

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics


class SNMPListenerUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                        end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        device_name = self.__config['deviceName']
        device_type = self.__config['deviceType']

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['deviceName'], e)

        result = {}
        # print("<hb> DATA", data)
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
                #print("str")
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
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

        self._log.debug(converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)


        # print("result:", result)
        return result
