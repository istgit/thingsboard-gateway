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
from datetime import timedelta

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class SNMP2UplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, profile, oids, data):
        device_name = self.__config['deviceName']
        device_type = self.__config['deviceType']

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['deviceName'], e)

        try:
            for datatype in ('attributes', 'telemetry'):
                # hb - use the datatype configurations in the profiles
                for datatype_config in profile[datatype]:
                    data_key = datatype_config["key"]
                    item_data = data.get(data_key)
                    value = None
                    if isinstance(item_data, dict):
                        # hb - use the OIDs
                        index = 1
                        value = {}
                        for k, v in item_data.items():
                            if (k in oids):
                                new_key = oids.get(k)
                            else:
                                new_key = str(index)
                                index += 1
                            lookup_val = str(v)
                            if (lookup_val in oids):
                                new_val = oids.get(lookup_val)
                            else:
                                new_val = lookup_val
                            value.update({new_key: new_val})
                            #value = {str(k): str(v) for k, v in item_data.items()}
                    elif isinstance(item_data, list):
                        if isinstance(item_data[0], str):
                            value = ','.join(item_data)
                        elif isinstance(item_data[0], dict):
                            res = {}
                            for item in item_data:
                                res.update(**item)
                            value = {str(k): str(v) for k, v in res.items()}
                    elif isinstance(item_data, str):
                        value = item_data
                    elif isinstance(item_data, bytes):
                        value = item_data.decode("UTF-8")
                    elif isinstance(item_data, timedelta):
                        value = item_data.total_seconds()
                    else:
                        value = item_data

                    if value is not None:
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(data_key, device_report_strategy,
                                                                               datatype_config, self._log)
                        if datatype == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, value)
                            print(datapoint_key, ": " ,value)
                        else:
                            telemetry_entry = TelemetryEntry({datapoint_key: value})
                            converted_data.add_to_telemetry(telemetry_entry)
                            print(datapoint_key, ": ", value)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

        self._log.debug(converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data
