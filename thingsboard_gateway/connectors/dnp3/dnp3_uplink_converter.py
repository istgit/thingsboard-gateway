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

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
import time
import logging
from typing import Dict, Tuple, Optional

class DNP3UplinkConverter(Converter):
    def __init__(self, device, logger):
        """
        Initialize the uplink converter for a RemoteTerminal.

        Args:
            device: RemoteTerminal instance containing config, soe_handler, and gateway.
            logger: Logger instance for debugging.
        """
        self._log = logger
        self.__device = device
        self.device_name = device.name
        self.device_type = device.config.get("deviceType", "dnp3 connector")
        self.datatypes = ("attributes", "telemetry")
        self.mappings = {datatype: device.config.get(datatype, []) for datatype in self.datatypes}
        self._log.debug(f"Initialized DNP3UplinkConverter for device {self.device_name}")

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, device, data: Dict[Tuple[int, str], Tuple[any, Optional[int]]]) -> ConvertedData:
        """
        Convert SOE handler data to ThingsBoard ConvertedData format.

        Args:
            device: RemoteTerminal instance.
            data: Dict from OutstationSOEProxy.data, e.g., {(outstation_id, field): (value, event_time)}

        Returns:
            ConvertedData: Object containing telemetry and attributes for ThingsBoard.
        """
        converted_data = ConvertedData(device_name=self.device_name, device_type=self.device_type)
        server_time = int(time.time() * 1000)  # Current server time in ms

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__device.config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.device_name, e)

        try:
            for (outstation_id, field), (value, event_time) in data.items():
                if outstation_id != device.remote_id:
                    continue  # Skip data for other outstations

                # Determine datatype (attributes or telemetry)
                datatype_config = {"key": field}  # Default config
                datatype = "telemetry"  # Default to telemetry
                for dt in self.datatypes:
                    for cfg in self.mappings[dt]:
                        if cfg.get("key") == field:
                            datatype_config = cfg
                            datatype = dt
                            break
                    if datatype == dt:
                        break

                if value is not None:
                    try:
                        # Convert field to DatapointKey
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(
                            field, device_report_strategy, datatype_config, self._log
                        )

                        if datatype == "attributes":
                            converted_data.add_to_attributes(datapoint_key, value)
                            self._log.debug(f"Added attribute: {datapoint_key} = {value} for {self.device_name}")
                        else:  # telemetry
                            timestamp = event_time if event_time else server_time
                            print(server_time)
                            telemetry_entry = TelemetryEntry(
                                values={datapoint_key: value},
                                ts=timestamp
                            )
                            print("TELEMETRY ENTRY", telemetry_entry)
                            converted_data.add_to_telemetry(telemetry_entry)
                            self._log.debug(f"Added telemetry: {datapoint_key} = {value}, ts={timestamp} for {self.device_name}")
                    except Exception as e:
                        self._log.error(f"Error processing field {field} for {self.device_name}: {str(e)}")
                else:
                    self._log.warning(f"Null value for field {field} in device {self.device_name}")

        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

        self._log.debug(converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                 count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                 count=converted_data.telemetry_datapoints_count)
        print("dnp3_uplink_converter.py is ACTIVE")
        return converted_data