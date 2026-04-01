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

        # Scaling factors from device manual (value = raw * multiplier + offset)
        self.scaling = {
            "imbVoltage": {"multiplier": 0.1, "offset": 0},
            "voltage1": {"multiplier": 0.1, "offset": 0},
            "voltage2": {"multiplier": 0.1, "offset": 0},
            "voltage3": {"multiplier": 0.1, "offset": 0},
            "current1": {"multiplier": 0.1, "offset": 0},
            "current2": {"multiplier": 0.1, "offset": 0},
            "current3": {"multiplier": 0.1, "offset": 0},
            "currentN": {"multiplier": 0.1, "offset": 0},
            "angle1": {"multiplier": 0.1, "offset": 0},
            "angle2": {"multiplier": 0.1, "offset": 0},
            "angle3": {"multiplier": 0.1, "offset": 0},
            "lineFrequency": {"multiplier": 0.01, "offset": 0},
            "THD": {"multiplier": 0.1, "offset": 0},
            "ambTemp": {"multiplier": 0.085, "offset": 50},
            "trxTemp": {"multiplier": 0.085, "offset": 50},
            "battVoltage": {"multiplier": 0.1, "offset": 0},
            "psuVoltage": {"multiplier": 0.1, "offset": 0},
            "gsmRSSI": {"multiplier": 1, "offset": 0},
            "gpsSatellites": {"multiplier": 1, "offset": 0},
        }

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
                        # Apply scaling if defined for this field
                        scaled_value = value
                        if field in self.scaling:
                            m = self.scaling[field]["multiplier"]
                            o = self.scaling[field]["offset"]

                            if field == "ambTemp" or field == "trxTemp":
                                scaled_value = round((value*0.085) - 50,2)
                            else:
                                scaled_value = round((value * m) + o,3)

                            
                            self._log.debug(f"Scaled {field}: raw={value} → {scaled_value:.2f} (m={m}, o={o})")

                        # Convert field to DatapointKey
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(
                            field, device_report_strategy, datatype_config, self._log
                        )

                        if datatype == "attributes":
                            converted_data.add_to_attributes(datapoint_key, scaled_value)
                            self._log.debug(f"Added attribute: {datapoint_key} = {scaled_value} for {self.device_name}")
                        else:  # telemetry
                            # Use event_time only if it's valid (after year 2000 = 946684800000 ms)
                            # Devices with stuck clocks (e.g., 1970) would cause ThingsBoard to
                            # ignore these updates since older polled data has a newer 2026 timestamp.
                            # Reject timestamps before 2020-01-01 (1577836800000 ms).
                            # This device clock is stuck in ~2001, so year-2000 threshold
                            # was not enough — 2001 timestamps pass that check.
                            YEAR_2020_MS = 1577836800000
                            if event_time and event_time > YEAR_2020_MS:
                                timestamp = event_time
                            else:
                                timestamp = server_time
                                if event_time and event_time > 0:
                                    self._log.debug(
                                        f"Replaced stale DNP3 timestamp {event_time} "
                                        f"({event_time // 1000}s epoch, ~2001) with server time for field '{field}'"
                                    )
                            telemetry_entry = TelemetryEntry(
                                values={datapoint_key: scaled_value},
                                ts=timestamp
                            )
                            print("TELEMETRY ENTRY", telemetry_entry)
                            converted_data.add_to_telemetry(telemetry_entry)
                            self._log.debug(f"Added telemetry: {datapoint_key} = {scaled_value}, ts={timestamp} for {self.device_name}")
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
        return converted_data#     Copyright 2025. ThingsBoard
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

# Fields injected by the TruTalk identity harvester.
# Always sent to ThingsBoard as client *attributes*, never as telemetry.
_TRUTALK_FIELDS = frozenset({
    "devSoftwareVersion",    # ?3  firmware version  e.g. "V3.17b"
    "devSerialNumber",       # ?4  serial number     e.g. "TCT000851"
    "devManufactureDate",    # ?5  manufacture date  e.g. "10_2017"
    "devProductName",        # ?5  model name (V3.1+ only)
    "devHardwareVersion",    # ?5  hardware version  (V3.1+ only)
    "devLocationLatitude",   # ?6  GPS latitude  (only set when sats > 0)
    "devLocationLongitude",  # ?6  GPS longitude (only set when sats > 0)
    "devGPSSatellites",      # ?6  satellite count (always set)
})

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

        # Scaling factors from device manual (value = raw * multiplier + offset)
        self.scaling = {
            "imbVoltage": {"multiplier": 0.1, "offset": 0},
            "voltage1": {"multiplier": 0.1, "offset": 0},
            "voltage2": {"multiplier": 0.1, "offset": 0},
            "voltage3": {"multiplier": 0.1, "offset": 0},
            "current1": {"multiplier": 0.1, "offset": 0},
            "current2": {"multiplier": 0.1, "offset": 0},
            "current3": {"multiplier": 0.1, "offset": 0},
            "currentN": {"multiplier": 0.1, "offset": 0},
            "angle1": {"multiplier": 0.1, "offset": 0},
            "angle2": {"multiplier": 0.1, "offset": 0},
            "angle3": {"multiplier": 0.1, "offset": 0},
            "lineFrequency": {"multiplier": 0.01, "offset": 0},
            "THD": {"multiplier": 0.1, "offset": 0},
            "ambTemp": {"multiplier": 0.085, "offset": 50},
            "trxTemp": {"multiplier": 0.085, "offset": 50},
            "battVoltage": {"multiplier": 0.1, "offset": 0},
            "psuVoltage": {"multiplier": 0.1, "offset": 0},
            "gsmRSSI": {"multiplier": 1, "offset": 0},
            "gpsSatellites": {"multiplier": 1, "offset": 0},
        }

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
                # TruTalk identity fields are always attributes, not telemetry
                if field in _TRUTALK_FIELDS:
                    datatype = "attributes"
                else:
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
                        # Apply scaling if defined for this field
                        scaled_value = value
                        if field in self.scaling:
                            m = self.scaling[field]["multiplier"]
                            o = self.scaling[field]["offset"]

                            if field == "ambTemp" or field == "trxTemp":
                                scaled_value = round((value*0.085) - 50,2)
                            else:
                                scaled_value = round((value * m) + o,3)

                            
                            self._log.debug(f"Scaled {field}: raw={value} → {scaled_value:.2f} (m={m}, o={o})")

                        # Convert field to DatapointKey
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(
                            field, device_report_strategy, datatype_config, self._log
                        )

                        if datatype == "attributes":
                            converted_data.add_to_attributes(datapoint_key, scaled_value)
                            self._log.debug(f"Added attribute: {datapoint_key} = {scaled_value} for {self.device_name}")
                        else:  # telemetry
                            # Use event_time only if it's valid (after year 2000 = 946684800000 ms)
                            # Devices with stuck clocks (e.g., 1970) would cause ThingsBoard to
                            # ignore these updates since older polled data has a newer 2026 timestamp.
                            # Reject timestamps before 2020-01-01 (1577836800000 ms).
                            # This device clock is stuck in ~2001, so year-2000 threshold
                            # was not enough — 2001 timestamps pass that check.
                            YEAR_2020_MS = 1577836800000
                            if event_time and event_time > YEAR_2020_MS:
                                timestamp = event_time
                            else:
                                timestamp = server_time
                                if event_time and event_time > 0:
                                    self._log.debug(
                                        f"Replaced stale DNP3 timestamp {event_time} "
                                        f"({event_time // 1000}s epoch, ~2001) with server time for field '{field}'"
                                    )
                            telemetry_entry = TelemetryEntry(
                                values={datapoint_key: scaled_value},
                                ts=timestamp
                            )
                            print("TELEMETRY ENTRY", telemetry_entry)
                            converted_data.add_to_telemetry(telemetry_entry)
                            self._log.debug(f"Added telemetry: {datapoint_key} = {scaled_value}, ts={timestamp} for {self.device_name}")
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