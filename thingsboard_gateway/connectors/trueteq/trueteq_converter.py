import json
import time
from thingsboard_gateway.connectors.converter import Converter, log

class TrueTeqMqttConverter(Converter):
    def __init__(self, config):
        self.config = config

    def convert(self, config, data):
        """
        Converts TrueTeq data into ThingsBoard-compatible MQTT format.

        :param config: Configuration for the converter.
        :param data: Raw data fetched from TrueTeq.
        :return: Dictionary containing telemetry data and attributes in ThingsBoard format.
        """
        try:
            # Parse the raw data (assume it's JSON)
            if isinstance(data, str):
                data = json.loads(data)

            # Extract relevant fields from the TrueTeq response
            timestamp = data.get("timestamp", int(time.time() * 1000))  # Default to current time if not provided
            telemetry_values = data.get("values", {})
            attributes = data.get("attributes", {})  # New field for attributes

            # Construct the ThingsBoard-compatible telemetry message
            converted_data = {
                "telemetry": [
                    {"ts": timestamp, "values": telemetry_values}
                ],
                "attributes": attributes
            }

            log.debug("Converted data: %s", converted_data)
            return converted_data
        except Exception as e:
            log.error("Error while converting data: %s", str(e))
            return {}