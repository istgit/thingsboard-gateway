import json
import logging
import time
from thingsboard_gateway.connectors.dnp3.dnp3_converter import DNP3Converter
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

# Configure logging for debugging and monitoring
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DNP3Uplink")

class DNP3Uplink:
    """
    Handles polling and sending DNP3 data to ThingsBoard via MQTT.
    """
    def __init__(self, master_application, mqtt_client):
        """
        Initializes the DNP3Uplink.
        :param master_application: DNP3 Master instance managing communication.
        :param mqtt_client: ThingsBoard MQTT client instance.
        """
        self.converter = DNP3Converter(master_application)
        self.mqtt_client = mqtt_client

    def poll_and_send(self):
        """
        Polls data from the DNP3 outstation, formats it, and sends it to ThingsBoard via MQTT.
        """
        try:
            logger.info("Polling data from DNP3 outstation...")
            formatted_data = self.converter.format_data()  # Retrieve formatted DNP3 data

            # Prepare payload for ThingsBoard (Assuming telemetry topic "v1/devices/me/telemetry")
            topic = "v1/devices/me/telemetry"
            payload = formatted_data

            # Publish data to ThingsBoard
            logger.info(f"Publishing data to ThingsBoard: {payload}")
            self.mqtt_client.publish(topic, payload)
            print("dnp3_uplink.py is ACTIVE")
        except Exception as e:
            logger.error(f"Error during communication: {e}")




