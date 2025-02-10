import requests
import json
import os
import time
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.trueteq.trueteq_converter import TrueTeqMqttConverter

class TrueTeqConnector(Connector):
    def __init__(self, gateway, config_file, connector_type):
        super().__init__()
        self.gateway = gateway
        self.config = self.load_config(config_file)
        self.converter = TrueTeqMqttConverter(self.config)  # Initialize the converter
        self.running = False

    def load_config(self, config_file):
        """Load configuration from the specified JSON file."""
        config_path = os.path.join(os.path.dirname(__file__), config_file)
        try:
            with open(config_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            log.error("Failed to load configuration from %s: %s", config_file, str(e))
            raise

    def open(self):
        log.info("Starting TrueTeq Connector...")
        self.running = True

    def close(self):
        log.info("Stopping TrueTeq Connector...")
        self.running = False

    def get_name(self):
        return self.get_name()

    def is_connected(self):
        return self.running

    def on_attributes_update(self, content):
        """
        Handle attribute updates from ThingsBoard.
        """
        log.debug("Attributes update received: %s", content)
        device_name = content.get("device")
        attributes = content.get("data")
        log.info("Updating attributes for device %s: %s", device_name, attributes)

    def server_side_rpc_handler(self, content):
        """
        Handle RPC commands from ThingsBoard.
        """
        try:
            rpc_id = content.get("id")
            method = content.get("data").get("method")
            params = content.get("data").get("params")
            device_name = content.get("device")

            log.info("Received RPC command for device %s: Method=%s, Params=%s", device_name, method, params)

            # Send the RPC command to TrueTeq
            headers = {
                "Authorization": f"Bearer {self.config['trueteqApiKey']}",
                "Content-Type": "application/json"
            }
            payload = {
                "method": method,
                "params": params
            }
            response = requests.post(f"{self.config['trueteqBaseUrl']}/api/rpc", headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json()
                log.info("RPC command executed successfully: %s", result)
                self.gateway.send_rpc_reply(device_name, rpc_id, result)  # Send the result back to ThingsBoard
            else:
                log.error("Failed to execute RPC command: %s", response.text)
                self.gateway.send_rpc_reply(device_name, rpc_id, {"error": "Failed to execute RPC command"})
        except Exception as e:
            log.exception("Error while handling RPC command: %s", str(e))

    def collect_data(self):
        try:
            headers = {
                "Authorization": f"Bearer {self.config['trueteqApiKey']}",
                "Content-Type": "application/json"
            }
            response = requests.get(f"{self.config['trueteqBaseUrl']}/api/telemetry", headers=headers)
            if response.status_code == 200:
                raw_data = response.json()
                log.debug("Fetched raw data: %s", raw_data)
                converted_data = self.converter.convert(self.config, raw_data)  # Use the converter
                self.send_to_thingsboard(converted_data)
            else:
                log.error("Failed to fetch data from TrueTeq: %s", response.text)
        except Exception as e:
            log.exception("Error while collecting data: %s", str(e))

    def send_to_thingsboard(self, converted_data):
        try:
            device_name = self.config['deviceName']
            telemetry = converted_data.get("telemetry", [])
            attributes = converted_data.get("attributes", {})

            # Send telemetry data
            if telemetry:
                self.gateway.send_telemetry(device_name, telemetry)
                log.debug("Sent telemetry data to ThingsBoard: %s", telemetry)

            # Send attributes
            if attributes:
                self.gateway.send_attributes(device_name, attributes)
                log.debug("Sent attributes to ThingsBoard: %s", attributes)
        except Exception as e:
            log.exception("Error while sending data to ThingsBoard: %s", str(e))

    def run(self):
        while self.running:
            self.collect_data()
            time.sleep(self.config.get("pollingInterval", 60))