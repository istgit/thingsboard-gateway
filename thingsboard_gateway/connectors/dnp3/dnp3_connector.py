import json
import logging
import os
import time
from dnp3_python.dnp3station.master_new import MyMasterNew
from dnp3_converter import DNP3Converter    # Import the converter class
from paho.mqtt import client as mqtt_client
from dnp3_uplink import DNP3Uplink

# Configuring logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DNP3Connector")

TB_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../config/tb_gateway.json")
DNP3_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../config/dnp3_config.json")

class DNP3Connector:
    def __init__(self):
        """Initialize the DNP3Connector with DNP3 and MQTT configurations."""
        self.load_config()
        self.master_application = None
        self.converter = None
        self.mqtt_client = None
        self.uplink = None

    def load_config(self):
        """Load configuration from tb_gateway.json (ThingsBoard MQTT settings).
        and dnp3_config.json (DNP3 settings).
        """
        try:
            # Load DNP3 Configuration
            if not os.path.exists(DNP3_CONFIG_PATH):
                raise FileNotFoundError(f"DNP3 config file not found at {DNP3_CONFIG_PATH}")

            with open(DNP3_CONFIG_PATH, "r") as dnp3_file:
                dnp3_config = json.load(dnp3_file)

            # Validate the structure
            if not isinstance(dnp3_config, dict):
                raise ValueError("Invalid JSON format in dnp3_config.json")

            # Extract DNP3 settings
            self.master_ip = dnp3_config.get("master_ip", "0.0.0.0")
            self.outstation_ip = dnp3_config.get("outstation_ip", "192.168.88.243")
            self.port = dnp3_config.get("port", 20000)
            self.master_id = dnp3_config.get("master_id", 1)
            self.outstation_id = dnp3_config.get("outstation_id", 1)
            self.polling_interval = dnp3_config.get("polling_interval", 5000)

            print(f"Loaded DNP3 configuration: {dnp3_config}")

            # Load ThingsBoard Configuration
            if not os.path.exists(TB_CONFIG_PATH):
                raise FileNotFoundError(f"TB config file not found at {TB_CONFIG_PATH}")

            with open(TB_CONFIG_PATH, "r") as tb_file:
                tb_config = json.load(tb_file)

            # Validate structure
            if not isinstance(tb_config, dict):
                raise ValueError("Invalid JSON format in tb_gateway.json")

            # Extract MQTT settings from ThingsBoard JSON
            thingsboard_settings =tb_config.get("thingsboard", {})
            self.tb_host = thingsboard_settings.get("host", "localhost")
            self.tb_port = thingsboard_settings.get("port", 1883)
            self.tb_access_token = thingsboard_settings.get("security", {}).get("accessToken", "")

            logger.info(f"Loaded DNP3 configuration: {thingsboard_settings}")

        except json.JSONDecodeError as e:
            print(f"Error - Failed to parse JSON: {e}")

        except Exception as e:
            logger.error(f"Error - Failed to load configuration: {e}")

    def initialize_mqtt(self):
        """Initialize the MQTT client."""
        try:
            self.mqtt_client = mqtt_client.Client()
            self.mqtt_client.username_pw_set(self.tb_access_token)
            self.mqtt_client.connect(self.tb_host, self.tb_port, 60)
            logger.info("MQTT client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}")
            raise


    def initialize_connection(self):
        """Initialize the DNP3 connection."""
        try:
            self.master_application = MyMasterNew(
                master_ip=self.master_ip,
                outstation_ip=self.outstation_ip,
                port=self.port,
                master_id=self.master_id,
                outstation_id=self.outstation_id
            )
            logger.info("DNP3 connection initialized successfully.")

            # Initialize the converter once connection is established
            self.converter = DNP3Converter(self.master_application)
            logger.info("DNP3 converter initialized successfully.")

            return True
        except Exception as e:
            logger.error(f"Failed to initialize DNP3 connection: {e}")
            return False

    def start(self):
        """Start the DNP3 connection and MQTT integration."""
        retry_count = 0

        while True:
            if not self.initialize_connection():
                retry_count += 1
                wait_time = min(5 * retry_count, 30)
                logger.warning(f"DNP3 Connection failed. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            self.initialize_mqtt()
            self.master_application.start()
            logger.info("DNP3 connection started successfully.")
            retry_count = 0

            # Wait for the connection to stabilize
            time.sleep(5)

            # Start Uplink process
            self.uplink = DNP3Uplink(self.master_application, self.mqtt_client)

            while True:
                try:
                    # Check connection state
                    if not self.master_application.is_connected:
                        logger.warning("DNP3 Connection lost. Restarting...")
                        time.sleep(5)   # Wait before retrying
                        break   # Exit inner loop to reinitialize connection


                    # Send read request
                    logger.info("Polling data from DNP3 outstation...")
                    self.master_application.send_scan_all_request()
                    logger.info("Scan request sent successfully.")
                    self.uplink.poll_and_send()


                    if self.converter:
                        formatted_data = self.converter.format_data()
                        logger.debug(f"Formatted data: {formatted_data}")

                    time.sleep(10)  # Poll every 10 seconds

                except Exception as e:
                    logger.error(f"Error during communication: {e}")
                    break  # Exit to reinitialize connection

            logger.info("Shutting down DNP3 Master before reconnecting...")
            self.master_application.shutdown()
            time.sleep(5)  # Short delay before retrying



if __name__ == "__main__":
    connector = DNP3Connector()
    connector.start()
