import logging
import time
from dnp3_python.dnp3station.master_new import MyMasterNew
from dnp3_converter import DNP3Converter    # Import the converter class

# Configuring logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DNP3Connector")


class DNP3Connector:
    def __init__(self, master_ip="0.0.0.0", outstation_ip="192.168.88.243", port=20000, master_id=2, outstation_id=1):
        self.master_ip = master_ip
        self.outstation_ip = outstation_ip
        self.port = port
        self.master_id = master_id
        self.outstation_id = outstation_id
        self.master_application = None
        self.converter = None # Initialize converter variable

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
        """Start the DNP3 connection and monitor connectivity."""
        retry_count = 0

        while True:
            if not self.initialize_connection():
                retry_count += 1
                wait_time = min(5 * retry_count, 30)
                logger.warning(f"DNP3 Connection failed. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            self.master_application.start()
            logger.info("DNP3 connection started successfully.")
            retry_count = 0

            # Wait for the connection to stabilize
            time.sleep(5)


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
