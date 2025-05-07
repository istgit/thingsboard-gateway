import logging
import json
import time
from dnp3_python.dnp3station.master_new import MyMasterNew

# Configuring logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DNP3Converter")

class DNP3Converter:
    def __init__(self, master_application: MyMasterNew):
        """
        Initializes the DNP3Converter.
        :param master_application: Instance of MyMasterNew. (DNP3 master)
        """
        self.master = master_application

        print("dnp3_converter.py is ACTIVE")

    def get_binary_inputs(self):
        """
        Reads binary inputs from the DNP3 outstation.
        :return: Dictionary of binary input values.
        """
        try:
            binary_inputs = self.master.get_db_by_group_variation(group=1, variation=2)

            if binary_inputs is None:
                binary_inputs = {}

            # Convert GroupVariation keys to strings
            binary_inputs = {str(k):v for k,v in binary_inputs.items()}

            logger.debug(f"Binary Inputs: {binary_inputs}")
            return {"binaryInputs": binary_inputs}

        except Exception as e:
            logger.error(f"Error getting binary inputs: {e}")
            return {"binaryInputs": {}}

    def get_analog_inputs(self):
        """
        Reads analog inputs from the DNP3 outstation.
        :return: Dictionary of analog input values.
        """
        try:
            analog_inputs = self.master.get_db_by_group_variation(group=30, variation=3)
            if analog_inputs is None:
                analog_inputs = {}

            # Convert GroupVariation keys to strings
            analog_inputs = {str(k):v for k,v in analog_inputs.items()}

            logger.debug(f"Analog Inputs: {analog_inputs}")
            return {"analogInputs": analog_inputs}

        except Exception as e:
            logger.error(f"Error getting analog inputs: {e}")
            return {"analogInputs": {}}

    def format_data(self):
        """
        Retrieves and formats binary and analog inputs into ThingsBoard-compatible JSON.
        :return: JSON string of formatted data.
        """
        data = {
            "ts": int(time.time() * 1000), # Timestamp in milliseconds
            "values": {}
        }

        # Fetch binary and analog values
        binary_data = self.get_binary_inputs()
        analog_data = self.get_analog_inputs()

        # Merge into values field
        data["values"].update(binary_data)
        data["values"].update(analog_data)

        formatted_json = json.dumps(data)
        logger.debug(f"Formatted JSON: {formatted_json}")
        return formatted_json

#
# print("Starting DNP3Converter script...")  # Debug print
#
# # Example Usage
# if __name__ == "__main__":
#     print("Initializing DNP3Connector...")  # Debug print
#     from dnp3_connector import DNP3Connector  # Import connector class
#
#     # Initialize the DNP3 Connector
#     connector = DNP3Connector()
#     print("Connector initialized, starting DNP3 master...")  # Debug print
#     master = connector.start()
#
#     print("DNP3 Master started, initializing Converter...")  # Debug print
#     converter = DNP3Converter(master)
#
#     print("Test Data Retrieval...")  # Debug print
#     print(converter.format_data())
