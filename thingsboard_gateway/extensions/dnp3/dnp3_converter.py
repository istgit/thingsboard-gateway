import logging
import json
import time
from pydnp3 import opendnp3
from dnp3_python.dnp3station.station_utils import collection_callback
from rich.logging import RichHandler
from rich.console import Console

# Configuring logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DNP3Converter")
logger.addHandler(RichHandler())  # Enhance logging with rich

class DNP3Converter:
    def __init__(self, master):
        """
        Initializes the DNP3Converter.
        :param master: DNP3 IMaster instance (from channel.AddMaster).
        """
        self.master = master
        self.console = Console()  # For additional detailed output
        print("dnp3_converter.py is ACTIVE")

    def get_binary_inputs(self):
        """
        Reads binary inputs from the DNP3 outstation.
        :return: Dictionary of binary input values.
        """
        try:
            binary_inputs = {}
            def collect_binary(header, collection):
                for idx, value in enumerate(collection.values):
                    binary_inputs[f"Group1Var2_{idx}"] = value.value
            self.master.Scan(opendnp3.Header().Binary(1, 2), opendnp3.TaskConfig().With(collection_callback(collect_binary)))
            logger.debug(f"Binary Inputs: {binary_inputs}")
            self.console.log(f"Binary Inputs: {binary_inputs}")  # Detailed rich logging
            return {"binaryInputs": binary_inputs}
        except Exception as e:
            logger.error(f"Error getting binary inputs: {e}")
            self.console.log(f"[red]Error getting binary inputs: {e}[/red]")  # Detailed rich logging
            return {"binaryInputs": {}}

    def get_analog_inputs(self):
        """
        Reads analog inputs from the DNP3 outstation.
        :return: Dictionary of analog input values.
        """
        try:
            analog_inputs = {}
            def collect_analog(header, collection):
                for idx, value in enumerate(collection.values):
                    analog_inputs[f"Group30Var3_{idx}"] = value.value
            self.master.Scan(opendnp3.Header().Analog(30, 3), opendnp3.TaskConfig().With(collection_callback(collect_analog)))
            logger.debug(f"Analog Inputs: {analog_inputs}")
            self.console.log(f"Analog Inputs: {analog_inputs}")  # Detailed rich logging
            return {"analogInputs": analog_inputs}
        except Exception as e:
            logger.error(f"Error getting analog inputs: {e}")
            self.console.log(f"[red]Error getting analog inputs: {e}[/red]")  # Detailed rich logging
            return {"analogInputs": {}}

    def format_data(self):
        """
        Retrieves and formats binary and analog inputs into ThingsBoard-compatible JSON.
        :return: JSON string of formatted data.
        """
        data = {
            "ts": int(time.time() * 1000),  # Timestamp in milliseconds
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
        self.console.log(f"Formatted JSON: {formatted_json}")  # Detailed rich logging
        return formatted_json