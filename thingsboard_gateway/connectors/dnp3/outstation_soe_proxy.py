import csv
import os
import asyncio
from typing import Dict, Tuple, Optional
import pathlib

from pydnp3 import opendnp3
from dnp3_python.dnp3station.visitors import *
from rich.pretty import pprint
from rich.logging import RichHandler
from rich.console import Console
import logging

from thingsboard_gateway.connectors.dnp3.visitorindexedbinary import VisitorIndexedAnalogOutputStatus, VisitorIndexedBinary, \
    VisitorIndexedBinaryOutputStatus, VisitorIndexedCounter, VisitorIndexedDoubleBitBinary, VisitorIndexedFrozenCounter, \
    VisitorIndexedTimeAndInterval


class OutstationSOEProxy(opendnp3.ISOEHandler):
    def __init__(self, logger: logging.Logger, outstation_id: int, profile_file: str):
        """
        Initialize SOE handler with profile mapping.

        Args:
            logger: Logger instance for debugging.
            outstation_id: DNP3 outstation ID.
            profile_file: Name of the profile CSV file (e.g., 'DNP3Profile2.csv').
        """
        super().__init__()
        self.data: Dict[
            Tuple[int, str], Tuple[any, Optional[int]]] = {}  # {(outstation_id, field): (value, event_time)}
        self.logger = logger
        self.logger.addHandler(RichHandler())  # Enhance logging with rich
        self.console = Console()  # For additional detailed output
        self.outstation_id = outstation_id
        self.logger.setLevel(logging.WARNING)
        # Dynamically determine the profile directory based on the location of this file
        profile_dir = os.path.dirname(os.path.abspath(__file__))
        self.profile = self._load_profile(os.path.join(profile_dir, profile_file))

    def _load_profile(self, profile_path: str) -> Dict[Tuple[opendnp3.GroupVariation, int], str]:
        """
        Load DNP3 profile from CSV file.

        Args:
            profile_path: Path to the CSV file.

        Returns:
            Dict mapping (GroupVariation, Index) to Field name.
        """
        profile = {}
        try:
            if not os.path.exists(profile_path):
                self.logger.error(f"Profile file {profile_path} not found")
                self.console.log(f"[red]Profile file {profile_path} not found[/red]")  # Detailed rich logging
                return profile

            with open(profile_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    group_variation_str = row['GroupVariation']
                    index = int(row['Index'])
                    field = row['Field']

                    try:
                        group_variation = getattr(opendnp3.GroupVariation, group_variation_str)
                    except AttributeError:
                        self.logger.warning(f"Invalid GroupVariation {group_variation_str} in profile")
                        self.console.log(
                            f"[yellow]Invalid GroupVariation {group_variation_str} in profile[/yellow]")  # Detailed rich logging
                        continue

                    profile[(group_variation, index)] = field
                    # self.logger.debug(f"Loaded profile mapping: {group_variation}, {index} → {field}")
                    # self.console.log(
                    #     f"Loaded profile mapping: {group_variation}, {index} → {field}")  # Detailed rich logging
        except Exception as e:
            self.logger.error(f"Error loading profile {profile_path}: {str(e)}")
            self.console.log(f"[red]Error loading profile {profile_path}: {str(e)}[/red]")  # Detailed rich logging
        return profile

    async def process_async(self, info, values):
        """
        Asynchronously process DNP3 SOE data.

        Args:
            info: HeaderInfo object from DNP3 stack.
            values: ICollection of DNP3 data points.
        """
        pprint("Made it to process_async")
        loop = asyncio.get_running_loop()
        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval,
        }
        visitor_class = visitor_class_types.get(type(values))
        if visitor_class:
            visitor = visitor_class()


            self.console.log("Starting Foreach on values...")
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, values.Foreach, visitor),
                    timeout=5.0
                )
                self.console.log("Finished Foreach on values.")
            except asyncio.TimeoutError:
                self.console.log("[red]Foreach timed out[/red]")


            for item in visitor.index_and_value:
                index = item[0]
                value = item[1]
                event_time = item[2] if len(item) > 2 else 0

                field = self.profile.get((info.gv, index), f"Unknown_{info.gv}_{index}")
                key = (self.outstation_id, field)
                self.data[key] = (value, event_time)
                print(f"Visitor received: index={index}, value={value}, time={event_time}")

                # self.logger.debug(
                #     f"SOE: Outstation {self.outstation_id}, Group {info.gv}, Index {index}, Field {field}, Value {value}, Time {event_time}")
                self.console.log(
                    f"SOE: Outstation {self.outstation_id}, Group {info.gv}, Index {index}, Field {field}, Value {value}, Time {event_time}")  # Detailed rich logging
        else:
            pprint("failed at visitor_class")

    def Process(self, info, values):
        """
        Synchronous wrapper for async processing.

        Args:
            info: HeaderInfo object from DNP3 stack.
            values: ICollection of DNP3 data points.
        """
        asyncio.run(self.process_async(info, values))

    def clear_data(self):
        """Clear stored data to prevent stale entries."""
        self.data.clear()
        # self.logger.debug(f"Cleared data for outstation {self.outstation_id}")
        self.console.log(f"Cleared data for outstation {self.outstation_id}")  # Detailed rich logging

    def Start(self):
        # pprint('In SOEHandler.Start')
        self.console.log('In SOEHandler.Start')  # Detailed rich logging

    def End(self):
        # pprint('In SOEHandler.End')
        self.console.log('In SOEHandler.End')  # Detailed rich logging