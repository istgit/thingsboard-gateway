from time import sleep
from pydnp3 import opendnp3, openpal
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from rich.logging import RichHandler
from rich.console import Console
from rich.pretty import pprint

class RemoteTerminal():
    def __init__(self, gateway, device, master, soe_handler):

        self.gateway = gateway
        self.config = device
        self.master_log_level:int=15
        self.name = self.config.get("deviceName")
        self.port:int = self.config.get("port", 20000)
        self.remote_ip = self.config.get("outstation_ip")
        self.remote_id:int = self.config.get("outstation_id")
        self.timeout:int = self.config.get("timeout", 10)
        self.max_retries:int = self.config.get("max_retries", 2)
        self.retry_delay:int = self.config.get("retry_delay",1)
        self.stale_if_longer_than:float = 2  # in seconds
        self.datatypes = ('attributes', 'telemetry')
        self.previous_poll_time = 0
        self.polling_interval:float = self.config.get("polling_interval",10000)
        self._log = init_logger(gateway, f"RTU_{self.name}", "DEBUG", enable_remote_logging=True)
        self._log.addHandler(RichHandler())  # Enhance logging with rich
        self.console = Console()  # For additional detailed output
        self.data = {}
        self.master = master
        self.soe_handler = soe_handler
        self.uplink_converter = None
        self.downlink_converter = None
        self.profile = self.config.get("profile")

        polling_int = int(self.polling_interval)

        # self._log.debug('Configuring some scans (periodic reads).')
        self.console.log('Configuring some scans (periodic reads).')  # Detailed rich logging

        # Add class scans after all masters are initialized
        #sleep(0.01)
        #master.AddClassScan(
        #        opendnp3.ClassField().AllClasses(),
        #        openpal.TimeDuration().Milliseconds(600000),
        #        opendnp3.TaskConfig().Default()
        #    )
        #self._log.debug(f"Added class scan for outstation {device}")

        #sleep(1)
        # Set up a "slow scan", an infrequent integrity poll that requests events and static data for all classes.
        #self.slow_scan = self.master.AddClassScan(opendnp3.ClassField().AllClasses(),
         #                                         openpal.TimeDuration().Milliseconds(polling_int),
          #                                      opendnp3.TaskConfig().Default())

        # Set up a "fast scan", a relatively-frequent exception poll that requests events and class 1 static data.
        #self.fast_scan = self.master.AddClassScan(opendnp3.ClassField(opendnp3.ClassField.CLASS_1),
        #                                          openpal.TimeDuration().Minutes(1),
        #                                          opendnp3.TaskConfig().Default())

    def __repr__(self):
        return f"RemoteTerminal(name={self.name}. outstation_id={self.remote_id})"

        #pprint(f"Outstation id ==== {self.remote_id}")