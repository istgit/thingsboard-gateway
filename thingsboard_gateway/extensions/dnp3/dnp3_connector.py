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

import csv
import logging
import multiprocessing
import os
import time
from random import choice
from socket import gethostbyname
from string import ascii_lowercase
from threading import Thread
from typing import Dict, Tuple, Optional


from pydnp3 import opendnp3, openpal, asiopal, asiodnp3

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

from thingsboard_gateway.extensions.dnp3.visitorindexedbinary import (
    VisitorIndexedAnalogOutputStatus,
    VisitorIndexedBinary,
    VisitorIndexedBinaryOutputStatus,
    VisitorIndexedCounter,
    VisitorIndexedDoubleBitBinary,
    VisitorIndexedFrozenCounter,
    VisitorIndexedTimeAndInterval,
    VisitorIndexedAnalogTime
)
from thingsboard_gateway.extensions.dnp3.trutalk_identity import TruTalkIdentityHarvester
import socket

from thingsboard_gateway.extensions.dnp3.trutalk_identity import (
     _parse_q3, _parse_q4, _parse_q5, _parse_q6,
)

RTUs = []


def _discover_unsolicited_header_values():
    """
    Safely discover which HeaderType enum values represent unsolicited responses.
    Different pydnp3 builds expose different attribute names, so we probe at
    import time rather than hardcoding a name that may not exist.
    Returns a frozenset of matching enum values (may be empty if none found).
    """
    _log = logging.getLogger("DNP3Connector")

    # Log ALL HeaderType enum members so we can identify the unsolicited value
    # in builds that use non-standard names.
    try:
        all_members = {
            name: getattr(opendnp3.HeaderType, name)
            for name in dir(opendnp3.HeaderType)
            if not name.startswith("_")
        }
        _log.info(f"[HeaderType] All enum members: {all_members}")
    except Exception as e:
        _log.warning(f"[HeaderType] Could not enumerate members: {e}")

    candidates = ("UNSOLICITED", "UnsolicitedResponse", "UNSOLICITED_RESPONSE")
    found = set()
    for name in candidates:
        val = getattr(opendnp3.HeaderType, name, None)
        if val is not None:
            found.add(val)

    if found:
        _log.info(f"[HeaderType] Unsolicited values found: {found}")
    else:
        _log.warning(
            "[HeaderType] No unsolicited enum value found under known names. "
            "Check the '[HeaderType] All enum members' log above and update "
            "the candidates list with the correct name for this pydnp3 build."
        )
    return frozenset(found)


# Evaluated once at import – safe even if no matching attribute exists.
_UNSOLICITED_HEADER_VALUES = _discover_unsolicited_header_values()


class OutstationSOEProxy(opendnp3.ISOEHandler):
    """
    Proxy for handling Sequence of Events (SOE) from DNP3 outstations.
    Maps DNP3 GroupVariation/Index to human-readable field names for ThingsBoard.

    Thread-safe: uses threading.Lock to protect data stores that are written
    by the DNP3 stack thread and read by the monitoring loop.
    """

    def __init__(self, logger, outstation_id: int, profile_file: str, profile_dir: str,
                 polling_interval_sec: int = 60):
        """
        Initialize SOE handler with profile mapping.

        Args:
            logger: Logger instance for debugging.
            outstation_id: DNP3 outstation ID.
            profile_file: Name of the profile CSV file (e.g., 'dnp3profile.csv').
            profile_dir: Directory containing profile files.
            polling_interval_sec: Scheduled polling interval in seconds.
        """
        import threading
        super().__init__()

        # ── Thread-safe data stores ──────────────────────────────────────────
        # static_data      – polled static / integrity-poll responses
        # event_data       – polled event GVs (Group2, Group22, Group32 …)
        # unsolicited_data – data the outstation PUSHED without being asked
        self._lock = threading.Lock()
        self.static_data:      Dict[Tuple[int, str], Tuple[any, Optional[int]]] = {}
        self.event_data:       Dict[Tuple[int, str], Tuple[any, Optional[int]]] = {}
        self.unsolicited_data: Dict[Tuple[int, str], Tuple[any, Optional[int]]] = {}

        self.logger = logger
        self.outstation_id = outstation_id
        self.profile_dir = profile_dir
        self.logger.setLevel(logging.DEBUG)

        # ── Timing-based unsolicited detection ──────────────────────────────
        # Since this pydnp3 build lacks HeaderType.UNSOLICITED, we use timing:
        # Event GVs arriving BETWEEN polls = unsolicited push from outstation
        # Event GVs arriving DURING poll window = polled response
        self.polling_interval_sec = polling_interval_sec
        self.last_poll_time = 0.0         # timestamp of last scheduled poll
        self.poll_window_sec = 5.0        # data within 5s of poll = "polled"
        self._response_start_time = 0.0   # wall-clock time of current response Start()
        self._response_had_static = False  # True if current response contained static GVs
        self._pending_events = {}           # event GV data buffered until End() classifies the response
        
        if _UNSOLICITED_HEADER_VALUES:
            self.logger.info(
                f"Outstation {outstation_id}: unsolicited detection via HeaderType "
                f"(values: {_UNSOLICITED_HEADER_VALUES})"
            )
        else:
            self.logger.info(
                f"Outstation {outstation_id}: unsolicited detection via TIMING "
                f"(poll interval={polling_interval_sec}s, window={self.poll_window_sec}s). "
                f"Event GVs between polls are treated as unsolicited."
            )

        if profile_dir.endswith(profile_file):
            profile_path = profile_dir
        else:
            profile_path = os.path.join(profile_dir, profile_file)

        print(f"===============================================")
        print(f"Profile directory: {self.profile_dir}")
        print(f"===============================================")
        self.profile = self._load_profile(profile_path)

        # Event GroupVariations - these trigger unsolicited responses
        self.event_gvs = {
            # Binary Input Events (Group 1 Var 2 - with time)
            opendnp3.GroupVariation.Group1Var1,
            opendnp3.GroupVariation.Group1Var2,
            opendnp3.GroupVariation.Group2Var2,  # Binary Input Event - Absolute time
            
            # Binary Output Events (Groups 11-13)
            opendnp3.GroupVariation.Group10Var2,
            opendnp3.GroupVariation.Group11Var2,
            opendnp3.GroupVariation.Group13Var2,

            
            # Counter Events (Groups 20-23 Var 5 - 32-bit)
            opendnp3.GroupVariation.Group20Var5,
            opendnp3.GroupVariation.Group21Var5,
            opendnp3.GroupVariation.Group22Var5,
            opendnp3.GroupVariation.Group23Var5,
            
            # Analog Input Events (Group 32-34 Var 4 - 16-bit with time)
            opendnp3.GroupVariation.Group32Var4,
            # opendnp3.GroupVariation.Group34Var1,
            
            # Analog Output Status Events (Groups 41-43 Var 2)
            opendnp3.GroupVariation.Group41Var2,
            opendnp3.GroupVariation.Group42Var2,
            opendnp3.GroupVariation.Group43Var2,
        }

    def _load_profile(self, profile_path: str) -> Dict[Tuple[opendnp3.GroupVariation, int], str]:
        """
        Load DNP3 profile from CSV file.
        Maps (GroupVariation, Index) -> Field Name
        """
        profile = {}
        try:
            self.logger.info(f"===============================================")
            self.logger.info(f"Profile directory: {self.profile_dir}")
            self.logger.info(f"===============================================")
        
            if not os.path.exists(profile_path):
                self.logger.error(f"===============================================")
                self.logger.error(f"Profile file {profile_path} not found")
                self.logger.error(f"===============================================")
                return profile

            self.logger.info(f"Loading profile from: {profile_path}")

            with open(profile_path, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0

                for row in reader:
                    row_count += 1

                    # Check if required columns exist
                    if 'GroupVariation' not in row or 'Index' not in row or 'Field' not in row:
                        self.logger.error(
                            f"Row {row_count} missing required columns. "
                            f"Expected: GroupVariation, Index, Field. Got: {list(row.keys())}"
                        )
                        continue

                    group_variation_str = row['GroupVariation'].strip()
                    index = int(row['Index'])
                    field = row['Field'].strip()

                    try:
                        group_variation = getattr(opendnp3.GroupVariation, group_variation_str)
                        profile[(group_variation, index)] = field
                        self.logger.debug(f"Loaded: {group_variation_str}[{index}] -> '{field}'")
                    except AttributeError:
                        self.logger.warning(
                            f"Invalid GroupVariation '{group_variation_str}' at row {row_count}. "
                            f"Must match opendnp3.GroupVariation enum (e.g., 'Group1Var2')"
                        )
                        continue

                self.logger.info(
                    f"Profile loaded: {len(profile)} mappings from {row_count} rows "
                    f"for outstation {self.outstation_id}"
                )

        except Exception as e:
            self.logger.error(f"Error loading profile {profile_path}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

        return profile

    def Process(self, info, values):
        """
        Process incoming DNP3 data and map to human-readable field names.

        Unsolicited detection strategy:
        1. If HeaderType.UNSOLICITED is available → use it (primary)
        2. Otherwise → timing-based detection:
           - Event GVs arriving DURING poll window → polled event
           - Event GVs arriving BETWEEN polls → unsolicited push
        """
        import time as time_module
        
        gv_str = str(info.gv).split('.')[-1]
        is_event_gv = info.gv in self.event_gvs
        current_time = time_module.time()

        # ── Unsolicited detection ─────────────────────────────────────────
        # last_poll_time is anchored in End() only when the response contained
        # static GVs (i.e. it was a poll). Unsolicited responses never contain
        # static GVs so they never update last_poll_time, meaning genuine
        # unsolicited messages always show a large time_since_last_poll and are
        # correctly flagged. See Start()/End() for the bracket logic.
        is_unsolicited = False
        
        if _UNSOLICITED_HEADER_VALUES:
            # Primary: use HeaderType if available
            try:
                is_unsolicited = bool(
                    info.headerType in _UNSOLICITED_HEADER_VALUES
                )
            except Exception:
                pass
        
        if not is_unsolicited and is_event_gv:
            # Fallback: timing-based detection for event GVs
            # Data arriving outside the poll window = unsolicited push
            time_since_last_poll = current_time - self.last_poll_time
            in_poll_window = (time_since_last_poll <= self.poll_window_sec)
            
            if not in_poll_window:
                is_unsolicited = True
                self.logger.debug(
                    f"Timing-based unsolicited: {time_since_last_poll:.1f}s "
                    f"since poll (window={self.poll_window_sec}s)"
                )

        self.logger.debug("=" * 60)
        self.logger.debug(f"SOE Process called for Outstation {self.outstation_id}")
        self.logger.debug(f"GroupVariation : {gv_str}")
        self.logger.debug(f"Is Event GV   : {is_event_gv}")
        self.logger.debug(f"Is Unsolicited: {is_unsolicited}")
        self.logger.debug("=" * 60)

        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalogTime
        }
        visitor_class = visitor_class_types.get(type(values))

        if not visitor_class:
            self.logger.warning(f"No visitor found for type: {type(values)}")
            return

        visitor = visitor_class()
        values.Foreach(visitor)

        self.logger.debug(f"Visitor extracted {len(visitor.index_and_value)} values")

        with self._lock:
            for item in visitor.index_and_value:
                index = item[0]
                value = item[1]
                event_time = item[2] if len(item) > 2 else 0
                
                # Fix invalid DNP3 timestamps (device clock wrong)
                # If timestamp is before year 2000 (946684800000 ms), use system time
                if event_time > 0 and event_time < 946684800000:
                    old_time = event_time
                    event_time = int(time.time() * 1000)  # Current system time in ms
                    self.logger.info(
                        f"🕐 Replaced invalid DNP3 timestamp {old_time} with system time {event_time}"
                    )

                field = self.profile.get((info.gv, index))

                if field is None:
                    for (profile_gv, profile_idx), profile_field in self.profile.items():
                        if str(profile_gv).split('.')[-1] == gv_str and profile_idx == index:
                            field = profile_field
                            break

                if field is None:
                    self.logger.warning(f"  UNMAPPED POINT: {gv_str}[{index}] = {value}")
                    self.logger.warning(
                        f"  Add to profile CSV: {gv_str},{index},Description,YourFieldName"
                    )
                    continue

                key = (self.outstation_id, field)

                if is_event_gv:
                    # Buffer ALL event GV data — End() will commit it to the
                    # correct store (unsolicited_data or event_data) once it
                    # knows whether this response contained static GVs or not.
                    # This eliminates the race where the monitoring loop could
                    # snapshot_and_clear() between Process() and End().
                    self._pending_events[key] = (value, event_time, field, gv_str, index)
                    self.logger.info("!" * 60)
                    self.logger.info(f"   ⏳ EVENT BUFFERED (pending End() classification)")
                    self.logger.info(f"   Outstation : {self.outstation_id}")
                    self.logger.info(f"   GV         : {gv_str}[{index}]")
                    self.logger.info(f"   Event Field: '{field}'")
                    self.logger.info(f"   Value      : {value}")
                    self.logger.info(f"   Timestamp  : {event_time}")
                    self.logger.info("!" * 60)

                else:
                    # Static / integrity-poll data — commit immediately, no ambiguity
                    self.static_data[key] = (value, event_time)
                    self._response_had_static = True  # flag for End() poll-window anchor
                    self.logger.debug(
                        f"STATIC: Outstation {self.outstation_id}, "
                        f"{gv_str}[{index}] -> '{field}' = {value}"
                    )

    def clear_data(self):
        """Clear all stored data."""
        with self._lock:
            self.static_data.clear()
            self.event_data.clear()
            self.unsolicited_data.clear()
        self.logger.debug(f"Cleared data for outstation {self.outstation_id}")

    def snapshot_and_clear(self):
        """
        Atomically snapshot all data stores and clear them.
        Returns (static_snap, event_snap, unsolicited_snap).

        Using one lock acquisition for both snapshot and clear prevents
        the race condition where new data arrives between a copy() and clear().
        """
        with self._lock:
            static_snap      = dict(self.static_data)
            event_snap       = dict(self.event_data)
            unsolicited_snap = dict(self.unsolicited_data)
            self.static_data.clear()
            self.event_data.clear()
            self.unsolicited_data.clear()
        return static_snap, event_snap, unsolicited_snap

    def mark_poll_start(self):
        """
        Mark the start of a scheduled poll window.
        Used for timing-based unsolicited detection when HeaderType
        doesn't expose UNSOLICITED values.
        """
        import time as time_module
        with self._lock:
            self.last_poll_time = time_module.time()

    def Start(self):
        """
        Called once at the start of every DNP3 response (solicited AND unsolicited).
        We use Start()/End() to bracket a response and track whether it contained
        any static GVs (Group30Var4 etc.). If it did, it was a poll response and
        we anchor last_poll_time at End() time so subsequent timing checks work.

        We cannot anchor in Start() because that would reset last_poll_time for
        unsolicited responses too, making them look like poll responses.
        Instead we set a per-response flag _response_had_static that Process()
        sets to True when it sees a non-event GV, and End() uses to decide
        whether to update last_poll_time.
        """
        import time as time_module
        self._response_start_time = time_module.time()
        self._response_had_static = False
        self._pending_events.clear()  # discard any orphaned events from previous response
        self.logger.debug(
            f'SOEHandler.Start — response opened at {self._response_start_time:.3f}'
        )

    def End(self):
        """
        Called once at the end of every DNP3 response.
        Now that we know the response type (poll vs unsolicited), commit the
        buffered event data to the correct store and anchor last_poll_time if
        this was a poll response.
        """
        import time as time_module
        is_poll_response = self._response_had_static

        if is_poll_response:
            with self._lock:
                self.last_poll_time = time_module.time()
            self.logger.debug('SOEHandler.End — poll response confirmed, last_poll_time anchored')
        else:
            self.logger.debug('SOEHandler.End — no static GVs, treating as unsolicited/event-only')

        # Commit buffered events to the correct store now that response type is known
        with self._lock:
            for key, (value, event_time, field, gv_str, index) in self._pending_events.items():
                if is_poll_response:
                    # Part of a poll response — store as polled event
                    self.event_data[key] = (value, event_time)
                    # Keep static in sync if this is an Event-suffixed field
                    if field.endswith("Event"):
                        static_key = (self.outstation_id, field[:-5])
                        self.event_data[static_key] = (value, event_time)
                        self.static_data[static_key] = (value, event_time)
                    self.logger.info("*" * 60)
                    self.logger.info(f"   EVENT DATA STORED (polled)")
                    self.logger.info(f"   Outstation : {self.outstation_id}")
                    self.logger.info(f"   GV         : {gv_str}[{index}]")
                    self.logger.info(f"   Event Field: '{field}'")
                    self.logger.info(f"   Value      : {value}")
                    self.logger.info(f"   Timestamp  : {event_time}")
                    self.logger.info("*" * 60)
                else:
                    # Genuinely unsolicited — store as unsolicited
                    self.unsolicited_data[key] = (value, event_time)
                    self.event_data[key] = (value, event_time)
                    if field.endswith("Event"):
                        static_key = (self.outstation_id, field[:-5])
                        self.unsolicited_data[static_key] = (value, event_time)
                        self.static_data[static_key] = (value, event_time)
                    self.logger.info("!" * 60)
                    self.logger.info(f"   ⚡ UNSOLICITED MESSAGE RECEIVED")
                    self.logger.info(f"   Outstation : {self.outstation_id}")
                    self.logger.info(f"   GV         : {gv_str}[{index}]")
                    self.logger.info(f"   Event Field: '{field}'")
                    self.logger.info(f"   Value      : {value}")
                    self.logger.info(f"   Timestamp  : {event_time}")
                    self.logger.info("!" * 60)

        self._pending_events.clear()


class RemoteTerminal:
    def __init__(self, gateway, device, master, soe_handler):
        self.gateway = gateway
        self.config = device
        self.name = self.config.get("deviceName")
        self.port = self.config.get("port", 20000)
        self.remote_ip = self.config.get("outstation_ip")
        self.remote_id = self.config.get("outstation_id")
        self.timeout = self.config.get("timeout", 6)
        self.datatypes = ('attributes', 'telemetry')
        self.previous_poll_time = 0
        self.polling_interval = self.config.get("polling_interval", 10000) / 1000.0
        self.retry_delay = self.config.get("retry_delay", asiopal.ChannelRetry().Default()) 

        if gateway is None:
            self._log = logging.getLogger(f"RTU_{self.name}")
            self._log.setLevel(logging.DEBUG)
        else:
            self._log = init_logger(gateway, f"RTU_{self.name}", "DEBUG", enable_remote_logging=True)

        self.master = master
        self.soe_handler = soe_handler
        self.profile = self.config.get("profile")
        self.uplink_converter = None
        self.downlink_converter = None

    def __repr__(self):
        return f"RemoteTerminal(name={self.name}, outstation_id={self.remote_id})"


# ─────────────────────────────────────────────────────────────────────────────
# process_batch – scalable, unsolicited-aware
# ─────────────────────────────────────────────────────────────────────────────
# Scaling notes for hundreds of outstations:
#  • ONE DNP3Manager per process, thread count = min(batch_size, 4)
#  • Atomic snapshot_and_clear() prevents DNP3-thread / monitor-loop race
#  • Unsolicited data is dispatched to the queue IMMEDIATELY (no wait)
#  • Polled static+event data is batched per check_interval (avoids flooding)
#  • Startup stagger is 50 ms (vs 200 ms) to keep init fast at scale
# ─────────────────────────────────────────────────────────────────────────────

def process_batch(batch, master_ip, master_id, stop_event, gateway_queue):
    """
    Process a batch of outstations using DNP3 master's automatic polling.
    Supports both scheduled polls and unsolicited messages from the outstation.
    """
    log_level = logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - [%(processName)s] - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(f"Batch_{os.getpid()}")
    logger.info(f"=== Starting batch with {len(batch)} devices ===")

    profile_dir = batch[0].get('profile_dir', None)
    if not profile_dir:
        logger.error("No profile_dir provided in batch config!")
        return

    logger.info(f"Using profile directory: {profile_dir}")

    # Scale DNP3Manager threads with batch size (capped at 4 to avoid RAM bloat)
    dnp3_threads = min(len(batch), 4)
    manager = asiodnp3.DNP3Manager(dnp3_threads, asiodnp3.ConsoleLogger().Create())
    channel_log_level = opendnp3.levels.NORMAL
    channel_retry = asiopal.ChannelRetry().Default()
    listener = asiodnp3.PrintingChannelListener().Create()

    device_data = {}

    # === PHASE 1: Initialize all devices ===
    logger.info(f"PHASE 1: Initializing {len(batch)} devices "
                f"(DNP3Manager threads={dnp3_threads})...")

    for i, device in enumerate(batch):
        outstation_ip        = device.get("outstation_ip")
        outstation_id        = device.get("outstation_id")
        port                 = device.get("port", 20000)
        device_name          = device.get("deviceName")
        profile_file         = device.get("profile", "DNP3Profile1.csv")
        polling_interval_sec = int(device.get('polling_interval', 60000) // 1000)

        logger.info(f"  [{i+1}/{len(batch)}] {device_name} "
                    f"(ID {outstation_id}) @ {outstation_ip}:{port} "
                    f"poll={polling_interval_sec}s")
        try:
            channel = manager.AddTCPClient(
                f"tcpclient_{outstation_id}",
                channel_log_level,
                channel_retry,
                outstation_ip,
                "0.0.0.0",
                port,
                listener
            )
            time.sleep(0.05)   # small stagger – avoids thundering-herd at startup

            stack_config = asiodnp3.MasterStackConfig()
            stack_config.link.LocalAddr  = master_id
            stack_config.link.RemoteAddr = outstation_id
            
            # Disable automatic startup integrity polls - we use scheduled application polls
            # Empty ClassField() means no classes are requested on startup
            stack_config.master.disableUnsolOnStartup = False  # Keep unsolicited enabled
            stack_config.master.startupIntegrityClassMask = opendnp3.ClassField()

            soe_handler = OutstationSOEProxy(
                logger, outstation_id, profile_file, profile_dir,
                polling_interval_sec=polling_interval_sec
            )

            master = channel.AddMaster(
                f"master_{outstation_id}",
                soe_handler,
                asiodnp3.DefaultMasterApplication().Create(),
                stack_config
            )

            # DNP3 master handles ALL scheduled polling via AddClassScan.
            # AllClasses = Class 0 (static) + Class 1/2/3 (events).
            master.AddClassScan(
                opendnp3.ClassField().AllClasses(),
                openpal.TimeDuration().Seconds(polling_interval_sec),
                opendnp3.TaskConfig().Default()
            )

            master.Enable()
            logger.info(f"  ✔ {device_name} master enabled "
                        f"(auto-poll every {polling_interval_sec}s, "
                        f"unsolicited handled by stack)")

            # ── TruTalk harvester — created here, fired concurrently in PHASE 1b ──
            try:
                trutalk_harvester = TruTalkIdentityHarvester(
                    host          = outstation_ip,
                    serial_number = device.get("trutalk_serial", device_name),
                    outstation_id = outstation_id,
                    soe_handler   = soe_handler,
                    logger        = logger,
                    port          = device.get("trutalk_port", 8870),
                )
            except Exception as tt_err:
                logger.warning(
                    f"  ⚠ TruTalk harvester init failed for {device_name}: {tt_err} "
                    f"(DNP3 telemetry unaffected)"
                )
                trutalk_harvester = None

            rtu = RemoteTerminal(None, device, master, soe_handler)

            device_data[outstation_id] = {
                'name':              device_name,
                'master':            master,
                'soe_handler':       soe_handler,
                'config':            device,
                'rtu':               rtu,
                'last_static_send':  0.0,
                'polling_interval':  polling_interval_sec,
                'check_interval':    1.0,   # seconds between polled-data checks
                'trutalk_harvester': trutalk_harvester,
            }

        except Exception as e:
            logger.error(f"  ✗ Failed to initialize {device_name}: {e}", exc_info=True)
            continue

    logger.info(f"PHASE 1 Complete: {len(device_data)}/{len(batch)} devices initialized")

    if not device_data:
        logger.error("No devices initialized. Exiting batch process.")
        manager.Shutdown()
        return

    # === PHASE 1b: TruTalk identity harvest — all devices concurrently ===
    # One thread per device so 200 devices finish in ~5s, not 200×2s = 400s.
    import threading as _threading
    harvesters_to_start = [
        dev_info['trutalk_harvester']
        for dev_info in device_data.values()
        if dev_info.get('trutalk_harvester') is not None
    ]
    if harvesters_to_start:
        logger.info(
            f"PHASE 1b: TruTalk identity harvest — "
            f"{len(harvesters_to_start)} device(s) in parallel (max wait 10s)..."
        )
        harvest_threads = []
        for harvester in harvesters_to_start:
            t = _threading.Thread(
                target=harvester.harvest,
                name=f"TruTalk-startup-{harvester.serial_number}",
                daemon=True,
            )
            t.start()
            harvest_threads.append(t)

        for t in harvest_threads:
            t.join(timeout=10)

        completed = sum(1 for t in harvest_threads if not t.is_alive())
        logger.info(
            f"PHASE 1b Complete: {completed}/{len(harvesters_to_start)} "
            f"TruTalk harvests finished within timeout"
        )
        for harvester in harvesters_to_start:
            harvester.start_daily()
        logger.info(
            f"  ✔ Daily TruTalk refresh scheduled for "
            f"{len(harvesters_to_start)} device(s) (every 24h)"
        )

    # Short wait for DNP3 stack to stabilize (no integrity polls configured)
    logger.info("Waiting 2s for DNP3 stack to stabilize...")
    time.sleep(2)

    # === PHASE 2: Load converters ===
    logger.info("PHASE 2: Loading converters...")
    for outstation_id, dev_info in device_data.items():
        rtu = dev_info['rtu']
        try:
            rtu.uplink_converter = TBModuleLoader.import_module(
                "dnp3",
                rtu.config.get('converter', 'DNP3UplinkConverter')
            )(rtu, logger)

            rtu.downlink_converter = TBModuleLoader.import_module(
                "dnp3",
                rtu.config.get('downlink_converter', 'DNP3DownlinkConverter')
            )(rtu.config)

            logger.info(f"  ✔ Converters loaded for {dev_info['name']}")
        except Exception as e:
            logger.error(f"  ✗ Converters failed for {dev_info['name']}: {e}", exc_info=True)

    # === PHASE 3: Monitoring loop ===
    logger.info("=" * 80)
    logger.info("PHASE 3: Starting monitoring loop")
    logger.info("  • DNP3 master handles all scheduled polling automatically")
    logger.info("  • Unsolicited messages are forwarded immediately")
    logger.info("  • Polled static/event data forwarded after each check interval")
    logger.info("=" * 80)

    data_sends  = 0
    unsol_sends = 0
    last_progress = time.time()

    try:
        while not stop_event.is_set():
            current_time = time.time()

            for outstation_id, dev_info in device_data.items():
                device_name = dev_info['name']
                soe_handler = dev_info['soe_handler']
                rtu         = dev_info['rtu']

                if not hasattr(rtu, 'uplink_converter') or rtu.uplink_converter is None:
                    continue   # converter not loaded yet

                # Atomic snapshot – safe against concurrent DNP3 stack writes
                static_snap, event_snap, unsolicited_snap = \
                    soe_handler.snapshot_and_clear()

                # ── 1. UNSOLICITED DATA – dispatch immediately ────────────
                if unsolicited_snap:
                    logger.info("!" * 60)
                    logger.info(f"  ⚡ UNSOLICITED from {device_name} "
                                f"({len(unsolicited_snap)} points)")
                    logger.info("!" * 60)
                    try:
                        converted = rtu.uplink_converter.convert(rtu, unsolicited_snap)
                        if converted and (converted.telemetry_datapoints_count > 0
                                          or converted.attributes_datapoints_count > 0):
                            gateway_queue.put({
                                'device_name':    device_name,
                                'converted_data': converted,
                                'source':         'unsolicited',
                            })
                            unsol_sends += 1
                            data_sends  += 1
                            logger.info(
                                f"  ✅ UNSOLICITED queued: "
                                f"{converted.telemetry_datapoints_count} telemetry, "
                                f"{converted.attributes_datapoints_count} attributes"
                            )
                    except Exception as e:
                        logger.error(f"  ✗ Error converting unsolicited data "
                                     f"for {device_name}: {e}", exc_info=True)

                # ── 2. POLLED DATA – batch per check_interval ─────────────
                # Merge static + events, but PRIORITIZE events over static for same key
                # Events have DNP3 timestamps and represent changes; static is just current value
                unsolicited_keys = set(unsolicited_snap.keys()) if unsolicited_snap else set()
                
                polled_data = {}
                
                # Start with static data, excluding keys already sent as unsolicited.
                # Without this filter, static Group30 data (with a fresh 2026 timestamp)
                # overwrites the unsolicited event we just sent, and ThingsBoard shows
                # the stale static value instead of the real-time unsolicited value.
                for key, value in static_snap.items():
                    if key not in unsolicited_keys:
                        polled_data[key] = value
                
                # Events overwrite static for same key, but skip if already sent as unsolicited
                for key, value in event_snap.items():
                    if key not in unsolicited_keys:
                        polled_data[key] = value  # Event overwrites static

                # If we got static data, a scheduled poll just completed
                # Mark the poll time for timing-based unsolicited detection
                if static_snap:
                    soe_handler.mark_poll_start()

                time_since_last = current_time - dev_info['last_static_send']
                if polled_data and time_since_last >= dev_info['check_interval']:
                    logger.info("=" * 80)
                    logger.info(f"  POLLED DATA for {device_name}")
                    logger.info(f"  Static: {len(static_snap)}  "
                                f"Events: {len(event_snap)}  "
                                f"Total: {len(polled_data)}")
                    logger.info("=" * 80)
                    try:
                        converted = rtu.uplink_converter.convert(rtu, polled_data)
                        if converted and (converted.telemetry_datapoints_count > 0
                                          or converted.attributes_datapoints_count > 0):
                            gateway_queue.put({
                                'device_name':    device_name,
                                'converted_data': converted,
                                'source':         'poll',
                            })
                            data_sends += 1
                            logger.info(
                                f"  ✅ POLLED queued: "
                                f"{converted.telemetry_datapoints_count} telemetry, "
                                f"{converted.attributes_datapoints_count} attributes"
                            )
                    except Exception as e:
                        logger.error(f"  ✗ Error converting polled data "
                                     f"for {device_name}: {e}", exc_info=True)
                    dev_info['last_static_send'] = current_time

            # Progress heartbeat every 5 minutes
            if current_time - last_progress >= 300:
                logger.info(
                    f"📈 Heartbeat | sends={data_sends} "
                    f"(unsolicited={unsol_sends}) | devices={len(device_data)}"
                )
                last_progress = current_time

            # 0.2 s keeps CPU near zero while still catching unsolicited
            # messages within ~200 ms. Lower this if you need faster response.
            time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info("Batch process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in monitoring loop: {e}", exc_info=True)
    finally:
        logger.info(
            f"=== Batch shutdown | sends={data_sends} "
            f"(unsolicited={unsol_sends}) ==="
        )
        try:
            manager.Shutdown()
        except Exception:
            pass
        logger.info("Batch process shutdown complete")

class Dnp3Connector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type

        self.__config = config
        self.__id = self.__config.get('id')
        self.name = config.get("name", 'dnp3 connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))

        self._log = init_logger(
            self.__gateway,
            self.name,
            self.__config.get('logLevel', 'INFO'),
            enable_remote_logging=self.__config.get('enableRemoteLogging', False),
            is_connector_logger=True
        )

        self._converter_log = init_logger(
            self.__gateway,
            self.name + "_converter",
            self.__config.get('logLevel', 'INFO'),
            enable_remote_logging=self.__config.get('enableRemoteLogging', False),
            is_connector_logger=True,
            attr_name=self.name
        )

        self.__devices = self.__config["devices"]
        self._master_id = self.__config.get("master_id", 2)
        self._master_ip = gethostbyname(self.__config["master_ip"])

        # Get profile directory from config
        connector_file_dir = os.path.dirname(os.path.abspath(__file__))
        self._profile_dir = self.__config.get(
            "profile_dir",
            connector_file_dir  #  Use actual connector directory
        )

        self.statistics = {
            'MessagesReceived': 0,
            'MessagesSent': 0
        }

        self.__methods = ["run", "g30v2", "show"]
        self.stop_event = multiprocessing.Event()
        self.processes = []
        self.gateway_queue = multiprocessing.Queue()

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        time.sleep(2)
        # Get the directory where this connector file is located
        connector_dir = os.path.dirname(os.path.abspath(__file__))  # Uses Python's __file__
        self.profile_dir = self.__config.get('profile_dir', connector_dir)
        self._log.info(f"===============================================")
        self._log.info(f"Profile directory: {self.profile_dir}")
        self._log.info(f"===============================================")

        # Add profile_dir to each device config
        for device in self.__devices:
            device['profile_dir'] = self._profile_dir

        batch_size = self.__config.get("batch_size", 20)
        devices = self.__devices
        batches = [devices[i:i + batch_size] for i in range(0, len(devices), batch_size)]

        self._log.info(f"Starting {len(batches)} batch processes with {len(devices)} total devices")

        for i, batch in enumerate(batches):
            self._log.info(f"Starting batch {i + 1}/{len(batches)} with {len(batch)} devices")
            p = multiprocessing.Process(
                target=process_batch,
                args=(batch, self._master_ip, self._master_id, self.stop_event, self.gateway_queue)
            )
            p.start()
            self.processes.append(p)

        self._connected = True

        # Main thread: process queue and send to gateway
        while not self.__stopped:
            try:
                # Check queue for data from batch processes
                if not self.gateway_queue.empty():
                    message = self.gateway_queue.get(timeout=1)

                    device_name    = message['device_name']
                    converted_data = message['converted_data']
                    source         = message.get('source', 'poll')  # 'poll' | 'unsolicited'

                    # Send to ThingsBoard - gateway expects device_name and ConvertedData
                    self.__gateway.send_to_storage(device_name, self.get_id(), converted_data)
                    
                    # Update statistics
                    self.statistics["MessagesReceived"] += 1
                    self.statistics["MessagesSent"] += 1
                    StatisticsService.count_connector_message(
                        self.name,
                        stat_parameter_name='connectorMsgsReceived'
                    )

                    if source == 'unsolicited':
                        self._log.info(
                            f"⚡ [UNSOLICITED] Sent to ThingsBoard for {device_name}: "
                            f"{converted_data.telemetry_datapoints_count} telemetry, "
                            f"{converted_data.attributes_datapoints_count} attributes"
                        )
                    else:
                        self._log.info(
                            f"✅ [POLL] Sent to ThingsBoard for {device_name}: "
                            f"{converted_data.telemetry_datapoints_count} telemetry, "
                            f"{converted_data.attributes_datapoints_count} attributes"
                        )

                else:
                    time.sleep(0.1)

            except Exception as e:
                self._log.error(f"Error processing queue: {str(e)}")
                import traceback
                self._log.error(traceback.format_exc())
                time.sleep(1)

        self._log.info("DNP3 Connector main loop ended")

    def collect_statistic_and_send(self, connector_name, connector_id, data):
        """Send data to ThingsBoard using gateway's send_to_storage method"""
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        try:
            self.__gateway.send_to_storage(connector_name, connector_id, data)
            self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

            # Update statistics
            StatisticsService.count_connector_message(
                self.name,
                stat_parameter_name='connectorMsgsReceived'
            )
        except Exception as e:
            self._log.error(f"Failed to send data to ThingsBoard for {connector_name}: {str(e)}")

    def close(self):
        self._log.info("Closing DNP3 Connector...")
        self.__stopped = True
        self.stop_event.set()

        for p in self.processes:
            p.join(timeout=5)
            if p.is_alive():
                self._log.warning(f"Process {p.pid} did not terminate, forcing...")
                p.terminate()

        self._connected = False
        self._log.info("DNP3 Connector closed")

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def on_attributes_update(self, content):
        """
        Handle remote configuration updates sent from ThingsBoard
        Expected shared attribute update format examples:
        {
        "device": "TCT000851",
        "polling_interval": 30000,
        "timeout": 10000,
        "profile": "DNP3Profile1.csv",
        "outstation_ip": "10.123.24.15",
        ...
        }
        or full device replacement:
        {
        "devices": [ { ... full device config ... } ]
        }
        """
        try:
            self._log.info("Received remote configuration update: %s", content)

            updated = False

            # Case 1: Update to a specific device
            if 'device' in content and 'data' in content:
                device_name = content['device']
                updates = content['data']

                for device in self.__devices:
                    if device.get('deviceName') == device_name:
                        self._log.info("Updating device: %s", device_name)

                        # Apply allowed fields
                        allowed_fields = [
                            'outstation_ip', 'port', 'outstation_id',
                            'polling_interval', 'timeout', 'max_retries', 'retry_delay',
                            'profile'
                        ]

                        for key, value in updates.items():
                            if key in allowed_fields:
                                old_value = device.get(key)
                                device[key] = value
                                self._log.info("  %s: %s â†’ %s", key, old_value, value)
                                updated = True
                            else:
                                self._log.warning("Ignoring non-allowed field: %s", key)

            # Case 2: Full replacement of devices list (more powerful but more dangerous)
            elif 'devices' in content:
                new_devices = content['devices']
                if isinstance(new_devices, list):
                    self._log.warning("Replacing entire devices list with new configuration")
                    self.__devices = new_devices
                    updated = True
                else:
                    self._log.error("Invalid 'devices' value received (not a list)")

            if updated:
                self._log.info("Configuration updated â€” restarting batches to apply changes")
                self.restart_batches()
            else:
                self._log.info("No meaningful configuration changes detected")

        except Exception as e:
            self._log.exception("Failed to process remote configuration update", exc_info=True)

    def _safe_rpc_reply(self, device_name, rpc_id, payload):
        """Send RPC reply if gateway supports it, otherwise log and move on."""
        try:
            if hasattr(self.__gateway, 'send_rpc_reply') and rpc_id is not None:
                self.__gateway.send_rpc_reply(device_name, rpc_id, payload)
                self._log.debug("RPC reply sent for %s: %s", device_name, payload)
            else:
                self._log.debug(
                    "RPC reply skipped (no send_rpc_reply or no rpc_id): %s", payload
                )
        except Exception as e:
            self._log.warning("Failed to send RPC reply: %s", e)

    def server_side_rpc_handler(self, content):
        """
        Handle RPC requests from ThingsBoard.

        Supported methods:
            trutalk_Get  – query the outstation via TruTalk ASCII protocol

        params.get controls WHAT to fetch:
            "attributes" or {} or ""  → full harvest: ?3 ?4 ?5 ?6
            "gps"                     → GPS only: ?6
            "identity"                → identity only: ?3 ?4 ?5
            "?3"                      → software version only
            "?4"                      → serial number only
            "?5"                      → manufacture date only
            "?6"                      → GPS / location only

        params.timeout (float, optional) – per-command TCP timeout (default 5s)

        ThingsBoard RPC button config:
            { "method": "trutalk_Get", "params": {} }
            { "method": "trutalk_Get", "params": {"get": "gps"} }
            { "method": "trutalk_Get", "params": {"get": "?6"} }

        ThingsBoard wraps this as:
            {
                "device": "TCT000851",
                "data": {
                    "id": 123,
                    "method": "rpcCommand",
                    "params": {"method": "trutalk_Get", "params": {"get": "gps"}}
                }
            }
        """
        try:
            self._log.info("RPC request received: %s", content)

            device_name = content.get('device')
            rpc_data    = content.get('data', {})
            rpc_id      = rpc_data.get('id') or content.get('id')
            method      = rpc_data.get('method', '')
            params      = rpc_data.get('params', {})

            # ThingsBoard wraps custom RPCs in an outer "rpcCommand" envelope
            if method == 'rpcCommand' and isinstance(params, dict) and 'method' in params:
                method = params.get('method', '')
                params = params.get('params', {})
                self._log.info("RPC: unwrapped rpcCommand → method=%s, params=%s", method, params)

            # ── Auto-detect raw TruTalk commands from terminal widget ─────
            # When the user types "?6" or "!256" in an RPC terminal widget,
            # ThingsBoard sends method="?6" with empty params.
            # Detect this and route through trutalk_Cmd automatically.
            if method.startswith('?') or method.startswith('!'):
                self._log.info("RPC: detected raw TruTalk command '%s' → routing to trutalk_Cmd", method)
                params = {'cmd': method}
                method = 'trutalk_Cmd'

            if method not in ('trutalk_Get', 'trutalk_Set', 'trutalk_Cmd'):
                self._log.warning("Unknown RPC method: %s", method)
                self._safe_rpc_reply(
                    device_name, rpc_id,
                    {"success": False, "error": f"Unknown method: {method}. Valid: trutalk_Get, trutalk_Set, trutalk_Cmd"}
                )
                return

            # ── Find the device config ────────────────────────────────────
            device_cfg = None
            for dev in self.__devices:
                if dev.get('deviceName') == device_name:
                    device_cfg = dev
                    break

            if device_cfg is None:
                self._log.error("RPC: device '%s' not found in config", device_name)
                self._safe_rpc_reply(
                    device_name, rpc_id,
                    {"success": False, "error": f"Device '{device_name}' not found"}
                )
                return

            if method == 'trutalk_Get':
                # ── trutalk_Get dispatch ──────────────────────────────────
                get_param = params.get('get', '') if isinstance(params, dict) else ''
                if isinstance(get_param, str):
                    get_param = get_param.strip().lower()

                self._safe_rpc_reply(
                    device_name, rpc_id,
                    {"success": True, "message": f"trutalk_Get({get_param or 'attributes'}) started for {device_name}"}
                )

                import threading
                t = threading.Thread(
                    target=self._rpc_trutalk_get_worker,
                    args=(device_name, device_cfg, get_param, params),
                    name=f"RPC-TruTalk-Get-{device_name}",
                    daemon=True,
                )
                t.start()
                self._log.info(
                    "RPC: trutalk_Get(%s) dispatched for %s",
                    get_param or 'attributes', device_name
                )

            elif method == 'trutalk_Set':
                # ── trutalk_Set dispatch ──────────────────────────────────
                # params format:
                #   {"values": {"voltageHighThreshold": 2650, "voltageLowThreshold": 1900}}
                # or from rule chain attribute update:
                #   {"values": {"anaOutput0": 2650, "anaOutput1": 1900}}
                values = params.get('values', {}) if isinstance(params, dict) else {}
                if not values:
                    self._log.warning("RPC: trutalk_Set with no values for %s", device_name)
                    self._safe_rpc_reply(
                        device_name, rpc_id,
                        {"success": False, "error": "No 'values' provided in params"}
                    )
                    return

                self._safe_rpc_reply(
                    device_name, rpc_id,
                    {"success": True, "message": f"trutalk_Set started for {device_name}: {list(values.keys())}"}
                )

                import threading
                t = threading.Thread(
                    target=self._rpc_trutalk_set_worker,
                    args=(device_name, device_cfg, values, params),
                    name=f"RPC-TruTalk-Set-{device_name}",
                    daemon=True,
                )
                t.start()
                self._log.info(
                    "RPC: trutalk_Set dispatched for %s: %s",
                    device_name, list(values.keys())
                )

            elif method == 'trutalk_Cmd':
                # ── trutalk_Cmd dispatch — raw command passthrough ────────
                # params format:
                #   {"cmd": "?6"}                     — single command (sync reply for terminal)
                #   {"cmd": "!256"}                   — single set command (sync reply)
                #   {"cmd": ["?120 0", "!120 0 2650", "?120 0"]}  — batch (async)
                #   {"cmd": "!49 300", "timeout": 10} — with custom timeout
                cmd_param = params.get('cmd', '') if isinstance(params, dict) else ''
                if not cmd_param:
                    self._log.warning("RPC: trutalk_Cmd with no 'cmd' for %s", device_name)
                    self._safe_rpc_reply(
                        device_name, rpc_id,
                        {"success": False, "error": "No 'cmd' provided in params. Example: {\"cmd\": \"?6\"}"}
                    )
                    return

                if isinstance(cmd_param, str):
                    # ── Single command: run synchronously so the terminal
                    #    widget gets the response in the RPC reply ──────────
                    self._log.info(
                        "RPC: trutalk_Cmd (sync) for %s: %s", device_name, cmd_param
                    )
                    result = self._rpc_trutalk_cmd_sync(
                        device_name, device_cfg, cmd_param, params
                    )
                    self._safe_rpc_reply(device_name, rpc_id, result)

                else:
                    # ── Multiple commands: run async ──────────────────────
                    self._safe_rpc_reply(
                        device_name, rpc_id,
                        {"success": True, "message": f"trutalk_Cmd started for {device_name}: {cmd_param}"}
                    )

                    import threading
                    t = threading.Thread(
                        target=self._rpc_trutalk_cmd_worker,
                        args=(device_name, device_cfg, cmd_param, params),
                        name=f"RPC-TruTalk-Cmd-{device_name}",
                        daemon=True,
                    )
                    t.start()
                    self._log.info(
                        "RPC: trutalk_Cmd (async) dispatched for %s: %s",
                        device_name, cmd_param
                    )

        except Exception as e:
            self._log.exception("RPC handler error: %s", e)

    # ── RPC TruTalk worker (runs in its own thread) ───────────────────────

    # Map shorthand names → list of TruTalk commands to send
    _TRUTALK_GET_PRESETS = {
        '':           ['?3', '?4', '?5', '?6'],   # default = full harvest
        'attributes': ['?3', '?4', '?5', '?6'],   # explicit full harvest
        'gps':        ['?6'],                       # GPS / location only
        'identity':   ['?3', '?4', '?5'],           # identity without GPS
        '?3':         ['?3'],                        # individual commands
        '?4':         ['?4'],
        '?5':         ['?5'],
        '?6':         ['?6'],
    }

    def _rpc_trutalk_get_worker(self, device_name, device_cfg, get_param, params):
        """
        Isolated worker that opens its OWN TCP connection to the outstation's
        TruTalk port, sends the requested commands, parses responses, builds
        ConvertedData, and pushes it directly to ThingsBoard as client attributes.

        Completely independent of the batch processes — no shared state, no
        SOE handler interaction, no locks on DNP3 data.
        """
        outstation_ip  = device_cfg.get('outstation_ip')
        serial_number  = device_cfg.get('trutalk_serial', device_name)
        trutalk_port   = device_cfg.get('trutalk_port', 8870)
        cmd_timeout    = params.get('timeout', 5.0) if isinstance(params, dict) else 5.0

        # Resolve which commands to run
        commands = self._TRUTALK_GET_PRESETS.get(get_param)
        if commands is None:
            self._log.error(
                "[RPC-TruTalk] Unknown get param: '%s'. "
                "Valid options: %s",
                get_param, list(self._TRUTALK_GET_PRESETS.keys())
            )
            return

        self._log.info(
            "[RPC-TruTalk] trutalk_Get(%s) → %s @ %s:%d (serial=%s, commands=%s)",
            get_param or 'attributes', device_name, outstation_ip,
            trutalk_port, serial_number, commands
        )

        try:
            attrs = self._trutalk_query_commands(
                outstation_ip, trutalk_port, serial_number, cmd_timeout, commands
            )

            if not attrs:
                self._log.warning(
                    "[RPC-TruTalk] No attributes returned for %s", device_name
                )
                return

            # ── Build ConvertedData with attributes only ──────────────────
            device_type = device_cfg.get('deviceType', 'dnp3 connector')
            converted_data = ConvertedData(
                device_name=device_name, device_type=device_type
            )

            for field, value in attrs.items():
                try:
                    datapoint_key = TBUtility.convert_key_to_datapoint_key(
                        field, None, {"key": field}, self._log
                    )
                    converted_data.add_to_attributes(datapoint_key, value)
                except Exception:
                    converted_data.add_to_attributes(field, value)

            self._log.info(
                "[RPC-TruTalk] Sending %d attributes to ThingsBoard for %s: %s",
                len(attrs), device_name, list(attrs.keys())
            )

            # ── Send directly to ThingsBoard ──────────────────────────────
            self.__gateway.send_to_storage(
                device_name, self.get_id(), converted_data
            )

            self._log.info(
                "[RPC-TruTalk] ✔ %d attributes sent for %s",
                len(attrs), device_name
            )

        except Exception as e:
            self._log.error(
                "[RPC-TruTalk] Worker failed for %s: %s",
                device_name, e, exc_info=True
            )

    # ── Unified TruTalk command sender ────────────────────────────────────

    def _trutalk_query_commands(self, host, port, serial_number, timeout, commands):
        """
        Open ONE TCP socket, send the requested TruTalk commands, parse each
        response, and return the merged attribute dict.

        Socket is ALWAYS closed in the finally block — outstation only
        allows one TCP session per port at a time.

        Args:
            host:          Outstation IP address.
            port:          TruTalk port (default 8870).
            serial_number: Device serial for command prefix.
            timeout:       Per-command TCP timeout in seconds.
            commands:      List of commands to send, e.g. ['?3', '?6'].

        Returns:
            Dict of attribute key → value.
        """


        _PARSERS = {
            '?3': _parse_q3,
            '?4': _parse_q4,
            '?5': _parse_q5,
            '?6': _parse_q6,
        }

        attrs = {}
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            self._log.info("[RPC-TruTalk] ✔ Connected to %s:%d", host, port)

            # Drain any greeting / stale data
            sock.settimeout(0.5)
            try:
                sock.recv(512)
            except socket.timeout:
                pass

            for cmd in commands:
                parser = _PARSERS.get(cmd)
                if parser is None:
                    self._log.warning("[RPC-TruTalk] No parser for command: %s", cmd)
                    continue

                try:
                    raw = self._trutalk_send_and_recv(sock, serial_number, cmd, timeout)
                    if raw:
                        parsed = parser(raw)
                        attrs.update(parsed)
                        self._log.info("[RPC-TruTalk] %s → %s", cmd, parsed)
                    else:
                        self._log.warning("[RPC-TruTalk] %s → empty response", cmd)
                    import time
                    time.sleep(0.1)   # small gap between commands
                except Exception as cmd_err:
                    self._log.warning("[RPC-TruTalk] %s failed: %s", cmd, cmd_err)

        except ConnectionRefusedError:
            self._log.warning(
                "[RPC-TruTalk] Port %d refused on %s — "
                "device may only allow 1 TCP session", port, host
            )
        except Exception as e:
            self._log.error("[RPC-TruTalk] Connection error: %s", e, exc_info=True)
        finally:
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                self._log.info(
                    "[RPC-TruTalk] ✔ Socket closed for %s:%d (port freed)",
                    host, port
                )

        return attrs

    def _trutalk_send_and_recv(self, sock, serial_number, cmd, timeout):
        """
        Send one TruTalk command on an existing socket and read the response.

        Single-line commands (?3, ?4, ?5) return after the first '$N:' line.
        Multi-line command (?6) returns after a blank line or read timeout.

        Returns the raw response string.
        """
        full_cmd = f"{serial_number} {cmd}\n"
        sock.sendall(full_cmd.encode())
        self._log.debug("[RPC-TruTalk] TX: %s", full_cmd.rstrip())

        sock.settimeout(timeout)
        buf = ""
        lines = []
        is_multiline = cmd.strip() == '?6'

        try:
            while True:
                chunk = sock.recv(4096).decode(errors='replace')
                if not chunk:
                    break
                buf += chunk
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line = line.rstrip('\r')
                    lines.append(line)
                    self._log.debug("[RPC-TruTalk] RX: %r", line)

                    if is_multiline and line.strip() == '':
                        return '\n'.join(lines)

                # Single-line: got $N response → small extra read then done
                if not is_multiline and lines and lines[0].startswith('$'):
                    sock.settimeout(0.3)
                    try:
                        extra = sock.recv(256).decode(errors='replace')
                        if extra:
                            buf += extra
                    except socket.timeout:
                        pass
                    break

        except socket.timeout:
            pass

        return '\n'.join(lines)

    # ══════════════════════════════════════════════════════════════════════
    # trutalk_Set — write configuration to outstation via TruTalk
    # ══════════════════════════════════════════════════════════════════════

    # V3.1 Analog Output Index → human-readable attribute name mapping.
    # Users can send either the attribute name or the raw index.
    _V31_ANALOG_OUTPUT_MAP = {
        # attr_name                        → (index, description, note)
        'voltageHighThreshold':              (0,  'Voltage high threshold',          'device value × 10'),
        'voltageLowThreshold':               (1,  'Voltage low threshold',           'device value × 10'),
        'voltageImbalanceThreshold':         (2,  'Voltage imbalance threshold',     ''),
        'currentHighThreshold':              (3,  'Current high threshold',          'device value × 10'),
        'neutralHighThreshold':              (4,  'Neutral/Earth high threshold',    ''),
        'temperatureHighThreshold':          (5,  'Temperature high threshold',      ''),
        'vibrationThreshold':                (6,  'Vibration Threshold',             ''),
        'vibrationTimeFilter':               (7,  'Vibration Time Filter',           ''),
        'vibrationIntensity':                (8,  'Vibration Intensity',             ''),
        'vibrationDebounce':                 (9,  'Vibration event debounce period', ''),
        'vibrationAlarmRest':                (10, 'Vibration alarm rest period',     ''),
    }

    # Reverse lookup: index → attr_name
    _V31_INDEX_TO_ATTR = {idx: name for name, (idx, _, _) in _V31_ANALOG_OUTPUT_MAP.items()}

    # Accept "anaOutput0"–"anaOutput10" aliases (from ThingsBoard analog output widget)
    _V31_ANA_OUTPUT_ALIASES = {
        f'anaOutput{idx}': name for name, (idx, _, _) in _V31_ANALOG_OUTPUT_MAP.items()
    }

    @staticmethod
    def _detect_trutalk_version(version_string):
        """
        Parse the ?3 response to determine the TruTalk protocol version.

        Examples:
            'V3.17b'  → 'V3.1'   (V3.1x firmware, minor >= 10)
            'V3.02'   → 'V3'     (V3.0x firmware, minor < 10)
            'V2.5'    → 'V2'
            'V1.3'    → 'V1'
            ''        → None

        Returns one of: 'V1', 'V2', 'V3', 'V3.1', or None.
        """
        import re
        if not version_string:
            return None
        v = version_string.strip()
        m = re.match(r'[Vv](\d+)\.(\d+)', v)
        if not m:
            return None
        major = int(m.group(1))
        minor = int(m.group(2))
        if major >= 3:
            return 'V3.1' if minor >= 10 else 'V3'
        elif major == 2:
            return 'V2'
        elif major == 1:
            return 'V1'
        return None

    def _rpc_trutalk_set_worker(self, device_name, device_cfg, values, params):
        """
        Isolated worker for trutalk_Set.

        Opens its own TCP connection, detects the device version via ?3,
        then sends the appropriate V3.1 set commands.
        After writing, reads back the values with ?120/?121 and pushes
        the verified results to ThingsBoard as client attributes.

        Completely independent of batch processes — no shared state.

        Args:
            device_name: ThingsBoard device name.
            device_cfg:  Device config dict.
            values:      {attr_name: value} to write.
                         e.g. {"voltageHighThreshold": 2650, "voltageLowThreshold": 1900}
            params:      Full RPC params (may contain 'timeout').
        """
        import socket
        import time as time_module
        from thingsboard_gateway.extensions.dnp3.trutalk_identity import _parse_q3

        outstation_ip = device_cfg.get('outstation_ip')
        serial_number = device_cfg.get('trutalk_serial', device_name)
        trutalk_port  = device_cfg.get('trutalk_port', 8870)
        cmd_timeout   = params.get('timeout', 5.0) if isinstance(params, dict) else 5.0

        self._log.info(
            "[RPC-TruTalk-Set] %s @ %s:%d (serial=%s, values=%s)",
            device_name, outstation_ip, trutalk_port, serial_number, values
        )

        sock = None
        results = {}

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(cmd_timeout)
            sock.connect((outstation_ip, trutalk_port))
            self._log.info(
                "[RPC-TruTalk-Set] ✔ Connected to %s:%d", outstation_ip, trutalk_port
            )

            # Drain greeting
            sock.settimeout(0.5)
            try:
                sock.recv(512)
            except socket.timeout:
                pass

            # ── Step 1: Detect version via ?3 ─────────────────────────────
            raw_q3 = self._trutalk_send_and_recv(
                sock, serial_number, '?3', cmd_timeout
            )
            version_str = ''
            if raw_q3:
                parsed_q3 = _parse_q3(raw_q3)
                version_str = parsed_q3.get('devSoftwareVersion', '')
            version = self._detect_trutalk_version(version_str)

            self._log.info(
                "[RPC-TruTalk-Set] Device version: %s (raw: '%s')",
                version or 'UNKNOWN', version_str
            )

            if version is None:
                self._log.error(
                    "[RPC-TruTalk-Set] Cannot detect version for %s — aborting",
                    device_name
                )
                return

            if version != 'V3.1':
                self._log.error(
                    "[RPC-TruTalk-Set] Version %s not yet supported for Set. "
                    "Only V3.1 is implemented. Device: %s", version, device_name
                )
                return

            time_module.sleep(0.1)

            # ── Step 2: Send set commands ─────────────────────────────────
            for attr_name, value in values.items():
                try:
                    result = self._trutalk_set_v31(
                        sock, serial_number, cmd_timeout, attr_name, value
                    )
                    results[attr_name] = result
                    time_module.sleep(0.1)
                except Exception as cmd_err:
                    self._log.error(
                        "[RPC-TruTalk-Set] Error setting %s=%s: %s",
                        attr_name, value, cmd_err
                    )
                    results[attr_name] = {'success': False, 'detail': str(cmd_err)}

            # ── Step 3: Read back to verify ───────────────────────────────
            self._log.info("[RPC-TruTalk-Set] Reading back values to verify...")
            verify_attrs = {}
            for attr_name in values:
                try:
                    readback = self._trutalk_readback_v31(
                        sock, serial_number, cmd_timeout, attr_name
                    )
                    if readback is not None:
                        verify_attrs[attr_name] = readback
                        self._log.info(
                            "[RPC-TruTalk-Set] ✔ Verify %s = %s", attr_name, readback
                        )
                    time_module.sleep(0.1)
                except Exception as e:
                    self._log.warning(
                        "[RPC-TruTalk-Set] Verify failed for %s: %s", attr_name, e
                    )

            # ── Step 4: Push verified values to ThingsBoard ───────────────
            if verify_attrs:
                device_type = device_cfg.get('deviceType', 'dnp3 connector')
                converted_data = ConvertedData(
                    device_name=device_name, device_type=device_type
                )
                for field, val in verify_attrs.items():
                    try:
                        dpk = TBUtility.convert_key_to_datapoint_key(
                            field, None, {"key": field}, self._log
                        )
                        converted_data.add_to_attributes(dpk, val)
                    except Exception:
                        converted_data.add_to_attributes(field, val)

                self.__gateway.send_to_storage(
                    device_name, self.get_id(), converted_data
                )
                self._log.info(
                    "[RPC-TruTalk-Set] ✔ %d verified attributes pushed to TB for %s",
                    len(verify_attrs), device_name
                )

            # Summary
            ok = sum(1 for r in results.values() if r.get('success'))
            self._log.info(
                "[RPC-TruTalk-Set] Complete for %s: %d/%d succeeded",
                device_name, ok, len(results)
            )

        except ConnectionRefusedError:
            self._log.warning(
                "[RPC-TruTalk-Set] Port %d refused on %s",
                trutalk_port, outstation_ip
            )
        except Exception as e:
            self._log.error(
                "[RPC-TruTalk-Set] Worker failed for %s: %s",
                device_name, e, exc_info=True
            )
        finally:
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                self._log.info(
                    "[RPC-TruTalk-Set] ✔ Socket closed for %s:%d (port freed)",
                    outstation_ip, trutalk_port
                )

    def _resolve_v31_analog_index(self, attr_name):
        """
        Resolve an attribute name to its V3.1 analog output index.

        Accepts:
            'voltageHighThreshold'  → index 0
            'anaOutput0'            → index 0
            '0' or 0                → index 0

        Returns (index, canonical_attr_name) or (None, None).
        """
        if attr_name in self._V31_ANALOG_OUTPUT_MAP:
            return self._V31_ANALOG_OUTPUT_MAP[attr_name][0], attr_name

        if attr_name in self._V31_ANA_OUTPUT_ALIASES:
            canonical = self._V31_ANA_OUTPUT_ALIASES[attr_name]
            return self._V31_ANALOG_OUTPUT_MAP[canonical][0], canonical

        try:
            idx = int(attr_name)
            if idx in self._V31_INDEX_TO_ATTR:
                return idx, self._V31_INDEX_TO_ATTR[idx]
        except (ValueError, TypeError):
            pass

        return None, None

    def _trutalk_set_v31(self, sock, serial_number, timeout, attr_name, value):
        """
        Send a V3.1 set command for one attribute.

        !120 <index> <value>  — analog output threshold
        !121 <index> <value>  — analog deadband

        Deadband attributes are identified by a 'Deadband' suffix:
            'voltageHighThresholdDeadband' → !121 0 <value>

        Returns {'success': bool, 'detail': str, 'response': str}.
        """
        import socket as _socket

        is_deadband = attr_name.endswith('Deadband')
        lookup_name = attr_name[:-8] if is_deadband else attr_name

        index, canonical = self._resolve_v31_analog_index(lookup_name)
        if index is None:
            msg = (
                f"Unknown attribute '{attr_name}' — "
                f"not in V3.1 analog output map. "
                f"Valid: {list(self._V31_ANALOG_OUTPUT_MAP.keys())}"
            )
            self._log.warning("[RPC-TruTalk-Set] %s", msg)
            return {'success': False, 'detail': msg}

        cmd_prefix = '!121' if is_deadband else '!120'
        try:
            int_value = int(round(float(value)))
        except (ValueError, TypeError) as e:
            msg = f"Invalid value '{value}' for {attr_name}: {e}"
            self._log.error("[RPC-TruTalk-Set] %s", msg)
            return {'success': False, 'detail': msg}

        trutalk_cmd = f"{cmd_prefix} {index} {int_value}"
        self._log.info(
            "[RPC-TruTalk-Set] TX: %s (attr=%s, index=%d, deadband=%s)",
            trutalk_cmd, attr_name, index, is_deadband
        )

        full_cmd = f"{serial_number} {trutalk_cmd}\n"
        sock.sendall(full_cmd.encode())

        # Set commands may return a short ack or nothing
        sock.settimeout(min(timeout, 2.0))
        response = ''
        try:
            chunk = sock.recv(4096).decode(errors='replace')
            response = chunk.strip()
            self._log.debug("[RPC-TruTalk-Set] RX: %r", response)
        except _socket.timeout:
            pass

        return {'success': True, 'detail': f"Sent {trutalk_cmd}", 'response': response}

    def _trutalk_readback_v31(self, sock, serial_number, timeout, attr_name):
        """
        Read back a V3.1 value with ?120 <index> or ?121 <index>.

        Returns the parsed numeric value, or None on failure.
        """
        import re

        is_deadband = attr_name.endswith('Deadband')
        lookup_name = attr_name[:-8] if is_deadband else attr_name

        index, _ = self._resolve_v31_analog_index(lookup_name)
        if index is None:
            return None

        query_prefix = '?121' if is_deadband else '?120'
        trutalk_cmd = f"{query_prefix} {index}"

        raw = self._trutalk_send_and_recv(sock, serial_number, trutalk_cmd, timeout)
        if not raw:
            return None

        numbers = re.findall(r'[-+]?\d+(?:\.\d+)?', raw)
        if numbers:
            try:
                return float(numbers[-1]) if '.' in numbers[-1] else int(numbers[-1])
            except (ValueError, TypeError):
                pass

        self._log.warning(
            "[RPC-TruTalk-Set] Could not parse readback for %s: %r", attr_name, raw
        )
        return None

    # ══════════════════════════════════════════════════════════════════════
    # trutalk_Cmd — raw TruTalk command passthrough
    # ══════════════════════════════════════════════════════════════════════

    def _rpc_trutalk_cmd_sync(self, device_name, device_cfg, cmd, params):
        """
        Execute a SINGLE TruTalk command synchronously and return the result
        dict directly (for the RPC reply).

        Used by the terminal widget so the user sees the response inline.
        Opens and closes its own TCP socket.

        Returns dict: {"success": bool, "response": str} or {"success": false, "error": str}
        """
        import socket
        import time as time_module

        outstation_ip = device_cfg.get('outstation_ip')
        serial_number = device_cfg.get('trutalk_serial', device_name)
        trutalk_port  = device_cfg.get('trutalk_port', 8870)
        cmd_timeout   = params.get('timeout', 5.0) if isinstance(params, dict) else 5.0

        self._log.info(
            "[RPC-TruTalk-Cmd] sync: %s → %s @ %s:%d",
            cmd, device_name, outstation_ip, trutalk_port
        )

        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(cmd_timeout)
            sock.connect((outstation_ip, trutalk_port))
            self._log.info("[RPC-TruTalk-Cmd] ✔ Connected to %s:%d", outstation_ip, trutalk_port)

            # Drain greeting
            sock.settimeout(0.5)
            try:
                sock.recv(512)
            except socket.timeout:
                pass

            raw_response = self._trutalk_raw_send_recv(
                sock, serial_number, cmd.strip(), cmd_timeout
            )

            self._log.info("[RPC-TruTalk-Cmd] %s → %r", cmd, raw_response)

            # Also push query results to ThingsBoard as client attributes
            if cmd.strip().startswith('?') and raw_response:
                try:
                    attr_key = 'trutalk_' + cmd.strip().replace('?', 'q').replace(' ', '_')
                    device_type = device_cfg.get('deviceType', 'dnp3 connector')
                    converted_data = ConvertedData(
                        device_name=device_name, device_type=device_type
                    )
                    try:
                        dpk = TBUtility.convert_key_to_datapoint_key(
                            attr_key, None, {"key": attr_key}, self._log
                        )
                        converted_data.add_to_attributes(dpk, raw_response)
                    except Exception:
                        converted_data.add_to_attributes(attr_key, raw_response)
                    self.__gateway.send_to_storage(
                        device_name, self.get_id(), converted_data
                    )
                except Exception as attr_err:
                    self._log.warning(
                        "[RPC-TruTalk-Cmd] Failed to push attribute: %s", attr_err
                    )

            return {"success": True, "response": raw_response or "(no response)"}

        except ConnectionRefusedError:
            msg = f"Port {trutalk_port} refused on {outstation_ip}"
            self._log.warning("[RPC-TruTalk-Cmd] %s", msg)
            return {"success": False, "error": msg}
        except socket.timeout:
            msg = f"Connection timed out to {outstation_ip}:{trutalk_port}"
            self._log.warning("[RPC-TruTalk-Cmd] %s", msg)
            return {"success": False, "error": msg}
        except Exception as e:
            self._log.error(
                "[RPC-TruTalk-Cmd] Sync failed for %s: %s",
                device_name, e, exc_info=True
            )
            return {"success": False, "error": str(e)}
        finally:
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                self._log.info(
                    "[RPC-TruTalk-Cmd] ✔ Socket closed for %s:%d (port freed)",
                    outstation_ip, trutalk_port
                )

    def _rpc_trutalk_cmd_worker(self, device_name, device_cfg, cmd_param, params):
        """
        Raw TruTalk command passthrough.  Opens its own TCP socket, sends
        one or more commands exactly as provided, and logs the raw responses.

        Accepts:
            cmd_param (str):  Single command, e.g. "?6", "!256", "!120 0 2650"
            cmd_param (list): Multiple commands in sequence on one socket,
                              e.g. ["?120 0", "!120 0 2650", "?120 0"]

        Query results (?commands) are pushed to ThingsBoard as client
        attributes so the user can see them on the device entity.

        Completely independent of batch processes.
        """
        import socket
        import time as time_module

        outstation_ip = device_cfg.get('outstation_ip')
        serial_number = device_cfg.get('trutalk_serial', device_name)
        trutalk_port  = device_cfg.get('trutalk_port', 8870)
        cmd_timeout   = params.get('timeout', 5.0) if isinstance(params, dict) else 5.0

        # Normalise to list
        if isinstance(cmd_param, str):
            commands = [cmd_param]
        elif isinstance(cmd_param, list):
            commands = cmd_param
        else:
            self._log.error("[RPC-TruTalk-Cmd] Invalid cmd type: %s", type(cmd_param))
            return

        self._log.info(
            "[RPC-TruTalk-Cmd] %s @ %s:%d (serial=%s, commands=%s)",
            device_name, outstation_ip, trutalk_port, serial_number, commands
        )

        sock = None
        results = []

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(cmd_timeout)
            sock.connect((outstation_ip, trutalk_port))
            self._log.info("[RPC-TruTalk-Cmd] ✔ Connected to %s:%d", outstation_ip, trutalk_port)

            # Drain greeting
            sock.settimeout(0.5)
            try:
                sock.recv(512)
            except socket.timeout:
                pass

            for cmd in commands:
                cmd = cmd.strip()
                if not cmd:
                    continue

                self._log.info("[RPC-TruTalk-Cmd] Sending: %s", cmd)
                try:
                    raw_response = self._trutalk_raw_send_recv(
                        sock, serial_number, cmd, cmd_timeout
                    )
                    results.append({
                        'cmd': cmd,
                        'response': raw_response,
                        'success': True,
                    })
                    self._log.info(
                        "[RPC-TruTalk-Cmd] %s → %r", cmd, raw_response
                    )
                    time_module.sleep(0.1)

                except Exception as cmd_err:
                    results.append({
                        'cmd': cmd,
                        'response': str(cmd_err),
                        'success': False,
                    })
                    self._log.error(
                        "[RPC-TruTalk-Cmd] %s failed: %s", cmd, cmd_err
                    )

            # ── Push query results as client attributes ───────────────────
            attrs = {}
            for r in results:
                if r['success'] and r['cmd'].startswith('?') and r['response']:
                    # Use the command as the attribute key, sanitised
                    # ?120 0 → trutalk_q120_0, ?6 → trutalk_q6
                    attr_key = 'trutalk_' + r['cmd'].replace('?', 'q').replace(' ', '_')
                    attrs[attr_key] = r['response']

            if attrs:
                device_type = device_cfg.get('deviceType', 'dnp3 connector')
                converted_data = ConvertedData(
                    device_name=device_name, device_type=device_type
                )
                for field, value in attrs.items():
                    try:
                        dpk = TBUtility.convert_key_to_datapoint_key(
                            field, None, {"key": field}, self._log
                        )
                        converted_data.add_to_attributes(dpk, value)
                    except Exception:
                        converted_data.add_to_attributes(field, value)

                self.__gateway.send_to_storage(
                    device_name, self.get_id(), converted_data
                )
                self._log.info(
                    "[RPC-TruTalk-Cmd] ✔ %d query results pushed to TB for %s: %s",
                    len(attrs), device_name, list(attrs.keys())
                )

            # Summary
            ok = sum(1 for r in results if r['success'])
            self._log.info(
                "[RPC-TruTalk-Cmd] Complete for %s: %d/%d commands succeeded",
                device_name, ok, len(results)
            )
            for r in results:
                self._log.info(
                    "[RPC-TruTalk-Cmd]   %s %s → %r",
                    "✔" if r['success'] else "✗", r['cmd'], r['response']
                )

        except ConnectionRefusedError:
            self._log.warning(
                "[RPC-TruTalk-Cmd] Port %d refused on %s",
                trutalk_port, outstation_ip
            )
        except Exception as e:
            self._log.error(
                "[RPC-TruTalk-Cmd] Worker failed for %s: %s",
                device_name, e, exc_info=True
            )
        finally:
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
                self._log.info(
                    "[RPC-TruTalk-Cmd] ✔ Socket closed for %s:%d (port freed)",
                    outstation_ip, trutalk_port
                )

    def _trutalk_raw_send_recv(self, sock, serial_number, cmd, timeout):
        """
        Send a raw TruTalk command and read whatever comes back.

        Unlike _trutalk_send_and_recv (which has specific handling for
        ?3-?6 query responses), this method works with ANY command:
        - ? queries: reads until $ response or timeout
        - ! set commands: reads short ack/echo or timeout
        - Multi-line responses: reads until blank line

        Returns the raw response string.
        """
        import socket as _socket

        full_cmd = f"{serial_number} {cmd}\n"
        sock.sendall(full_cmd.encode())
        self._log.debug("[RPC-TruTalk-Cmd] TX: %s", full_cmd.rstrip())

        sock.settimeout(timeout)
        buf = ""
        lines = []

        try:
            while True:
                chunk = sock.recv(4096).decode(errors='replace')
                if not chunk:
                    break
                buf += chunk
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line = line.rstrip('\r')
                    lines.append(line)
                    self._log.debug("[RPC-TruTalk-Cmd] RX: %r", line)

                    # Blank line after content = end of multi-line response
                    if line.strip() == '' and len(lines) > 1:
                        return '\n'.join(lines).strip()

                # Single-line $ response (query result)
                if lines and lines[0].startswith('$'):
                    sock.settimeout(0.3)
                    try:
                        extra = sock.recv(256).decode(errors='replace')
                        if extra:
                            buf += extra
                    except _socket.timeout:
                        pass
                    break

                # ! set commands may return a short ack or nothing
                if cmd.strip().startswith('!') and lines:
                    sock.settimeout(0.5)
                    try:
                        extra = sock.recv(256).decode(errors='replace')
                        if extra:
                            buf += extra
                            while '\n' in buf:
                                line, buf = buf.split('\n', 1)
                                lines.append(line.rstrip('\r'))
                    except _socket.timeout:
                        pass
                    break

        except _socket.timeout:
            pass

        return '\n'.join(lines).strip()