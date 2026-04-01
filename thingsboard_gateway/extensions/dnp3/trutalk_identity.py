"""
trutalk_identity.py
───────────────────
Harvests device-identity attributes from the CVMS outstation via the
TruTalk ASCII protocol on port 8870, completely independently of the
DNP3 connection on port 20000.

Called once at gateway startup (before the DNP3 master initialises) and
then once every 24 hours from a background thread.  Results are injected
directly into OutstationSOEProxy.static_data so the normal converter
pipeline picks them up as ThingsBoard client attributes on the next poll.

Real device response format (observed):
    $3 : V3.17b
    $4 : TCT000851
    $5 : 10_2017
    $6 :
    V (238 237 237)
    A (0 0 0 8)
    P (0 0 0)
    S (81%)
    G (0.000000 0.000000 0)

All commands must be prefixed with the device serial number:
    "<serial_number> ?3\\n"
"""

import re
import socket
import logging
import threading
import time
from typing import Dict, Optional, Tuple, Any

# ── Constants ─────────────────────────────────────────────────────────────────

TRUTALK_PORT    = 8870
CONNECT_TIMEOUT = 5.0    # seconds to establish TCP connection
CMD_TIMEOUT     = 5.0    # seconds to wait for a response
DAILY_INTERVAL  = 86400  # seconds between scheduled refreshes

# Map TruTalk-derived values → ThingsBoard attribute key names.
# These field names all start with "dev" so the uplink converter
# automatically routes them to add_to_attributes() (never telemetry).
_FIELD_MAP = {
    "devSoftwareVersion":  None,   # filled from ?3
    "devSerialNumber":     None,   # filled from ?4
    "devManufactureDate":  None,   # filled from ?5  ($5 value = manufacture date)
    "devLocationLatitude": None,   # filled from ?6  G field
    "devLocationLongitude":None,   # filled from ?6  G field
    "devGPSSatellites":    None,   # filled from ?6  G field (bonus — useful)
}


# ── Parser helpers ────────────────────────────────────────────────────────────

def _strip_response_value(line: str) -> str:
    """Remove the '$N :' prefix and strip whitespace."""
    # Handles both "$3: value" and "$3 : value" (real device adds space)
    return re.sub(r'^\$\d+\s*:\s*', '', line).strip()


def _parse_q3(response: str) -> Dict[str, Any]:
    """$3 : V3.17b  →  {devSoftwareVersion: 'V3.17b'}"""
    val = _strip_response_value(response.strip())
    return {"devSoftwareVersion": val} if val else {}


def _parse_q4(response: str) -> Dict[str, Any]:
    """$4 : TCT000851  →  {devSerialNumber: 'TCT000851'}"""
    val = _strip_response_value(response.strip())
    return {"devSerialNumber": val} if val else {}


def _parse_q5(response: str) -> Dict[str, Any]:
    """
    $5 : 10_2017  →  {devManufactureDate: '10_2017'}

    Older V3.0 firmware returns only a manufacture date string.
    Newer V3.1 simulator returns Model=... SN=... FW=... HW=... MFG=...
    We handle both.
    """
    val = _strip_response_value(response.strip())
    if not val:
        return {}
    # V3.1 format: Model=CVMS-V3.1 SN=1000 FW=V3.1.4 HW=HW2.3 MFG=2023-06-15
    attrs = {}
    if '=' in val:
        for part in val.split():
            if '=' in part:
                k, v = part.split('=', 1)
                key_map = {
                    'Model': 'devProductName',
                    'FW':    'devSoftwareVersion',
                    'HW':    'devHardwareVersion',
                    'MFG':   'devManufactureDate',
                }
                if k in key_map:
                    attrs[key_map[k]] = v
    else:
        # V3.0: bare manufacture date string e.g. "10_2017"
        attrs['devManufactureDate'] = val
    return attrs


def _parse_q6(response: str) -> Dict[str, Any]:
    """
    Parse ?6 multi-line response for GPS coordinates.

    Expected lines (somewhere in the response):
        G (0.000000 0.000000 0)
        G (-25.847956 28.204994 5)

    Returns devLocationLatitude, devLocationLongitude, devGPSSatellites.
    Only populates coordinates if GPS has a lock (satellites > 0).
    """
    attrs = {}
    for line in response.splitlines():
        m = re.search(
            r'G\s*\(\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(\d+)\s*\)',
            line
        )
        if m:
            lat  = float(m.group(1))
            lon  = float(m.group(2))
            sats = int(m.group(3))
            attrs['devGPSSatellites'] = sats
            if sats >= 0:
                attrs['devLocationLatitude']  = lat
                attrs['devLocationLongitude'] = lon

            # Possibly omit lat/lon when sats == 0 to avoid storing
            # 0.000000 / 0.000000 as the device location.

            break
    return attrs


# ── Core harvester ────────────────────────────────────────────────────────────

class TruTalkIdentityHarvester:
    """
    Opens a short TCP session to the TruTalk port (8870), sends identity
    commands, parses responses, and injects results into the SOE handler's
    static_data dict so the normal DNP3 converter pipeline sends them to
    ThingsBoard as client attributes.

    Usage
    -----
        harvester = TruTalkIdentityHarvester(
            host          = "10.123.24.10",
            serial_number = "TCT000851",
            outstation_id = 2,
            soe_handler   = soe_handler,   # OutstationSOEProxy instance
            logger        = logger,
        )
        harvester.harvest()          # blocking, call once at startup
        harvester.start_daily()      # then kick off background 24h refresh
    """

    def __init__(self,
                 host:          str,
                 serial_number: str,
                 outstation_id: int,
                 soe_handler,                   # OutstationSOEProxy
                 logger:        logging.Logger,
                 port:          int = TRUTALK_PORT):
        self.host          = host
        self.port          = port
        self.serial_number = serial_number
        self.outstation_id = outstation_id
        self.soe_handler   = soe_handler
        self.logger        = logger
        self._stop         = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── Low-level send/receive ────────────────────────────────────────────────

    def _send_command(self, sock: socket.socket, cmd: str) -> str:
        """
        Send one TruTalk command and read until the response is complete.

        The protocol is line-oriented.  Single-line responses end after the
        first non-empty line.  Multi-line responses (?6) end with a blank line
        or a 0.5 s read timeout.
        """
        full_cmd = f"{self.serial_number} {cmd}\n"
        sock.sendall(full_cmd.encode())
        self.logger.debug(f"[TruTalk] TX: {full_cmd.rstrip()}")

        sock.settimeout(CMD_TIMEOUT)
        buf    = ""
        lines  = []
        blank_seen = False

        try:
            while True:
                chunk = sock.recv(4096).decode(errors='replace')
                if not chunk:
                    break
                buf += chunk
                # Process complete lines
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line = line.rstrip('\r')
                    lines.append(line)
                    self.logger.debug(f"[TruTalk] RX line: {line!r}")
                    if line == '':
                        blank_seen = True

                # Stop conditions:
                # ?3/?4/?5 return one $N: line then go quiet
                # ?6 returns several lines then a blank line
                if lines:
                    first = lines[0]
                    is_multiline = cmd.strip().startswith('?6')
                    if is_multiline and blank_seen:
                        break
                    if not is_multiline and first.startswith('$'):
                        # Got the response line — small extra read to catch
                        # any trailing \r\n then stop
                        sock.settimeout(0.3)
                        try:
                            extra = sock.recv(256).decode(errors='replace')
                            if extra:
                                buf += extra
                        except socket.timeout:
                            pass
                        break
        except socket.timeout:
            pass   # normal end of response

        result = '\n'.join(lines)
        return result

    # ── Harvest ───────────────────────────────────────────────────────────────

    def harvest(self) -> Dict[str, Any]:
        """
        Connect to TruTalk port, query identity commands, return attribute dict.
        Also injects results directly into soe_handler.static_data.
        """
        self.logger.info(
            f"[TruTalk] Starting identity harvest for {self.serial_number} "
            f"@ {self.host}:{self.port}"
        )
        attrs: Dict[str, Any] = {}

        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(CONNECT_TIMEOUT)
            sock.connect((self.host, self.port))
            self.logger.info(f"[TruTalk] ✔ Connected to {self.host}:{self.port}")

            # Small drain — device may push a greeting or stale unsolicited
            sock.settimeout(0.5)
            try:
                greeting = sock.recv(512)
                if greeting:
                    self.logger.debug(
                        f"[TruTalk] Drained {len(greeting)}B greeting/startup data"
                    )
            except socket.timeout:
                pass
            sock.settimeout(CMD_TIMEOUT)

            commands = [
                ("?3", _parse_q3),
                ("?4", _parse_q4),
                ("?5", _parse_q5),
                ("?6", _parse_q6),
            ]

            for cmd, parser in commands:
                try:
                    raw = self._send_command(sock, cmd)
                    if raw:
                        parsed = parser(raw)
                        attrs.update(parsed)
                        self.logger.info(
                            f"[TruTalk] {cmd} → {parsed}"
                        )
                    else:
                        self.logger.warning(
                            f"[TruTalk] {cmd} → empty response"
                        )
                    time.sleep(0.1)   # small gap between commands
                except Exception as cmd_err:
                    self.logger.warning(
                        f"[TruTalk] {cmd} failed: {cmd_err}"
                    )

        except ConnectionRefusedError:
            self.logger.warning(
                f"[TruTalk] Port {self.port} refused — "
                f"DNP3 may be holding the only session. "
                f"Will retry on next scheduled refresh."
            )
            return attrs
        except Exception as e:
            self.logger.error(f"[TruTalk] Harvest error: {e}", exc_info=True)
            return attrs
        finally:
            # ALWAYS close socket — outstation only allows 1 TCP session per port
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass   # already disconnected / not connected
                try:
                    sock.close()
                except OSError:
                    pass
                self.logger.info(
                    f"[TruTalk] ✔ Socket closed for {self.host}:{self.port} "
                    f"(port freed for next connection)"
                )

        # ── Inject into SOE handler static_data ──────────────────────────────
        if attrs:
            injected = 0
            for field, value in attrs.items():
                key = (self.outstation_id, field)
                self.soe_handler.static_data[key] = (value, None)
                injected += 1
                self.logger.info(
                    f"[TruTalk] ✔ Injected attribute: {field} = {value!r}"
                )
            self.logger.info(
                f"[TruTalk] ══ Harvest complete: {injected} attributes "
                f"injected into static_data for outstation {self.outstation_id} ══"
            )
        else:
            self.logger.warning(
                "[TruTalk] Harvest returned no attributes — "
                "device may not support TruTalk identity commands."
            )

        return attrs

    # ── Daily background refresh ──────────────────────────────────────────────

    def _refresh_loop(self):
        """Background thread: sleep 24 h, then harvest again."""
        self.logger.info(
            f"[TruTalk] Daily refresh thread started "
            f"(interval={DAILY_INTERVAL}s)"
        )
        while not self._stop.wait(timeout=DAILY_INTERVAL):
            self.logger.info("[TruTalk] Daily refresh triggered")
            try:
                self.harvest()
            except Exception as e:
                self.logger.error(
                    f"[TruTalk] Daily refresh failed: {e}", exc_info=True
                )
        self.logger.info("[TruTalk] Daily refresh thread stopped")

    def start_daily(self):
        """Start the 24-hour background refresh thread (daemon — dies with process)."""
        self._thread = threading.Thread(
            target=self._refresh_loop,
            name=f"TruTalk-refresh-{self.serial_number}",
            daemon=True
        )
        self._thread.start()
        self.logger.info(
            f"[TruTalk] Daily refresh scheduled every {DAILY_INTERVAL}s"
        )

    def stop(self):
        """Signal the refresh thread to stop."""
        self._stop.set()