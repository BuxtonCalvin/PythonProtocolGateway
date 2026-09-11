# Description: scraper for Modbus UDP devices, inheriting from modbus_base and implementing UDP-specific client setup and register access logic.
# File: modbus_udp.py
#
# Copyright 2026 Kevin Burke
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://apache.org
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from threading import RLock

# scraper for Modbus UDP devices, inheriting from modbus_base and implementing UDP-specific client setup and register access logic.
from typing import Any

from pymodbus.client.base import ModbusBaseSyncClient
from pymodbus.client.udp import ModbusUdpClient
from pymodbus.pdu import ExceptionResponse, ModbusPDU

from classes.protocol_settings import Registry_Type
from defs.common import TransportSettings

from .modbus_base import modbus_base


class modbus_udp(modbus_base):


    transport_type: str = "scraper"
    def __init__(self, settings : TransportSettings) -> None:
        super().__init__(settings)

        self.host = settings.get("host", fallback="")
        if not self.host:
            raise ValueError("Host is not set")

        self.port = settings.getint("port", fallback=502)

        client_str: str = self.host+"-udp-"+str(self.port)
        #check if client is already initialized
        with self._clients_lock:
            if client_str in modbus_base.clients:
                self.client = modbus_base.clients[client_str]
                return
        self.timeout: int = settings.getint("timeout", fallback=7)
        self.retries: int = settings.getint("retries", fallback=3)

        # Concrete pymodbus client classes (ModbusUdpClient etc.) are already
        # ModbusBaseSyncClient subtypes, so no cast is needed for assignment
        # to self.client (declared as Optional[ModbusBaseSyncClient] on the
        # base class). The local sync_client alias in read_registers below
        # gives static analysis the concrete Generic[ModbusPDU] binding for
        # response types.
        self.client = ModbusUdpClient(host=self.host, port=self.port, timeout=self.timeout, retries=self.retries)

        #add to clients
        with self._clients_lock:
            modbus_base.clients[client_str] = self.client

    def read_registers(self, start: int, count: int = 1, registry_type: Registry_Type = Registry_Type.INPUT, device_id: int | None = None) -> ModbusPDU | None:
        # read_registers method to handle retries and prevent "fire and forget" failures
        if self.client is None:
            self._log.error("read_registers called before client was initialized")
            return None

        sync_client: ModbusBaseSyncClient = self.client
        resolved_device_id: int = self._get_device_id(device_id)
        port_lock: RLock = self._get_port_lock()

        with port_lock:
        # Try the operation up to 'retries' times
            for attempt in range(self.retries):
                try:
                    if registry_type == Registry_Type.INPUT:
                        response: ModbusPDU = sync_client.read_input_registers(start, count=count, device_id=resolved_device_id)
                    elif registry_type == Registry_Type.HOLDING:
                        response = sync_client.read_holding_registers(start, count=count, device_id=resolved_device_id)
                    elif registry_type == Registry_Type.COIL:
                        response = sync_client.read_coils(start, count=count, device_id=resolved_device_id)
                    elif registry_type == Registry_Type.DISCRETE:
                        response = sync_client.read_discrete_inputs(start, count=count, device_id=resolved_device_id)
                    else:
                        self._log.warning(
                            f"read_registers: unsupported registry_type '{registry_type.name}' for UDP transport — returning None"
                        )
                        return None

                    # pymodbus v3+: sync clients return ModbusPDU directly.
                    # Error responses are ExceptionResponse instances, not flagged via isError().
                    if not isinstance(response, ExceptionResponse):
                        return response

                    self._log.warning(f"Modbus UDP Attempt {attempt + 1} failed: {response}")

                except Exception as e:
                    self._log.error(f"Network error on attempt {attempt + 1}: {e}")

            # If the loop finishes without returning, all retries failed
            self._log.error(f"Failed to read {registry_type} after {self.retries} attempts.")

    def _drain_connection_priming_bytes(self) -> None:
        """
        Some Modbus-TCP bridges (observed on a Waveshare RS485-to-TCP unit)
        send a one-time, unsolicited banner immediately after a client
        attaches — before any request has been sent — seemingly a
        network-discovery announcement (its bytes matched the device's own
        MAC address in one observed case). It lands in-band on the same TCP
        stream as normal Modbus traffic.

        Modbus TCP is strictly request/response with no concept of an
        unsolicited message, so pymodbus's framer has no way to recognize
        this for what it is — it tries to parse the leading bytes as an
        MBAP header, gets nonsense (a bogus protocol id, "very short frame"
        warnings), and desyncs: the real response behind it doesn't get
        matched up until the framer accumulates and re-parses enough bytes,
        which can silently eat several seconds on the very first request of
        a fresh connection. Worse, if that first request happens to be a
        detection probe (see eg4_metadata.detect_eg4_hardware_kind, which
        reads exactly one register to tell an EG4 battery apart from an
        inverter), a corrupted/failed read there doesn't just cost time —
        it can produce a wrong answer that cascades into completely
        unrelated, wasteful register reads for the rest of that connection.

        This drains and discards anything sitting in the socket's receive
        buffer before this transport ever sends its own first request, so
        it never reaches pymodbus's framer at all. It's deliberately
        content-agnostic — nothing is ever supposed to arrive on a fresh
        Modbus TCP socket before the client speaks first, so any bytes seen
        here are already known to be non-Modbus noise regardless of what
        they actually are; this isn't specific to MAC addresses or to this
        one Waveshare model.

        Safe no-op if the client's socket isn't exposed the way expected
        (e.g. a future pymodbus internal change) — this is a best-effort
        guard, not something that should ever block a connection.
        """
        sock: Any | None = getattr(self.client, "socket", None)
        if sock is None:
            return

        original_timeout: float | None = None
        try:
            original_timeout = sock.gettimeout()
        except OSError as e:
            # Non-fatal either way — original_timeout just stays None, and
            # the restore below becomes a no-op-ish "set to None" instead of
            # putting back whatever it actually was. Logged at debug rather
            # than escalated since this is a best-effort guard, not
            # something that should ever affect a connection's outcome.
            self._log.debug(f"{self.transport_name}: could not read socket timeout before priming-byte drain: {e}")

        drained: bytes = b""
        try:
            # Short and deliberately non-configurable: this only needs to be
            # long enough for bytes the device sends immediately on connect
            # to arrive — anything not already in flight isn't what this
            # guards against.
            sock.settimeout(0.25)
            while True:
                chunk: bytes = sock.recv(4096)
                if not chunk:
                    break
                drained += chunk
                if len(chunk) < 4096:
                    break
        except OSError as e:
            # The expected/common case: nothing was waiting, which is fine —
            # most Modbus TCP devices never send anything unprompted. Covers
            # both a plain read timeout and any other socket-level hiccup
            # during this best-effort drain.
            self._log.debug(f"{self.transport_name}: priming-byte drain read failed (non-fatal): {e}")
        finally:
            try:
                sock.settimeout(original_timeout)
            except OSError as e:
                self._log.debug(f"{self.transport_name}: could not restore socket timeout after priming-byte drain: {e}")

        if drained:
            self._log.info(
                f"{self.transport_name}: discarded {len(drained)} unsolicited "
                f"byte(s) received before any request was sent on this "
                f"connection. Some Modbus-TCP bridges send a one-time startup "
                f"banner that isn't part of the Modbus protocol; this has been "
                f"filtered out automatically and does not indicate a problem."
            )

            return None

    def connect(self) -> bool:
        if self.client is None:
            self._log.error(f"Cannot connect '{self.transport_name}' — client not initialized")
            self.connected = False
            return False

        try:
            # pymodbus connect() registers the UDP endpoint
            self.connected = bool(self.client.connect())

            if self.connected:
                self._log.info(f"Modbus UDP configured: {self.connected} for {self.transport_name} on port {self.port}")
                # Must run before super().connect(), which triggers
                # init_after_connect() — serial-number/EG4 hardware-kind
                # detection reads happen there, and those are exactly the
                # reads a priming-byte-corrupted first response would
                # otherwise misinform (see docstring above).
                self._drain_connection_priming_bytes()
                super().connect()
            else:
                self._log.error(f"Failed to configure UDP transport for {self.transport_name} on port {self.port}")

        except Exception as e:
            self._log.error(f"Exception during UDP configuration: {e}")
            self.connected = False

        return self.connected
