# Description: scraper for Modbus RTU devices over RS-232/RS-485 serial, inheriting from modbus_base and implementing RTU-specific client setup and register access logic.
# File: modbus_rtu.py
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


# scraper for Modbus RTU devices over RS-232/RS-485 serial, inheriting from modbus_base and implementing
# RTU-specific client setup and register access logic.

from __future__ import annotations

from threading import RLock
from typing import Any

import serial
from pymodbus.client import ModbusSerialClient
from pymodbus.client.base import ModbusBaseSyncClient
from pymodbus.exceptions import ConnectionException, ModbusException, ModbusIOException
from pymodbus.pdu import ExceptionResponse, ModbusPDU

from classes.protocol_settings import Registry_Type
from defs.common import (
    TransportSettings,
    find_usb_serial_port,
    get_usb_serial_port_info,
)

from .modbus_base import modbus_base


class modbus_rtu(modbus_base):


    transport_type: str = "scraper"
    def __init__(self, settings : TransportSettings) -> None:
        super().__init__(settings)

        self.port = settings.get("port", fallback="/dev/ttyUSB0")
        if not self.port:
            raise ValueError("Port is not set")

        self.port = find_usb_serial_port(self.port)
        if not self.port:
            raise ValueError("Port is not valid / not found")

        self._log.info(f"Serial Port: {self.port} = {get_usb_serial_port_info(self.port)}")

        self.baudrate = settings.getint("baudrate", fallback=9600)

        address : int = settings.getint("address", 0)
        self.addresses: list[int] = [address]

        client_str: str = f"{self.port}({self.baudrate})"
        # Thread-safe client access
        with self._clients_lock:
            if client_str in modbus_base.clients:
                self.client = modbus_base.clients[client_str]
                return

        self._log.debug(f"Creating new client with baud rate: {self.baudrate}")


        self.client = ModbusSerialClient(
            port=self.port,
            baudrate=int(self.baudrate),
            stopbits=settings.getint("stopbits", fallback=1),
            parity=settings.get("parity", fallback="N"),
            bytesize=settings.getint("bytesize", fallback=8),
            timeout=settings.getfloat("timeout", fallback=2.0),
        )

        #add to clients (thread-safe)
        with self._clients_lock:
            modbus_base.clients[client_str] = self.client

    def read_registers(self, start: int, count: int = 1, registry_type: Registry_Type = Registry_Type.INPUT, device_id: int | None = None) -> ModbusPDU | None:
        if self.client is None:
            self._log.error("read_registers called before client was initialized")
            return None

        sync_client: ModbusBaseSyncClient = self.client
        resolved_device_id: int = self._get_device_id(device_id)
        port_lock: RLock= self._get_port_lock()
        result: ModbusPDU | None = None
        with port_lock:
            try:
                if registry_type == Registry_Type.INPUT:
                    result = sync_client.read_input_registers(start, count=count, device_id=resolved_device_id)
                elif registry_type == Registry_Type.HOLDING:
                    result = sync_client.read_holding_registers(start, count=count, device_id=resolved_device_id)
                elif registry_type == Registry_Type.COIL:
                    result = sync_client.read_coils(start, count=count, device_id=resolved_device_id)
                elif registry_type == Registry_Type.DISCRETE:
                    result = sync_client.read_discrete_inputs(start, count=count, device_id=resolved_device_id)
                else:
                    self._log.warning(
                        f"read_registers: unsupported registry_type '{registry_type.name}' for RTU transport — returning None")
                    return None

            except ConnectionException:
                self._log.error(f"Connection lost to {self.transport_name} at {self.port}")
                self._log.error(f"❌ [COMMUNICATION LOST] --- Name: {self.transport_name} ---")
                self.connected = False
                return None
            except (serial.SerialException, OSError) as e:
                # pymodbus's ConnectionException doesn't cover every way a
                # serial port can die mid-read — a yanked USB adapter, a
                # udev-renumbered /dev/tty path, or the underlying OS file
                # descriptor going bad typically surface as
                # serial.SerialException or a raw OSError instead, and
                # neither of those is caught by the ConnectionException
                # branch above. Left uncaught here, they'd fall through to
                # the generic `except Exception` below, which logs but does
                # NOT set self.connected = False — meaning the transport
                # would keep believing it's connected and keep trying (and
                # failing) reads against a dead port instead of
                # reconnecting. Mirrors modbus_tcp's equivalent
                # BrokenPipeError/ConnectionResetError/ConnectionError
                # handling.
                self._log.error(f"Serial port error for {self.transport_name} on {self.port}: {e}")
                self._log.error(f"❌ [Communication Lost] --- Name: {self.transport_name} ---")
                try:
                    self.client.close()
                except Exception:
                    self._log.error(f"❌ [Could not close client] --- Name: {self.transport_name} ---")
                self.connected = False
                return None
            except ModbusIOException as e:
                self._log.error(f"Modbus IO Exception on {self.transport_name}: {e}")
                return None
            except ModbusException as e:
                self._log.error(f"General Modbus error on {self.transport_name}: {e}")
                return None
            except Exception as e:
                self._log.error(f"Unexpected error during read: {e}")
                return None

        if isinstance(result, ExceptionResponse):
            self._log.error("Modbus Error: Result is None")
            return None

        # Use hasattr to safely check for the error attribute/method
        is_error: bool = result.isError() if hasattr(result, "isError") else bool(getattr(result, "is_error", False))

        if is_error:
            self._log.error(f"Modbus Error: {result}")
            return None

        return result

    def _drain_connection_priming_bytes(self) -> None:
        """
        RTU/serial equivalent of modbus_tcp's connection-priming-byte guard
        (see that module's docstring for the full rationale). A TCP-bridged
        Modbus device sending an unsolicited banner right after connect is
        the confirmed case this originally guarded against; it hasn't been
        observed on a direct serial connection, and opening a port with
        pyserial doesn't itself trigger any handshake on the far end. But
        some USB-to-RS485/RS232 adapters or in-line signal converters are
        known to emit their own short startup announcement on power-up/open,
        and the same underlying problem — pymodbus's framer has no concept
        of an unsolicited message and would try to parse it as a real
        response — would apply equally here. The guard costs at most ~0.25s
        once per connect and is a no-op when nothing arrives, so it's cheap
        insurance to carry over rather than something to add reactively
        after chasing the exact same bug a second time on different
        hardware.
        """
        ser: Any | None = getattr(self.client, "socket", None)  # a pyserial.Serial instance once connected
        if ser is None:
            return

        original_timeout: float | None = None
        try:
            original_timeout = ser.timeout
        except Exception as e:
            # Non-fatal either way — original_timeout just stays None, and
            # the restore below becomes a no-op-ish "set to None" instead of
            # putting back whatever it actually was. Logged at debug rather
            # than escalated since this is a best-effort guard, not
            # something that should ever affect a connection's outcome.
            self._log.debug(f"{self.transport_name}: could not read serial timeout before priming-byte drain: {e}")

        drained: bytes = b""
        try:
            ser.timeout = 0.25
            while True:
                waiting: int = ser.in_waiting
                chunk: bytes = ser.read(waiting if waiting > 0 else 1)
                if not chunk:
                    break
                drained += chunk
                if ser.in_waiting == 0:
                    break
        except Exception as e:
            self._log.debug(f"{self.transport_name}: priming-byte drain read failed (non-fatal): {e}")
        finally:
            try:
                ser.timeout = original_timeout
            except Exception as e:
                self._log.debug(f"{self.transport_name}: could not restore serial timeout after priming-byte drain: {e}")

        if drained:
            self._log.info(
                f"{self.transport_name}: discarded {len(drained)} unsolicited "
                f"byte(s) received before any request was sent on this "
                f"connection. Some serial adapters send a one-time startup "
                f"announcement that isn't part of the Modbus protocol; this "
                f"has been filtered out automatically and does not indicate "
                f"a problem."
            )

    def connect(self) -> bool:
        if self.client is None:
            self._log.error(f"Cannot connect {self.transport_name} — client not initialized")
            self.connected = False
            return False

        try:
            # Ensure a stale handle is fully released before reconnecting —
            # mirrors modbus_tcp.connect(); a serial port left half-open
            # from a previous failed session can otherwise surface as a
            # confusing "port already in use"/"Errno 16" on the next
            # connect() attempt instead of a clean reconnect.
            try:
                self.client.close()
            except Exception:
                self._log.error(f"Failed to close RTU connection to {self.port}")

            # pymodbus connect() returns a boolean-like value
            self.connected = bool(self.client.connect())

            if self.connected:
                self._log.info(f"Modbus RTU connected: {self.connected} for {self.transport_name} on port {self.port}")
                # Must run before super().connect(), which triggers
                # init_after_connect() — serial-number/EG4 hardware-kind
                # detection reads happen there, and those are exactly the
                # reads a priming-byte-corrupted first response would
                # otherwise misinform.
                self._drain_connection_priming_bytes()
                super().connect()
            else:
                self._log.error(f"Failed to establish RTU connection to {self.port}")

        except Exception as e:
            self._log.error(f"Exception during connection: {e}")
            self.connected = False

        return self.connected
